"""Scenario and workload regressions using fictional numerical fixtures."""

import copy
from datetime import datetime, timedelta, timezone
import unittest

from engine.robustness import assess


def payload(opportunities=None):
    counts = [20] * 6 if opportunities is None else opportunities
    first = datetime(2026, 9, 1, 17, tzinfo=timezone.utc)
    return {
        "player": {"team": "TEST-HOME", "context": {"quarterback_id": "QB-A", "playcaller_id": "OC-A"}},
        "history": [{"game_id": f"history-{index}", "played_at": (first + timedelta(weeks=index)).isoformat(),
                     "opportunities": count, "production": 100, "team": "TEST-HOME",
                     "quarterback_id": "QB-A", "playcaller_id": "OC-A"}
                    for index, count in enumerate(counts)],
    }


def model():
    return {
        "simulation": {"executed": True, "draws": 3000},
        "decimal_odds": 1 + 100 / 110,
        "scripts": {name: {"draws": 1000, "wins": 800, "losses": 200, "pushes": 0}
                    for name in ("early_lead", "competitive", "early_deficit")},
    }


class RobustnessTests(unittest.TestCase):
    def test_all_script_values_and_stable_workload_can_pass(self):
        data, projection = payload(), model()
        originals = copy.deepcopy((data, projection))
        result = assess(data, projection)
        self.assertTrue(result["qualified"], result["blockers"])
        self.assertEqual(result["warnings"], [])
        self.assertEqual(len(result["game_scripts"]), 3)
        self.assertGreater(result["worst_script_unit_ev"], 0)
        self.assertEqual(result["workload_trend"]["recent_to_previous_ratio"], 1)
        self.assertEqual((data, projection), originals)

    def test_profitable_mixture_cannot_hide_losing_deficit_script(self):
        projection = model()
        projection["scripts"]["early_deficit"].update(wins=450, losses=550)
        mixture_ev = sum(row["wins"] / 3000 * (projection["decimal_odds"] - 1)
                         - row["losses"] / 3000 for row in projection["scripts"].values())
        self.assertGreater(mixture_ev, 0)
        result = assess(payload(), projection)
        self.assertFalse(result["qualified"])
        self.assertEqual(result["worst_script"], "early_deficit")
        self.assertTrue(any("early_deficit fails" in reason for reason in result["blockers"]))

    def test_small_positive_point_edge_fails_sampling_error_screen(self):
        projection = model()
        projection["scripts"]["competitive"].update(wins=530, losses=470)
        result = assess(payload(), projection)
        competitive = next(row for row in result["game_scripts"] if row["name"] == "competitive")
        self.assertGreater(competitive["unit_expected_value"], 0)
        self.assertLess(competitive["conservative_unit_expected_value"], 0)
        self.assertFalse(result["qualified"])

    def test_zero_or_rare_script_draws_block(self):
        for count in (0, 29):
            with self.subTest(count=count):
                projection = model()
                projection["scripts"]["early_deficit"] = {"draws": count}
                if count:
                    projection["scripts"]["early_deficit"].update(wins=count, losses=0, pushes=0)
                projection["simulation"]["draws"] = 2000 + count
                result = assess(payload(), projection)
                self.assertFalse(result["qualified"])
                self.assertTrue(any("fewer than 30" in reason for reason in result["blockers"]))

    def test_missing_script_and_unexecuted_model_block(self):
        projection = model()
        del projection["scripts"]["early_lead"]
        projection["simulation"]["executed"] = False
        result = assess(payload(), projection)
        self.assertFalse(result["qualified"])
        self.assertTrue(any("missing" in reason for reason in result["blockers"]))
        self.assertTrue(any("actually executed" in reason for reason in result["blockers"]))
        self.assertTrue(any("do not match" in reason for reason in result["blockers"]))

    def test_pushes_are_refunded_and_not_counted_as_losses(self):
        projection = model()
        for row in projection["scripts"].values():
            row.update(wins=700, losses=0, pushes=300)
        result = assess(payload(), projection)
        self.assertTrue(result["qualified"], result["blockers"])
        self.assertAlmostEqual(result["game_scripts"][0]["unit_expected_value"], .7 * 100 / 110)

    def test_workload_decline_uses_chronological_latest_six_games(self):
        data = payload([100, 100, 20, 20, 20, 10, 10, 10])
        data["history"].reverse()
        result = assess(data, model())
        self.assertFalse(result["qualified"])
        trend = result["workload_trend"]
        self.assertEqual(trend["previous_mean_opportunities"], 20)
        self.assertEqual(trend["recent_mean_opportunities"], 10)
        self.assertEqual(trend["recent_to_previous_ratio"], .5)

    def test_workload_ratio_boundary_and_zero_baselines_are_explicit(self):
        for counts, expected_flag, expected_ratio in (([20, 20, 20, 15, 15, 15], False, .75),
                                                      ([20, 20, 20, 0, 0, 0], True, 0),
                                                      ([0] * 6, False, None)):
            with self.subTest(counts=counts):
                result = assess(payload(counts), model())
                self.assertEqual(result["workload_trend"]["decline_flag"], expected_flag)
                self.assertEqual(result["workload_trend"]["recent_to_previous_ratio"], expected_ratio)

    def test_fewer_than_six_games_is_an_explicit_warning_only(self):
        result = assess(payload([20] * 5), model())
        self.assertTrue(result["qualified"], result["blockers"])
        self.assertFalse(result["workload_trend"]["available"])
        self.assertTrue(any("six historical" in warning for warning in result["warnings"]))

    def test_negative_yards_do_not_distort_opportunity_trend(self):
        data = payload()
        for row in data["history"][-3:]:
            row["production"] = -5
        result = assess(data, model())
        self.assertTrue(result["qualified"])
        self.assertFalse(result["workload_trend"]["decline_flag"])

    def test_explicit_historical_context_mismatches_block_without_filtering(self):
        for field in ("team", "quarterback_id", "playcaller_id"):
            with self.subTest(field=field):
                data = payload()
                data["history"][0][field] = "OLD-CONTEXT"
                result = assess(data, model())
                self.assertFalse(result["qualified"])
                self.assertEqual(result["workload_trend"]["context"][field]["mismatched_games"], ["history-0"])
                self.assertEqual(result["workload_trend"]["historical_games"], 6)

    def test_absent_optional_context_warns_without_inventing_or_rejecting(self):
        data = payload()
        del data["player"]["context"]
        for row in data["history"]:
            del row["team"]
            del row["quarterback_id"]
            del row["playcaller_id"]
        result = assess(data, model())
        self.assertTrue(result["qualified"])
        self.assertTrue(any("regime continuity was not checked" in warning for warning in result["warnings"]))
        self.assertTrue(any("Historical team metadata is missing" in warning for warning in result["warnings"]))

    def test_invalid_counts_metadata_and_nonfinite_values_are_value_errors(self):
        scenarios = []
        projection = model()
        projection["scripts"]["early_lead"]["wins"] = 1001
        scenarios.append((payload(), projection))
        projection = model()
        projection["scripts"]["early_lead"]["draws"] = True
        scenarios.append((payload(), projection))
        data = payload()
        data["history"][0]["opportunities"] = float("nan")
        scenarios.append((data, model()))
        data = payload()
        data["history"][0]["team"] = []
        scenarios.append((data, model()))
        for data, projection in scenarios:
            with self.subTest(data=data, projection=projection):
                with self.assertRaises(ValueError):
                    assess(data, projection)


if __name__ == "__main__":
    unittest.main()
