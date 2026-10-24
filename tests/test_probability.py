import copy
import math
import unittest
from datetime import datetime, timedelta, timezone

from engine.probability import analyze, american_to_decimal, implied_probability


def payload():
    return {
        "mode": "pregame",
        "as_of": "2026-10-24T12:00:00Z",
        "game": {"season": 2026, "kickoff": "2026-10-25T17:00:00Z"},
        "market": {"stat": "rushing_yards", "side": "over", "line": 50.5, "american_odds": -110},
        "history": [
            {
                "game_id": f"historical-{index}",
                "played_at": f"2026-09-{index + 1:02d}T17:00:00Z",
                "season": 2026,
                "opportunities": 10 + index,
                "production": 40 + index * 10,
            }
            for index in range(6)
        ],
        "projection": {
            "model_id": "empirical-bootstrap-v1",
            "simulations": 3000,
            "seed": 68,
            "scripts": [
                {"name": "early_lead", "weight": 0.25, "opportunity_multiplier": 1.1, "efficiency_multiplier": 1},
                {"name": "competitive", "weight": 0.50, "opportunity_multiplier": 1, "efficiency_multiplier": 1},
                {"name": "early_deficit", "weight": 0.25, "opportunity_multiplier": 0.9, "efficiency_multiplier": 1},
            ],
            "sensitivity": {"opportunity_multiplier": 0.8, "efficiency_multiplier": 0.9},
        },
    }


def calibration():
    records = []
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index in range(100):
        predicted = start + timedelta(days=index)
        records.append({
            "prediction_id": f"prediction-{index}",
            "game_id": f"holdout-game-{index}",
            "player_id": "holdout-player",
            "predicted_at": predicted.isoformat(),
            "settled_at": (predicted + timedelta(hours=4)).isoformat(),
            "probability": 0.6,
            "outcome": 1 if index < 60 else 0,
            "source_url": f"https://example.com/results/{index}",
        })
    return {
        "model_id": "empirical-bootstrap-v1",
        "market_stat": "rushing_yards",
        "training_ended_at": "2025-12-31T00:00:00Z",
        "predictions": records,
    }


def uniform_history(data, production=50, opportunities=10):
    for record in data["history"]:
        record["production"] = production
        record["opportunities"] = opportunities
    for script in data["projection"]["scripts"]:
        script["opportunity_multiplier"] = 1
        script["efficiency_multiplier"] = 1


class OddsTests(unittest.TestCase):
    def test_american_odds_and_break_even(self):
        self.assertAlmostEqual(american_to_decimal(-110), 1 + 100 / 110)
        self.assertEqual(american_to_decimal(150), 2.5)
        self.assertEqual(american_to_decimal(-100), 2)
        self.assertAlmostEqual(implied_probability(-110), 110 / 210)
        self.assertEqual(implied_probability(150), 0.4)

    def test_invalid_odds(self):
        for value in [0, 90, -90, True, "-110", math.nan, math.inf]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                american_to_decimal(value)


class SimulationTests(unittest.TestCase):
    def test_real_draw_count_seed_reproducibility_and_no_mutation(self):
        data = payload()
        original = copy.deepcopy(data)
        first = analyze(data)
        second = analyze(data)
        self.assertEqual(first, second)
        self.assertEqual(data, original)
        self.assertEqual(first["simulation"]["draws"], 3000)
        self.assertTrue(first["simulation"]["executed"])
        self.assertEqual(sum(first["simulation"]["script_draws"].values()), 3000)
        self.assertAlmostEqual(first["probability_win"] + first["probability_loss"] + first["probability_push"], 1)
        self.assertLess(first["probability_interval_95"]["low"], first["probability_win"])
        self.assertGreater(first["probability_interval_95"]["high"], first["probability_win"])
        self.assertLess(first["stress"]["probability_win"], first["probability_win"])

    def test_push_refunds_stake_and_fractional_line_cannot_push(self):
        data = payload()
        uniform_history(data)
        data["market"]["line"] = 50
        report = analyze(data)
        self.assertEqual(report["probability_push"], 1)
        self.assertEqual(report["probability_win"], 0)
        self.assertEqual(report["unit_expected_value"], 0)
        self.assertFalse(report["recommended_value"])
        data["market"]["line"] = 49.5
        report = analyze(data)
        self.assertEqual(report["probability_push"], 0)
        self.assertEqual(report["probability_win"], 1)
        self.assertAlmostEqual(report["unit_expected_value"], 100 / 110)

    def test_sample_windows_do_not_claim_unavailable_ten_games(self):
        report = analyze(payload())
        self.assertEqual(report["hit_rates"]["last_4"]["sample_games"], 4)
        self.assertEqual(report["hit_rates"]["last_5"]["sample_games"], 5)
        self.assertEqual(report["hit_rates"]["last_10"]["sample_games"], 6)
        self.assertFalse(report["hit_rates"]["last_10"]["complete_window"])

    def test_zero_opportunity_games_are_preserved(self):
        data = payload()
        uniform_history(data)
        data["history"][0].update(opportunities=0, production=0)
        report = analyze(data)
        self.assertEqual(report["simulation"]["historical_games"], 6)
        self.assertGreater(report["probability_loss"], 0)

    def test_negative_net_yards_are_not_discarded(self):
        data = payload()
        uniform_history(data, production=-5)
        data["market"]["line"] = 0.5
        report = analyze(data)
        self.assertEqual(report["baseline"]["mean"], -5)
        self.assertEqual(report["probability_loss"], 1)

    def test_invalid_input_cases(self):
        cases = []
        for key, value in [("simulations", 200001), ("simulations", True), ("seed", "68"), ("model_id", "imaginary")]:
            data = payload()
            data["projection"][key] = value
            cases.append(data)
        data = payload()
        data["history"][0]["production"] = math.nan
        cases.append(data)
        data = payload()
        data["history"][0]["season"] = 2025
        cases.append(data)
        data = payload()
        data["history"][0]["played_at"] = data["as_of"]
        cases.append(data)
        data = payload()
        data["history"][0]["game_id"] = data["history"][1]["game_id"]
        cases.append(data)
        data = payload()
        data["projection"]["scripts"][0]["weight"] = 0.9
        cases.append(data)
        data = payload()
        data["projection"]["scripts"][0]["name"] = ["early_lead"]
        cases.append(data)
        data = payload()
        data["history"][0]["opportunities"] = 0
        cases.append(data)
        data = payload()
        uniform_history(data, production=0, opportunities=0)
        cases.append(data)
        data = payload()
        data["market"]["stat"] = "receptions"
        data["history"][0]["production"] = -1
        cases.append(data)
        for index, data in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(ValueError):
                analyze(data)


class CalibrationTests(unittest.TestCase):
    def test_no_calibration_blocks_qualification(self):
        report = analyze(payload())
        self.assertFalse(report["calibration"]["qualified"])
        self.assertEqual(report["calibration"]["observations"], 0)
        self.assertIsNone(report["calibration"]["brier_score"])

    def test_supplied_holdout_metrics_are_computed(self):
        data = payload()
        for script in data["projection"]["scripts"]:
            script["opportunity_multiplier"] = 1
        data["calibration"] = calibration()
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertEqual(report["observations"], 100)
        self.assertEqual(report["unique_games"], 100)
        self.assertAlmostEqual(report["brier_score"], 0.24)
        self.assertAlmostEqual(report["brier_baseline"], 0.24)
        self.assertAlmostEqual(report["brier_skill_score"], 0)
        self.assertAlmostEqual(report["expected_calibration_error"], 0)
        self.assertAlmostEqual(report["conservative_probability_floor"], .6 - math.sqrt(math.log(20) / 200))
        self.assertIn("support floor", report["reason"])

    def test_many_distinct_games_and_strong_support_qualify(self):
        data = payload()
        uniform_history(data, production=100)
        data["calibration"] = calibration()
        for record in data["calibration"]["predictions"]:
            record.update(probability=1, outcome=1)
        report = analyze(data)["calibration"]
        self.assertTrue(report["qualified"], report["reason"])
        self.assertEqual(report["candidate_bin_unique_games"], 100)
        self.assertAlmostEqual(report["conservative_probability_floor"], 1 - math.sqrt(math.log(20) / 200))
        self.assertIsNone(report["brier_skill_score"])
        self.assertIn("Assumes independent games", report["uncertainty_method"])

    def test_correlated_props_cannot_inflate_game_count(self):
        data = payload()
        uniform_history(data, production=100)
        data["calibration"] = calibration()
        for index, record in enumerate(data["calibration"]["predictions"]):
            record.update(game_id="one-game", player_id=f"player-{index}", line=50.5,
                          side="over", probability=1, outcome=1)
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertEqual(report["observations"], 100)
        self.assertEqual(report["unique_games"], 1)
        self.assertIn("distinct settled games", report["reason"])

    def test_candidate_bin_needs_distinct_games_even_if_global_coverage_is_adequate(self):
        data = payload()
        uniform_history(data, production=100)
        data["calibration"] = calibration()
        for index, record in enumerate(data["calibration"]["predictions"]):
            record.update(player_id=f"player-{index}", line=50.5, side="over")
            if index < 20:
                record.update(game_id="same-high-probability-game", probability=1, outcome=1)
            else:
                record.update(probability=0, outcome=0)
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertEqual(report["unique_games"], 81)
        self.assertEqual(report["candidate_bin_observations"], 20)
        self.assertEqual(report["candidate_bin_unique_games"], 1)
        self.assertIn("probability range needs at least 10 distinct", report["reason"])

    def test_global_distinct_game_threshold_boundary(self):
        for games, qualified in ((29, False), (30, True)):
            data = payload()
            uniform_history(data, production=100)
            data["calibration"] = calibration()
            for index, record in enumerate(data["calibration"]["predictions"]):
                record.update(game_id=f"game-{index % games}", player_id=f"player-{index}",
                              line=50.5, side="over", probability=1, outcome=1)
            report = analyze(data)["calibration"]
            with self.subTest(games=games):
                self.assertEqual(report["qualified"], qualified, report["reason"])
                self.assertEqual(report["unique_games"], games)
                self.assertAlmostEqual(report["conservative_probability_floor"],
                                       1 - math.sqrt(math.log(20) / (2 * games)))

    def test_candidate_bin_game_threshold_boundary(self):
        for games, qualified in ((9, False), (10, True)):
            data = payload()
            uniform_history(data, production=100)
            data["calibration"] = calibration()
            for index, record in enumerate(data["calibration"]["predictions"]):
                record.update(player_id=f"player-{index}", line=50.5, side="over")
                if index < 20:
                    record.update(game_id=f"high-bin-{index % games}", probability=1, outcome=1)
                else:
                    record.update(probability=0, outcome=0)
            report = analyze(data)["calibration"]
            with self.subTest(games=games):
                self.assertEqual(report["qualified"], qualified, report["reason"])
                self.assertEqual(report["candidate_bin_unique_games"], games)

    def test_game_weighting_limits_one_games_influence_and_bound_caps_at_forecast(self):
        data = payload()
        uniform_history(data, production=100)
        data["calibration"] = calibration()
        records = data["calibration"]["predictions"]
        for index, record in enumerate(records):
            record.update(player_id=f"player-{index}", line=50.5, side="over", probability=1, outcome=1)
        # Ninety winning props in one game count once beside ten losing games.
        for index, record in enumerate(records):
            if index < 90:
                record["game_id"] = "concentrated-wins"
            else:
                record["outcome"] = 0
        report = analyze(data)["calibration"]
        self.assertAlmostEqual(report["candidate_bin_equal_game_win_rate"], 1 / 11)
        self.assertEqual(report["conservative_probability_floor"], 0)

    def test_game_and_player_identity_are_required(self):
        for field in ("game_id", "player_id"):
            data = payload()
            data["calibration"] = calibration()
            del data["calibration"]["predictions"][0][field]
            report = analyze(data)["calibration"]
            self.assertFalse(report["qualified"])
            self.assertIn(field, report["reason"])

    def test_reliability_bins_preserve_counts_and_brier_skill_diagnostics(self):
        data = payload()
        data["calibration"] = calibration()
        for index, record in enumerate(data["calibration"]["predictions"]):
            record["probability"] = .8 if index < 50 else .2
            record["outcome"] = 1 if index < 50 else 0
        report = analyze(data)["calibration"]
        self.assertAlmostEqual(report["brier_score"], .04)
        self.assertAlmostEqual(report["brier_baseline"], .25)
        self.assertAlmostEqual(report["brier_skill_score"], .84)
        self.assertEqual(sum(row["observations"] for row in report["reliability_bins"]), 100)
        self.assertEqual(report["reliability_bins"][8]["unique_games"], 50)
        self.assertEqual(report["reliability_bins"][8]["predicted_mean"], .8)
        self.assertEqual(report["reliability_bins"][8]["observed_mean"], 1)

    def test_unseen_probability_range_does_not_inherit_global_calibration(self):
        data = payload()
        data["calibration"] = calibration()
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertEqual(report["candidate_bin_observations"], 0)
        self.assertIn("probability range", report["reason"])

    def test_leakage_mismatch_and_duplicate_records_fail_closed(self):
        cases = []
        for field, value in [("model_id", "wrong-model"), ("market_stat", "receptions"), ("training_ended_at", "2026-01-02T00:00:00Z")]:
            record = calibration()
            record[field] = value
            cases.append(record)
        record = calibration()
        record["predictions"][0]["settled_at"] = "2026-11-01T00:00:00Z"
        cases.append(record)
        record = calibration()
        record["predictions"][1] = copy.deepcopy(record["predictions"][0])
        cases.append(record)
        record = calibration()
        del record["predictions"][0]["outcome"]
        cases.append(record)
        record = calibration()
        record["predictions"][0]["source_url"] = "http://example.com/unverified"
        cases.append(record)
        for index, record in enumerate(cases):
            data = payload()
            data["calibration"] = record
            with self.subTest(case=index):
                self.assertFalse(analyze(data)["calibration"]["qualified"])

    def test_small_sample_and_miscalibration_fail(self):
        data = payload()
        data["calibration"] = calibration()
        data["calibration"]["predictions"] = data["calibration"]["predictions"][:50]
        self.assertFalse(analyze(data)["calibration"]["qualified"])
        data["calibration"] = calibration()
        for record in data["calibration"]["predictions"]:
            record["probability"] = 0.99
        self.assertFalse(analyze(data)["calibration"]["qualified"])

    def test_synthetic_calibration_cannot_validate_real_model(self):
        data = payload()
        data["calibration"] = calibration()
        data["calibration"]["synthetic"] = True
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertIn("Synthetic", report["reason"])

    def test_duplicate_prediction_ids_cannot_inflate_sample_size(self):
        data = payload()
        data["calibration"] = calibration()
        for record in data["calibration"]["predictions"]:
            record["prediction_id"] = "same-prediction"
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertIn("unique", report["reason"])

    def test_copied_prediction_events_with_changed_metadata_fail(self):
        data = payload()
        data["calibration"] = calibration()
        duplicate = copy.deepcopy(data["calibration"]["predictions"][0])
        duplicate.update(prediction_id="new-id", probability=0.51, source_url="https://example.org/copied")
        data["calibration"]["predictions"][1] = duplicate
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertIn("duplicate prediction events", report["reason"])

    def test_mixed_synthetic_records_fail(self):
        data = payload()
        data["calibration"] = calibration()
        data["calibration"]["predictions"][0]["synthetic"] = True
        self.assertFalse(analyze(data)["calibration"]["qualified"])

    def test_stable_prediction_id_is_required(self):
        data = payload()
        data["calibration"] = calibration()
        del data["calibration"]["predictions"][0]["prediction_id"]
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertIn("prediction IDs", report["reason"])

    def test_all_push_projection_skips_undefined_conditional_probability_bin(self):
        data = payload()
        uniform_history(data)
        data["market"]["line"] = 50
        data["calibration"] = calibration()
        report = analyze(data)
        self.assertEqual(report["probability_push"], 1)
        self.assertIsNone(report["calibration"]["candidate_probability"])
        self.assertFalse(report["recommended_value"])


class LiveTests(unittest.TestCase):
    def live_payload(self):
        data = payload()
        data["mode"] = "live"
        data["as_of"] = "2026-10-25T18:00:00Z"
        data["live"] = {"production": 40, "observed_at": data["as_of"], "remaining_opportunities": [0]}
        uniform_history(data)
        return data

    def test_live_model_uses_accrued_plus_remaining_only(self):
        data = self.live_payload()
        data["market"]["line"] = 39.5
        report = analyze(data)
        self.assertEqual(report["baseline"]["mean"], 40)
        self.assertEqual(report["stress"]["probability_win"], 1)
        self.assertEqual(report["baseline"]["mean_opportunities"], 0)
        self.assertTrue(report["live"]["executed"])
        self.assertEqual(report["live"]["remaining_needed"], 0)
        data["live"]["remaining_opportunities"] = [2]
        report = analyze(data)
        self.assertEqual(report["baseline"]["mean"], 50)
        self.assertEqual(report["stress"]["probability_win"], 1)

    def test_live_requires_own_phase_calibration(self):
        data = self.live_payload()
        data["calibration"] = calibration()
        for record in data["calibration"]["predictions"]:
            record["probability"] = 0
            record["outcome"] = 0
        report = analyze(data)
        self.assertFalse(report["calibration"]["qualified"])
        self.assertIn("mode", report["calibration"]["reason"])
        data["calibration"]["mode"] = "live"
        report = analyze(data)["calibration"]
        self.assertFalse(report["qualified"])
        self.assertNotIn("mode", report["reason"])
        self.assertEqual(report["conservative_probability_floor"], 0)

    def test_missing_live_inputs_fail(self):
        for value in [None, {}, {"production": 40, "remaining_opportunities": []}]:
            data = self.live_payload()
            data["live"] = value
            with self.subTest(live=value), self.assertRaises(ValueError):
                analyze(data)


if __name__ == "__main__":
    unittest.main()
