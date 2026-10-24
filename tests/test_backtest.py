from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

from engine.backtest import run_backtest


def snapshot(game_id="holdout-1", line=50.5, synthetic=False):
    return {
        "schema_version": 1, "mode": "pregame", "synthetic": synthetic,
        "as_of": "2026-10-10T12:00:00Z",
        "game": {"id": game_id, "season": 2026, "kickoff": "2026-10-11T17:00:00Z",
                 "home_team": "HOME", "away_team": "AWAY", "spread": -3},
        "player": {"id": "player-1", "name": "Player One", "team": "HOME", "position": "RB"},
        "market": {"sportsbook": "Example", "stat": "rushing_yards", "side": "over",
                   "line": line, "american_odds": -110, "observed_at": "2026-10-10T11:59:00Z"},
        "history": [{"game_id": f"history-{i}", "played_at": f"2026-09-{day:02d}T20:00:00Z",
                     "season": 2026, "opportunities": 15 + i, "production": 40 + i * 10}
                    for i, day in enumerate((1, 7, 14, 21, 28))],
        "projection": {"model_id": "empirical-bootstrap-v1", "simulations": 1000, "seed": 68,
                       "scripts": [{"name": name, "weight": weight,
                                    "opportunity_multiplier": 1, "efficiency_multiplier": 1}
                                   for name, weight in (("early_lead", 0.25), ("competitive", 0.5), ("early_deficit", 0.25))],
                       "sensitivity": {"opportunity_multiplier": 0.8, "efficiency_multiplier": 0.9}},
        "evidence": [],
    }


def dataset(*cases):
    return {"as_of": "2026-10-24T12:00:00Z", "model_id": "empirical-bootstrap-v1",
            "market_stat": "rushing_yards", "mode": "pregame",
            "training_ended_at": "2026-10-01T00:00:00Z", "cases": list(cases)}


def case(game_id="holdout-1", result=75, line=50.5, synthetic=False):
    return {"snapshot": snapshot(game_id, line, synthetic), "official_result": result,
            "settled_at": "2026-10-11T22:00:00Z", "source_url": "https://example.com/official-result"}


class BacktestTests(unittest.TestCase):
    def test_real_execution_counts_outcomes_and_preserves_input(self):
        values = dataset(case("win", 75), case("loss", 20), case("push", 50, line=50))
        original = deepcopy(values)
        report = run_backtest(values)
        self.assertEqual(report["summary"]["cases_executed"], 3)
        self.assertEqual(report["summary"]["total_simulation_draws"], 3000)
        self.assertEqual(report["summary"]["wins"], 1)
        self.assertEqual(report["summary"]["losses"], 1)
        self.assertEqual(report["summary"]["pushes"], 1)
        self.assertEqual(report["summary"]["nonpush_observations"], 2)
        self.assertEqual(values, original)
        self.assertEqual(report, run_backtest(values))
        self.assertTrue(all(len(row["snapshot_sha256"]) == 64 for row in report["calibration"]["predictions"]))

    def test_calibration_is_conditional_on_nonpushes(self):
        value = dataset(case(result=75, line=50))
        report = run_backtest(value)
        row = report["calibration"]["predictions"][0]
        from engine.probability import analyze
        direct = analyze(value["cases"][0]["snapshot"])
        self.assertAlmostEqual(row["probability"], direct["probability_win"] / (direct["probability_win"] + direct["probability_loss"]))

    def test_all_push_forecasts_are_excluded_without_inventing_probability(self):
        value = dataset(case(result=50, line=50))
        for history_row in value["cases"][0]["snapshot"]["history"]:
            history_row["production"] = 50
        report = run_backtest(value)
        self.assertEqual(report["summary"]["total_simulation_draws"], 1000)
        self.assertEqual(report["calibration"]["predictions"], [])
        self.assertEqual(len(report["excluded_predictions"]), 1)
        self.assertIsNone(report["summary"]["brier_score"])

    def test_negative_official_yards_are_allowed(self):
        report = run_backtest(dataset(case(result=-2)))
        self.assertEqual(report["summary"]["losses"], 1)

    def test_synthetic_cases_are_marked_not_real_validation(self):
        report = run_backtest(dataset(case(synthetic=True)))
        self.assertTrue(report["calibration"]["synthetic"])
        self.assertEqual(report["summary"]["synthetic_cases"], 1)

    def test_duplicate_case_rejected_even_if_sportsbook_differs(self):
        first, second = case(), case()
        second["snapshot"]["market"]["sportsbook"] = "Another Book"
        with self.assertRaisesRegex(ValueError, "duplicate"):
            run_backtest(dataset(first, second))

    def test_leakage_future_history_training_and_settlement_rejected(self):
        variants = []
        value = dataset(case())
        value["training_ended_at"] = "2026-10-10T13:00:00Z"
        variants.append(value)
        value = dataset(case())
        value["cases"][0]["settled_at"] = "2026-11-02T00:00:00Z"
        variants.append(value)
        value = dataset(case())
        value["cases"][0]["settled_at"] = "2026-10-11T16:00:00Z"
        variants.append(value)
        value = dataset(case())
        value["cases"][0]["snapshot"]["history"][0]["played_at"] = "2026-10-10T12:01:00Z"
        variants.append(value)
        for value in variants:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    run_backtest(value)

    def test_mismatched_model_category_and_phase_rejected(self):
        for field, bad_value in (("model_id", "imaginary-model"), ("market_stat", "receiving_yards"), ("mode", "live")):
            with self.subTest(field=field):
                value = dataset(case())
                value[field] = bad_value
                with self.assertRaises(ValueError):
                    run_backtest(value)

    def test_invalid_official_result_or_source_rejected(self):
        for result in (float("nan"), float("inf"), True, 1.5):
            with self.subTest(result=result):
                with self.assertRaises(ValueError):
                    run_backtest(dataset(case(result=result)))
        for source in ("http://example.com/result", "https://user:password@example.com/result", "missing"):
            with self.subTest(source=source):
                value = dataset(case())
                value["cases"][0]["source_url"] = source
                with self.assertRaises(ValueError):
                    run_backtest(value)

    def test_cases_processed_chronologically(self):
        early, late = case("early"), case("late")
        late["snapshot"]["as_of"] = "2026-10-11T12:00:00Z"
        late["snapshot"]["market"]["observed_at"] = "2026-10-11T11:59:00Z"
        report = run_backtest(dataset(late, early))
        self.assertEqual([row["game_id"] for row in report["calibration"]["predictions"]], ["early", "late"])

    def test_future_cutoff_requires_every_case_to_be_synthetic(self):
        future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        value = dataset(case())
        value["as_of"] = future
        with self.assertRaisesRegex(ValueError, "future"):
            run_backtest(value)
        value = dataset(case(synthetic=True))
        value["as_of"] = future
        report = run_backtest(value)
        self.assertTrue(report["synthetic"])
        self.assertTrue(report["calibration"]["synthetic"])
        self.assertTrue(report["calibration"]["predictions"][0]["synthetic"])
        value["cases"].append(case("real", synthetic=False))
        with self.assertRaisesRegex(ValueError, "future"):
            run_backtest(value)

    def test_malformed_local_enum_fields_raise_value_error(self):
        for field in ("mode", "market_stat"):
            with self.subTest(field=field):
                value = dataset(case())
                value[field] = []
                with self.assertRaises(ValueError):
                    run_backtest(value)


if __name__ == "__main__":
    unittest.main()
