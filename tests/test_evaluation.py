"""Gate integration tests using deliberately fictional fixtures, never real picks."""

import copy
from datetime import datetime, timedelta, timezone
import unittest

from engine.evaluation import SCORE_WEIGHTS, evaluate
from engine.probability import MODEL_ID
from engine.rules import COMPUTED_LIVE, COMPUTED_PREGAME


def eligible_fixture():
    """Contrived data traverses the qualified branch; it is not model validation.

    The synthetic flag is deliberately false only inside this unit fixture so
    tests can exercise every other gate. Example domains and fictional players
    make its non-production provenance explicit.
    """
    cutoff = "2026-10-17T12:00:00Z"
    source_time = "2026-10-17T11:55:00Z"
    data = {
        "schema_version": 1,
        "synthetic": False,
        "mode": "pregame",
        "as_of": cutoff,
        "game": {
            "id": "FICTIONAL-SUNDAY-GAME", "season": 2026,
            "kickoff": "2026-10-18T17:00:00Z",
            "home_team": "FICTIONAL-HOME", "away_team": "FICTIONAL-AWAY",
            "spread": -3.5,
        },
        "player": {
            "id": "fictional-player", "name": "Fictional Test Player",
            "team": "FICTIONAL-HOME", "position": "RB", "stable_games": 5,
            "risk": {
                "injury_designation_last_14_days": False,
                "missed_game_last_4_weeks": False,
                "final_status_confirmed": True,
                "qb_changed_last_2_weeks": False,
                "playcaller_changed_last_2_weeks": False,
                "usage_restriction_unresolved": False,
            },
        },
        "market": {
            "sportsbook": "FICTIONAL-TEST-BOOK", "stat": "rushing_yards",
            "side": "over", "line": 40.5, "american_odds": -110,
            "observed_at": source_time,
        },
        "history": [
            {
                "game_id": f"fictional-history-{index}",
                "season": 2026, "played_at": f"2026-09-{index + 7:02d}T17:00:00Z",
                "opportunities": 10, "production": 100,
                "home": bool(index % 2), "opponent": f"FICTIONAL-{index}",
                "snap_share": .75, "route_rate": .80,
            }
            for index in range(5)
        ],
        "projection": {
            "model_id": MODEL_ID, "simulations": 1000, "seed": 68,
            "scripts": [
                {"name": name, "weight": weight,
                 "opportunity_multiplier": 1, "efficiency_multiplier": 1}
                for name, weight in [("early_lead", .25), ("competitive", .50),
                                     ("early_deficit", .25)]
            ],
            "sensitivity": {"opportunity_multiplier": .8, "efficiency_multiplier": .9},
        },
        "evidence": [
            {
                "rule_id": rule_id, "status": "supported",
                "summary": f"Fictional source-backed test assertion for rule {rule_id}.",
                "sources": [{"url": f"https://official.example/rule/{rule_id}",
                             "published_at": source_time, "retrieved_at": source_time}],
            }
            for rule_id in range(1, 51) if rule_id not in COMPUTED_PREGAME
        ],
    }
    first_prediction = datetime(2026, 1, 2, tzinfo=timezone.utc)
    data["calibration"] = {
        "model_id": MODEL_ID, "market_stat": "rushing_yards", "mode": "pregame",
        "training_ended_at": "2026-01-01T00:00:00Z",
        "predictions": [
            {
                "prediction_id": f"fictional-prediction-{index}",
                "predicted_at": (first_prediction + timedelta(days=index)).isoformat(),
                "settled_at": (first_prediction + timedelta(days=index, hours=4)).isoformat(),
                "probability": 1, "outcome": 1,
                "source_url": f"https://official.example/fictional-results/{index}",
            }
            for index in range(100)
        ],
    }
    return data


def live_fixture():
    data = eligible_fixture()
    data["mode"] = "live"
    data["as_of"] = "2026-10-18T18:00:00Z"
    source_time = "2026-10-18T17:59:30Z"
    data["market"]["observed_at"] = source_time
    data["calibration"]["mode"] = "live"
    data["live"] = {
        "observed_at": source_time, "quarter": 3, "clock_seconds": 600,
        "score": {"home": 14, "away": 10}, "possession": "FICTIONAL-HOME",
        "player_active": True, "production": 35,
        "remaining_opportunities": [2, 3, 4],
    }
    for record in data["evidence"]:
        for source in record["sources"]:
            source.update(published_at=source_time, retrieved_at=source_time)
    data["evidence"].extend(
        {
            "rule_id": rule_id, "status": "supported",
            "summary": f"Fictional live assertion for rule {rule_id}.",
            "sources": [{"url": f"https://official.example/live/{rule_id}",
                         "published_at": source_time, "retrieved_at": source_time}],
        }
        for rule_id in range(51, 60) if rule_id not in COMPUTED_LIVE
    )
    return data


class EvaluationTests(unittest.TestCase):
    def assert_blocked(self, data, text):
        report = evaluate(data)
        self.assertEqual(report["decision"], "NO_BET")
        self.assertTrue(any(text in reason for reason in report["blockers"]), report["blockers"])
        return report

    def test_fully_supplied_fixture_can_traverse_the_qualified_branch(self):
        data = eligible_fixture()
        original = copy.deepcopy(data)
        report = evaluate(data)
        self.assertEqual(report["decision"], "QUALIFIED", report["blockers"])
        self.assertEqual(report["blockers"], [])
        self.assertEqual(report["target_window"], "early")
        self.assertEqual(report["model"]["simulation"]["draws"], 1000)
        self.assertTrue(report["model"]["calibration"]["qualified"])
        self.assertEqual(report["audit_counts"]["not_applicable"], 18)
        self.assertEqual(len(report["audit"]), 68)
        self.assertFalse(report["historical_windows"]["last_10"]["complete_window"])
        self.assertEqual(data, original)

    def test_missing_adversarial_coaching_research_blocks_an_otherwise_eligible_pick(self):
        data = eligible_fixture()
        data["evidence"] = [row for row in data["evidence"] if row["rule_id"] != 20]
        self.assert_blocked(data, "Rule 20")

    def test_contradictory_simulation_evidence_is_not_erased_by_a_successful_run(self):
        data = eligible_fixture()
        data["evidence"].append({
            "rule_id": 39, "status": "contradicted",
            "summary": "Fictional test contradiction: source disputes the model assumptions.",
            "sources": [{"url": "https://official.example/contradiction",
                         "published_at": "2026-10-17T11:55:00Z",
                         "retrieved_at": "2026-10-17T11:55:00Z"}],
        })
        report = self.assert_blocked(data, "Rule 39")
        self.assertTrue(report["model"]["simulation"]["executed"])
        self.assertEqual(report["audit"][38]["status"], "contradicted")

    def test_unresolved_usage_restriction_cannot_be_overridden_by_positive_value(self):
        data = eligible_fixture()
        data["player"]["risk"]["usage_restriction_unresolved"] = True
        report = self.assert_blocked(data, "restriction remains unresolved")
        self.assertTrue(report["model"]["recommended_value"])

    def test_synthetic_data_cannot_qualify_even_when_all_other_gates_pass(self):
        data = eligible_fixture()
        data["synthetic"] = True
        self.assert_blocked(data, "Synthetic demonstration data")

    def test_absent_calibration_blocks_a_strong_simulated_edge(self):
        data = eligible_fixture()
        del data["calibration"]
        report = self.assert_blocked(data, "calibration is not qualified")
        self.assertEqual(report["model"]["probability_win"], 1)
        self.assertEqual(report["audit"][49]["status"], "contradicted")

    def test_stale_actual_quote_is_blocked_even_with_fresh_research_citations(self):
        data = eligible_fixture()
        data["market"]["observed_at"] = "2026-10-17T11:44:59Z"
        report = self.assert_blocked(data, "Sportsbook quote is stale")
        self.assertEqual(report["audit"][42]["status"], "documented")

    def test_perfect_heuristic_score_cannot_override_missing_research(self):
        data = eligible_fixture()
        data["scores"] = {name: 10 for name in SCORE_WEIGHTS}
        data["evidence"] = [row for row in data["evidence"] if row["rule_id"] != 21]
        report = self.assert_blocked(data, "Rule 21")
        self.assertEqual(report["heuristic_score"]["value"], 10)
        self.assertFalse(report["heuristic_score"]["is_probability"])

    def test_sunday_four_pm_kickoff_is_outside_the_requested_slate(self):
        data = eligible_fixture()
        data["game"]["kickoff"] = "2026-10-18T20:00:00Z"
        report = self.assert_blocked(data, "outside the Sunday morning")
        self.assertEqual(report["target_window"], "outside_target")

    def test_sunday_morning_kickoff_is_in_scope(self):
        data = eligible_fixture()
        data["game"]["kickoff"] = "2026-10-18T13:30:00Z"
        report = evaluate(data)
        self.assertEqual(report["decision"], "QUALIFIED", report["blockers"])
        self.assertEqual(report["target_window"], "morning")

    def test_future_as_of_cannot_qualify_even_with_internally_consistent_sources(self):
        def move_to_future(value):
            if isinstance(value, dict):
                return {key: move_to_future(item) for key, item in value.items()}
            if isinstance(value, list):
                return [move_to_future(item) for item in value]
            if isinstance(value, str):
                return value.replace("2026-", "2099-")
            return 2099 if type(value) is int and value == 2026 else value

        data = move_to_future(eligible_fixture())
        report = self.assert_blocked(data, "Future research snapshots")
        self.assertTrue(report["model"]["calibration"]["qualified"])

    def test_internally_matching_history_cannot_hide_wrong_nfl_season(self):
        data = eligible_fixture()
        data["game"]["season"] = 2025
        for row in data["history"]:
            row["season"] = 2025
        self.assert_blocked(data, "does not match the game's NFL season")


class LiveEvaluationTests(unittest.TestCase):
    def test_fresh_verified_inputs_and_phase_matched_calibration_traverse_live_gates(self):
        report = evaluate(live_fixture())
        self.assertEqual(report["decision"], "QUALIFIED", report["blockers"])
        self.assertEqual(report["audit_counts"]["not_applicable"], 9)
        self.assertEqual(report["model"]["live"]["production_already_recorded"], 35)
        self.assertEqual(report["model"]["live"]["remaining_needed_to_win"], 6)
        self.assertEqual(report["audit"][53]["status"], "computed")
        self.assertEqual(report["audit"][54]["status"], "computed")
        self.assertEqual(report["audit"][57]["status"], "computed")

    def test_stale_live_snapshot_is_blocked_despite_fresh_quote_and_citations(self):
        data = live_fixture()
        data["live"]["observed_at"] = "2026-10-18T17:58:59Z"
        report = evaluate(data)
        self.assertEqual(report["decision"], "NO_BET")
        self.assertTrue(any("Live game snapshot is stale" in reason for reason in report["blockers"]))

    def test_pregame_calibration_cannot_qualify_a_live_projection(self):
        data = live_fixture()
        data["calibration"]["mode"] = "pregame"
        report = evaluate(data)
        self.assertEqual(report["decision"], "NO_BET")
        self.assertIn("mode", report["model"]["calibration"]["reason"])

    def test_live_requires_remaining_opportunity_scenarios(self):
        data = live_fixture()
        del data["live"]["remaining_opportunities"]
        with self.assertRaisesRegex(ValueError, "remaining_opportunities"):
            evaluate(data)

    def test_live_requires_source_backed_current_availability(self):
        data = live_fixture()
        data["live"]["player_active"] = False
        report = evaluate(data)
        self.assertEqual(report["decision"], "NO_BET")
        self.assertTrue(any("not confirmed active" in reason for reason in report["blockers"]))
        data = live_fixture()
        data["evidence"] = [row for row in data["evidence"] if row["rule_id"] != 53]
        report = evaluate(data)
        self.assertEqual(report["decision"], "NO_BET")
        self.assertTrue(any("Rule 53" in reason for reason in report["blockers"]))


if __name__ == "__main__":
    unittest.main()
