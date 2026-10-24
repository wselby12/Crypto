"""Sunday workflow regressions use fictional or mocked research only."""

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from engine.slate import filter_and_rank
from engine.sunday import run_sunday
from tests.test_evaluation import eligible_fixture


def manifest_fixture():
    first = eligible_fixture()
    first["game"]["id"] = "early"
    first["player"]["id"] = "player-early"
    second = deepcopy(first)
    second["game"].update(id="morning", kickoff="2026-10-18T13:30:00Z", spread=-2.5)
    second["player"]["id"] = "player-morning"
    return {
        "as_of": first["as_of"], "max_selections": 3, "max_per_game": 1,
        "schedule": {
            "season": 2026, "complete": True, "synthetic": False,
            "source": {"url": "https://official.example/schedule",
                       "published_at": "2026-10-17T11:55:00Z", "retrieved_at": "2026-10-17T11:55:00Z"},
            "games": [
                {**first["game"], "total": 45.5, "pace": 65, "data_quality": .8},
                {**second["game"], "total": 47.5, "pace": 65, "data_quality": .8},
            ],
        },
        "candidates": [first, second],
    }


def qualified(payload):
    return {"decision": "QUALIFIED", "blockers": [], "synthetic": payload.get("synthetic", False),
            "current_snapshot": True, "current_until": "2099-01-01T00:00:00Z",
            "model": {"calibration": {"conservative_probability_floor": .75},
                                                  "conservative_unit_expected_value": .2},
            "audit": [{"id": 4, "status": "documented", "reason": "Fictional existing source research"}]}


class SundayTests(unittest.TestCase):
    def test_games_are_ranked_before_any_candidate_and_input_is_preserved(self):
        manifest = manifest_fixture()
        original = deepcopy(manifest)
        calls = []
        def rank(schedule, as_of):
            calls.append("rank")
            return filter_and_rank(schedule, as_of)
        def evaluate(payload):
            calls.append("evaluate")
            return qualified(payload)
        with patch("engine.sunday.filter_and_rank", side_effect=rank), patch("engine.sunday.evaluate", side_effect=evaluate):
            report = run_sunday(manifest)
        self.assertEqual(calls, ["rank", "evaluate", "evaluate"])
        self.assertEqual(manifest, original)
        self.assertEqual([row["game_id"] for row in report["shortlist"]], ["morning", "early"])
        self.assertEqual(report["provenance"]["wagers_placed"], 0)
        self.assertEqual(report["decisions"][0]["report"]["audit"][0]["status"], "documented")

    def test_duplicate_batch_is_rejected_before_any_model_runs(self):
        manifest = manifest_fixture()
        manifest["candidates"].append(deepcopy(manifest["candidates"][0]))
        with patch("engine.sunday.evaluate") as evaluate:
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                run_sunday(manifest)
        evaluate.assert_not_called()

    def test_source_context_mismatches_override_a_positive_model(self):
        mutations = {
            "cutoff": lambda row: row.update(as_of="2026-10-17T12:01:00Z"),
            "kickoff": lambda row: row["game"].update(kickoff="2026-10-18T17:05:00Z"),
            "home_team": lambda row: (row["game"].update(home_team="OTHER"), row["player"].update(team="OTHER")),
            "spread": lambda row: row["game"].update(spread=-4),
            "absent": lambda row: row["game"].update(id="not-in-schedule"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                manifest = manifest_fixture()
                mutate(manifest["candidates"][0])
                with patch("engine.sunday.evaluate", side_effect=qualified):
                    report = run_sunday(manifest)
                candidate = next(entry for entry in report["decisions"] if entry["candidate_index"] == 0)
                self.assertEqual(candidate["report"]["decision"], "NO_BET")
                self.assertTrue(candidate["report"]["blockers"])
                self.assertNotIn(0, [row["candidate_index"] for row in report["shortlist"]])

    def test_scheduled_exclusion_blocks_current_shortlist(self):
        manifest = manifest_fixture()
        manifest["schedule"]["games"][0]["spread"] = -10
        manifest["candidates"][0]["game"]["spread"] = -10
        with patch("engine.sunday.evaluate", side_effect=qualified):
            report = run_sunday(manifest)
        entry = next(row for row in report["decisions"] if row["candidate_index"] == 0)
        self.assertEqual(entry["report"]["decision"], "NO_BET")
        self.assertTrue(any("excluded" in reason for reason in entry["report"]["blockers"]))

    def test_manifest_and_schedule_synthetic_flags_propagate_without_mutation(self):
        for location in ("manifest", "schedule"):
            with self.subTest(location=location):
                manifest = manifest_fixture()
                target = manifest if location == "manifest" else manifest["schedule"]
                target["synthetic"] = True
                original = deepcopy(manifest)
                with patch("engine.sunday.evaluate", side_effect=qualified) as evaluate:
                    report = run_sunday(manifest)
                self.assertTrue(report["synthetic"])
                self.assertTrue(all(call.args[0]["synthetic"] for call in evaluate.call_args_list))
                self.assertTrue(all(row["report"]["decision"] == "NO_BET" for row in report["decisions"]))
                self.assertEqual(report["shortlist"], [])
                self.assertEqual(manifest, original)

    def test_incomplete_schedule_and_historical_snapshots_keep_research_without_actions(self):
        for kind in ("incomplete", "historical"):
            with self.subTest(kind=kind):
                manifest = manifest_fixture()
                if kind == "incomplete":
                    manifest["schedule"]["complete"] = False
                def evaluate(payload):
                    result = qualified(payload)
                    if kind == "historical":
                        result["current_snapshot"] = False
                    return result
                with patch("engine.sunday.evaluate", side_effect=evaluate):
                    report = run_sunday(manifest)
                self.assertEqual(report["overview"]["qualified_research_count"], 2)
                self.assertEqual(report["shortlist"], [])
                self.assertEqual(len(report["deferred"]), 2)

    def test_concentration_limits_do_not_create_same_player_or_game_stacks(self):
        manifest = manifest_fixture()
        extra = deepcopy(manifest["candidates"][1])
        extra["player"]["id"] = "another-morning-player"
        extra["market"]["line"] = 30.5
        duplicate_player = deepcopy(manifest["candidates"][0])
        duplicate_player["player"]["id"] = "player-morning"
        duplicate_player["market"]["line"] = 20.5
        manifest["candidates"].extend([extra, duplicate_player])
        with patch("engine.sunday.evaluate", side_effect=qualified):
            report = run_sunday(manifest)
        self.assertEqual(len(report["shortlist"]), 2)
        self.assertEqual(len({row["game_id"] for row in report["shortlist"]}), 2)
        self.assertEqual(len({row["player_id"] for row in report["shortlist"]}), 2)
        self.assertTrue(any("concentration" in row["reason"] for row in report["deferred"]))

    def test_limit_and_missing_candidate_coverage_are_explicit(self):
        manifest = manifest_fixture()
        manifest["max_selections"] = 1
        with patch("engine.sunday.evaluate", side_effect=qualified):
            report = run_sunday(manifest)
        self.assertEqual(len(report["shortlist"]), 1)
        self.assertTrue(any("limit reached" in row["reason"] for row in report["deferred"]))
        manifest["candidates"] = []
        report = run_sunday(manifest)
        self.assertEqual(report["coverage"]["eligible_game_count"], 2)
        self.assertEqual(set(report["coverage"]["games_without_candidates"]), {"early", "morning"})
        self.assertEqual(report["shortlist"], [])

    def test_one_player_cannot_fill_multiple_slots_with_alternate_lines_or_id_whitespace(self):
        manifest = manifest_fixture()
        manifest["max_per_game"] = 3
        manifest["candidates"] = manifest["candidates"][:1]
        alternate = deepcopy(manifest["candidates"][0])
        alternate["market"]["line"] = 30.5
        alternate["player"]["id"] = " " + alternate["player"]["id"] + " "
        manifest["candidates"].append(alternate)
        with patch("engine.sunday.evaluate", side_effect=qualified):
            report = run_sunday(manifest)
        self.assertEqual(len(report["shortlist"]), 1)
        self.assertTrue(any("Player concentration" in row["reason"] for row in report["deferred"]))

    def test_live_candidates_and_invalid_limits_are_schema_errors(self):
        manifest = manifest_fixture()
        for value in (True, 0, 11, 1.5):
            manifest["max_selections"] = value
            with self.assertRaises(ValueError):
                run_sunday(manifest)
        manifest = manifest_fixture()
        manifest["candidates"][0]["mode"] = "live"
        with self.assertRaises(ValueError):
            run_sunday(manifest)

    def test_real_synthetic_fixture_keeps_every_no_bet_decision(self):
        root = Path(__file__).resolve().parents[1]
        schedule = json.loads((root / "examples/synthetic_schedule.json").read_text())
        candidate = json.loads((root / "examples/synthetic_candidate.json").read_text())
        candidate["projection"]["simulations"] = 1000
        manifest = {"as_of": candidate["as_of"], "schedule": schedule, "candidates": [candidate]}
        report = run_sunday(manifest)
        self.assertEqual(report["overview"]["no_bet_count"], 1)
        self.assertEqual(report["shortlist"], [])
        self.assertTrue(report["decisions"][0]["report"]["model"]["simulation"]["executed"])
        self.assertEqual(report["decisions"][0]["candidate_index"], 0)

    def test_missing_malformed_and_expired_deadlines_defer_current_looking_snapshots(self):
        selected_at = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)
        for expiry in (None, "2026-10-17T13:00:00", "not-a-date", "2026-10-17T12:00:00Z"):
            with self.subTest(expiry=expiry):
                def evaluate(payload):
                    result = qualified(payload)
                    result["current_until"] = expiry
                    return result
                with patch("engine.sunday.datetime") as clock, patch("engine.sunday.evaluate", side_effect=evaluate):
                    clock.now.return_value = selected_at
                    report = run_sunday(manifest_fixture())
                self.assertEqual(report["shortlist"], [])
                self.assertEqual(report["overview"]["qualified_research_count"], 2)
                self.assertTrue(all(not row["report"]["sunday_context"]["current_at_selection"] for row in report["decisions"]))

    def test_later_evaluation_cannot_keep_an_earlier_expired_quote_active(self):
        clock_time = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)
        def evaluate(payload):
            nonlocal clock_time
            result = qualified(payload)
            if payload["game"]["id"] == "early":
                result["current_until"] = "2026-10-17T12:01:00Z"
            else:
                # The second candidate remains current, but its evaluation took
                # long enough for the first candidate to expire.
                clock_time = datetime(2026, 10, 17, 12, 2, tzinfo=timezone.utc)
                result["current_until"] = "2026-10-17T12:05:00Z"
            return result
        with patch("engine.sunday.datetime") as clock, patch("engine.sunday.evaluate", side_effect=evaluate):
            clock.now.side_effect = lambda tz: clock_time
            report = run_sunday(manifest_fixture())
        self.assertEqual(report["selection_at"], "2026-10-17T12:02:00+00:00")
        self.assertEqual([row["game_id"] for row in report["shortlist"]], ["morning"])
        self.assertEqual(report["shortlist"][0]["current_until"], "2026-10-17T12:05:00+00:00")
        earlier = next(row for row in report["decisions"] if row["candidate_index"] == 0)
        self.assertEqual(earlier["report"]["decision"], "QUALIFIED")
        self.assertIn("expired", earlier["selection_reason"])

    def test_full_engine_current_snapshots_create_a_reproducible_shortlist(self):
        selected_at = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)
        with patch("engine.evaluation.datetime") as evaluation_clock, patch("engine.sunday.datetime") as selection_clock:
            evaluation_clock.now.return_value = selected_at
            selection_clock.now.return_value = selected_at
            report = run_sunday(manifest_fixture())
        self.assertEqual(report["overview"]["shortlisted_count"], 2)
        self.assertEqual([row["game_id"] for row in report["shortlist"]], ["morning", "early"])
        self.assertTrue(all(row["current_until"] == "2026-10-17T12:10:00+00:00" for row in report["shortlist"]))
        for row in report["decisions"]:
            research = row["report"]
            self.assertEqual(research["decision"], "QUALIFIED", research["blockers"])
            self.assertTrue(research["current_snapshot"])
            self.assertTrue(research["sunday_context"]["current_at_selection"])
            self.assertTrue(research["model"]["simulation"]["executed"])
            self.assertEqual(research["model"]["simulation"]["draws"], 1000)
            self.assertTrue(research["model"]["calibration"]["qualified"])
            self.assertTrue(research["robustness"]["qualified"])
            self.assertIn("not independently fetched", report["provenance"]["source_verification"])


if __name__ == "__main__":
    unittest.main()
