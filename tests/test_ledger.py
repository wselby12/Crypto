from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from engine.ledger import Ledger


def payload(stat="rushing_yards", line=49.5, odds=-110):
    return {
        "as_of": "2025-10-12T16:00:00Z",
        "game": {"id": "game-1", "kickoff": "2025-10-12T17:00:00Z"},
        "player": {"id": "player-1"},
        "market": {"stat": stat, "side": "over", "line": line, "american_odds": odds, "sportsbook": "Test Book"},
    }


def report(decision="QUALIFIED"):
    return {"decision": decision, "blocking_reasons": [] if decision == "QUALIFIED" else ["missing coaching evidence"]}


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.ledger = Ledger(":memory:")

    def tearDown(self):
        self.ledger.close()

    def settle(self, identifier, result, **kwargs):
        return self.ledger.settle(identifier, result, "https://www.nfl.com/result", "2025-10-12T21:00:00Z", **kwargs)

    def test_record_preserves_originals_and_does_not_place_wager(self):
        original_payload = payload()
        original_report = report()
        identifier = self.ledger.record(original_payload, original_report)
        original_payload["market"]["line"] = 99.5
        original_report["decision"] = "NO_BET"
        stored = self.ledger.get(identifier)
        self.assertEqual(stored["payload"]["market"]["line"], 49.5)
        self.assertEqual(stored["report"]["decision"], "QUALIFIED")
        self.assertEqual(len(stored["input_sha256"]), 64)
        self.assertEqual(len(stored["report_sha256"]), 64)
        self.assertIsNone(stored["settlement"])
        self.assertEqual(self.ledger.performance()["settled_bets"], 0)

    def test_roi_and_hit_rate_distinguish_pushes(self):
        win = self.ledger.record(payload(line=50), report())
        loss = self.ledger.record(payload(line=50), report())
        push = self.ledger.record(payload(line=50), report())
        self.settle(win, 60, stake_units=2)
        self.settle(loss, 40, stake_units=1)
        pushed = self.settle(push, 50, stake_units=3)
        self.assertEqual(pushed["net_units"], 0)
        totals = self.ledger.performance()
        self.assertEqual((totals["wins"], totals["losses"], totals["pushes"]), (1, 1, 1))
        self.assertAlmostEqual(totals["net_units"], 2 * 100 / 110 - 1)
        self.assertEqual(totals["total_stake_units"], 6)
        self.assertAlmostEqual(totals["roi"], (2 * 100 / 110 - 1) / 6)
        self.assertEqual(totals["hit_rate"], 0.5)
        self.assertEqual(totals["by_category"]["rushing_yards"]["settled_bets"], 3)

    def test_rejected_research_does_not_inflate_betting_results(self):
        identifier = self.ledger.record(payload(), report("NO_BET"))
        with self.assertRaisesRegex(ValueError, "zero for research"):
            self.settle(identifier, 90)
        result = self.settle(identifier, 90, stake_units=0)
        self.assertEqual(result["outcome"], "win")
        totals = self.ledger.performance()
        self.assertEqual(totals["rejected_decisions"], 1)
        self.assertEqual(totals["research_settlements"], 1)
        self.assertEqual(totals["settled_bets"], 0)
        self.assertEqual(totals["rejection_reasons"], {"missing coaching evidence": 1})
        self.assertIsNone(totals["roi"])

    def test_duplicate_settlement_does_not_overwrite(self):
        identifier = self.ledger.record(payload(), report())
        first = self.settle(identifier, 60)
        with self.assertRaisesRegex(ValueError, "already settled"):
            self.settle(identifier, 40)
        self.assertEqual(self.ledger.get(identifier)["settlement"], first)

    def test_failure_causes_are_explicit_and_counted(self):
        identifier = self.ledger.record(payload(), report())
        result = self.settle(identifier, 30, failure_cause="coaching")
        self.assertEqual(result["failure_cause"], "coaching")
        self.assertEqual(self.ledger.performance()["failure_causes"], {"coaching": 1})
        other = self.ledger.record(payload(), report())
        with self.assertRaisesRegex(ValueError, "losing result"):
            self.settle(other, 60, failure_cause="injury")

    def test_negative_official_yardage_is_a_valid_loss(self):
        identifier = self.ledger.record(payload(stat="rushing_yards"), report())
        result = self.settle(identifier, -4)
        self.assertEqual(result["official_result"], -4)
        self.assertEqual(result["outcome"], "loss")
        self.assertEqual(result["net_units"], -1)

    def test_count_and_fractional_official_stats_are_rejected(self):
        count = self.ledger.record(payload(stat="receptions", line=5.5), report())
        with self.assertRaisesRegex(ValueError, "negative only"):
            self.settle(count, -1)
        yards = self.ledger.record(payload(), report())
        with self.assertRaisesRegex(ValueError, "integer"):
            self.settle(yards, 55.5)

    def test_synthetic_positive_stake_is_forbidden(self):
        synthetic = payload()
        synthetic["synthetic"] = True
        identifier = self.ledger.record(synthetic, report())
        with self.assertRaisesRegex(ValueError, "nonsynthetic"):
            self.settle(identifier, 60)
        self.settle(identifier, 60, stake_units=0)
        self.assertEqual(self.ledger.performance()["settled_bets"], 0)

    def test_clv_compares_prices_only_at_same_line(self):
        same = self.ledger.record(payload(line=50), report())
        different = self.ledger.record(payload(line=50), report())
        result = self.settle(same, 60, closing_line=50, closing_american_odds=-130)
        self.assertEqual(result["closing_line_value_points"], 0)
        self.assertAlmostEqual(result["closing_implied_probability_minus_initial"], 130 / 230 - 110 / 210)
        result = self.settle(different, 60, closing_line=52, closing_american_odds=-130)
        self.assertEqual(result["closing_line_value_points"], 2)
        self.assertIsNone(result["closing_implied_probability_minus_initial"])

    def test_missing_closing_line_does_not_invent_comparability(self):
        identifier = self.ledger.record(payload(), report())
        with self.assertRaisesRegex(ValueError, "closing_line is required"):
            self.settle(identifier, 60, closing_american_odds=-130)

    def test_settlement_rejects_invalid_source_time_numbers(self):
        identifier = self.ledger.record(payload(), report())
        invalid = [
            {"official_result": float("nan")}, {"stake_units": -1}, {"stake_units": True},
            {"source_url": "http://example.com"}, {"source_url": "https://user:pass@example.com"},
            {"settled_at": "2025-10-12T16:30:00Z"}, {"settled_at": "2025-10-12T21:00:00"},
            {"settled_at": "2999-10-12T21:00:00Z"},
        ]
        base = dict(official_result=60, source_url="https://www.nfl.com/result", settled_at="2025-10-12T21:00:00Z")
        for bad in invalid:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.ledger.settle(identifier, **(base | bad))

    def test_unrepresentable_json_integers_are_validation_errors(self):
        malformed = payload()
        malformed["market"]["line"] = 10 ** 400
        with self.assertRaisesRegex(ValueError, "finite number"):
            self.ledger.record(malformed, report())
        identifier = self.ledger.record(payload(), report())
        with self.assertRaisesRegex(ValueError, "finite number"):
            self.settle(identifier, 60, stake_units=10 ** 400)

    def test_odds_outside_sqlite_integer_range_are_validation_errors(self):
        for value in [2 ** 63, -(2 ** 63) - 1, 10 ** 100]:
            with self.subTest(odds=value):
                malformed = payload(odds=value)
                with self.assertRaisesRegex(ValueError, "signed 64-bit"):
                    self.ledger.record(malformed, report())
        identifier = self.ledger.record(payload(), report())
        with self.assertRaisesRegex(ValueError, "signed 64-bit"):
            self.settle(identifier, 60, closing_line=49.5, closing_american_odds=2 ** 63)

    def test_hash_verification_detects_changed_snapshot(self):
        identifier = self.ledger.record(payload(), report())
        self.ledger._connection.execute("UPDATE decisions SET input_json = ? WHERE id = ?", ("{}", identifier))
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.ledger.get(identifier)
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.ledger.performance()

    def test_settlement_digest_detects_changed_results_before_accounting(self):
        identifier = self.ledger.record(payload(), report())
        self.assertEqual(self.ledger.get(identifier)["settlement_integrity"], "unsettled")
        original = self.settle(identifier, 60)
        self.assertEqual(self.ledger.get(identifier)["settlement_integrity"], "verified")
        changed = dict(original, net_units=999)
        self.ledger._connection.execute(
            "UPDATE settlements SET settlement_json = ? WHERE decision_id = ?",
            (json.dumps(changed), identifier),
        )
        with self.assertRaisesRegex(ValueError, "Settlement snapshot integrity"):
            self.ledger.get(identifier)
        with self.assertRaisesRegex(ValueError, "Settlement snapshot integrity"):
            self.ledger.performance()

    def test_legacy_settlement_migration_retains_unverified_status_without_backfill(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "legacy-settlements.sqlite3")
            with Ledger(path) as ledger:
                identifier = ledger.record(payload(), report())
                original = ledger.settle(identifier, 60, "https://www.nfl.com/result", "2025-10-12T21:00:00Z")
            # Reconstruct the original two-column table as an existing v0.1
            # ledger, preserving the historical financial record verbatim.
            with sqlite3.connect(path) as connection:
                connection.executescript("""
                    ALTER TABLE settlements RENAME TO newer_settlements;
                    CREATE TABLE settlements (
                        decision_id TEXT PRIMARY KEY REFERENCES decisions(id),
                        settlement_json TEXT NOT NULL
                    );
                    INSERT INTO settlements SELECT decision_id, settlement_json FROM newer_settlements;
                    DROP TABLE newer_settlements;
                """)
            with Ledger(path) as ledger:
                stored = ledger.get(identifier)
                self.assertEqual(stored["settlement"], original)
                self.assertEqual(stored["settlement_integrity"], "legacy_unverified")
                totals = ledger.performance()
                self.assertEqual(totals["settled_bets"], 1)
                self.assertEqual(totals["legacy_unverified_settlements"], 1)
                self.assertAlmostEqual(totals["net_units"], 100 / 110)
                self.assertIsNone(ledger._connection.execute(
                    "SELECT settlement_sha256 FROM settlements WHERE decision_id = ?", (identifier,)
                ).fetchone()[0])
                new_identifier = ledger.record(payload(), report())
                ledger.settle(new_identifier, 40, "https://www.nfl.com/result", "2025-10-12T21:00:00Z")
                self.assertEqual(ledger.get(new_identifier)["settlement_integrity"], "verified")
                self.assertEqual(ledger.performance()["legacy_unverified_settlements"], 1)

    def test_disk_persistence_and_category_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "nested" / "results.sqlite3")
            with Ledger(path) as ledger:
                identifier = ledger.record(payload(stat="receptions", line=5.5, odds=150), report())
            with Ledger(path) as ledger:
                self.assertEqual(ledger.get(identifier)["payload"]["market"]["stat"], "receptions")
                result = ledger.settle(identifier, 6, "https://www.nfl.com/result", "2025-10-12T21:00:00Z", stake_units=2)
                self.assertEqual(result["net_units"], 3)
                self.assertEqual(ledger.performance()["by_category"]["receptions"]["roi"], 1.5)

    def test_invalid_payload_cannot_be_recorded(self):
        malformed = payload()
        malformed["market"]["american_odds"] = -90
        with self.assertRaises(ValueError):
            self.ledger.record(malformed, report())
        malformed = payload()
        malformed["extra"] = float("inf")
        with self.assertRaises(ValueError):
            self.ledger.record(malformed, report())


def sunday_run():
    first, second = payload(), payload()
    second["player"]["id"] = "player-2"
    manifest = {"schedule": {"games": [{"id": "game-1"}], "complete": True},
                "candidates": [first, second]}
    result = {"decisions": [{"candidate_index": 1, "report": report("NO_BET")},
                            {"candidate_index": 0, "report": report("NO_BET")}],
              "shortlist": []}
    return manifest, result


class RunLedgerTests(unittest.TestCase):
    def setUp(self):
        self.ledger = Ledger(":memory:")

    def tearDown(self):
        self.ledger.close()

    def counts(self):
        return tuple(self.ledger._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                     for table in ("runs", "decisions", "run_decisions"))

    def test_entire_run_and_every_rejection_are_preserved_without_wagers(self):
        manifest, result = sunday_run()
        original_manifest, original_result = deepcopy(manifest), deepcopy(result)
        receipt = self.ledger.record_run(manifest, result)
        self.assertEqual(manifest, original_manifest)
        self.assertEqual(result, original_result)
        stored = self.ledger.get_run(receipt["run_id"])
        self.assertEqual(stored["manifest"], original_manifest)
        self.assertEqual(stored["result"], original_result)
        self.assertEqual(stored["decision_ids"], receipt["decision_ids"])
        self.assertEqual([row["candidate_index"] for row in stored["decisions"]], [1, 0])
        self.assertEqual([row["player_id"] for row in stored["decisions"]], ["player-2", "player-1"])
        self.assertEqual(stored["input_sha256"], receipt["input_sha256"])
        self.assertEqual(stored["report_sha256"], receipt["report_sha256"])
        self.assertEqual(len(receipt["input_sha256"]), 64)
        self.assertTrue(all(row["created_at"] == stored["created_at"] for row in stored["decisions"]))
        manifest["candidates"][0]["market"]["line"] = 999
        result["decisions"][0]["report"]["decision"] = "QUALIFIED"
        self.assertEqual(self.ledger.get_run(receipt["run_id"])["manifest"], original_manifest)
        performance = self.ledger.performance()
        self.assertEqual(performance["total_decisions"], 2)
        self.assertEqual(performance["rejected_decisions"], 2)
        self.assertEqual(performance["settled_bets"], 0)

    def test_malformed_indices_or_missing_candidates_do_not_insert_partial_runs(self):
        variants = []
        for indices in ([0, 0], [0, 2], [False, 1], [0, "1"], [1]):
            manifest, result = sunday_run()
            result["decisions"] = [{"candidate_index": index, "report": report("NO_BET")} for index in indices]
            variants.append((manifest, result))
        manifest, result = sunday_run()
        manifest["candidates"][0]["market"]["american_odds"] = -90
        variants.append((manifest, result))
        manifest, result = sunday_run()
        manifest["schedule"]["bad_number"] = float("nan")
        variants.append((manifest, result))
        for manifest, result in variants:
            with self.subTest(manifest=manifest, result=result):
                with self.assertRaises(ValueError):
                    self.ledger.record_run(manifest, result)
                self.assertEqual(self.counts(), (0, 0, 0))

    def test_database_failure_rolls_back_run_decisions_and_links_together(self):
        original = self.ledger.record(payload(), report())
        self.ledger._connection.executescript("""
            CREATE TRIGGER reject_second_link BEFORE INSERT ON run_decisions
            WHEN NEW.position = 1
            BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END;
        """)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "simulated write failure"):
            self.ledger.record_run(*sunday_run())
        self.assertEqual(self.counts(), (0, 1, 0))
        self.assertEqual(self.ledger.get(original)["decision"], "QUALIFIED")

    def test_original_and_child_snapshot_corruption_are_detected(self):
        receipt = self.ledger.record_run(*sunday_run())
        self.ledger._connection.execute("UPDATE runs SET input_json = '{}' WHERE id = ?", (receipt["run_id"],))
        with self.assertRaisesRegex(ValueError, "Run snapshot integrity"):
            self.ledger.get_run(receipt["run_id"])
        receipt = self.ledger.record_run(*sunday_run())
        self.ledger._connection.execute("UPDATE decisions SET report_json = '{}' WHERE id = ?", (receipt["decision_ids"][0],))
        with self.assertRaisesRegex(ValueError, "Decision snapshot integrity"):
            self.ledger.get_run(receipt["run_id"])

    def test_missing_or_changed_run_links_are_detected(self):
        receipt = self.ledger.record_run(*sunday_run())
        self.ledger._connection.execute("UPDATE run_decisions SET candidate_index = 99 WHERE run_id = ? AND position = 0", (receipt["run_id"],))
        with self.assertRaisesRegex(ValueError, "link integrity"):
            self.ledger.get_run(receipt["run_id"])
        receipt = self.ledger.record_run(*sunday_run())
        self.ledger._connection.execute("DELETE FROM run_decisions WHERE run_id = ? AND position = 0", (receipt["run_id"],))
        with self.assertRaisesRegex(ValueError, "link integrity"):
            self.ledger.get_run(receipt["run_id"])

    def test_changed_linked_settlement_is_detected_by_run_retrieval(self):
        receipt = self.ledger.record_run(*sunday_run())
        identifier = receipt["decision_ids"][0]
        settled = self.ledger.settle(identifier, 60, "https://www.nfl.com/result", "2025-10-12T21:00:00Z", stake_units=0)
        self.assertEqual(self.ledger.get_run(receipt["run_id"])["decisions"][0]["settlement_integrity"], "verified")
        self.ledger._connection.execute(
            "UPDATE settlements SET settlement_json = ? WHERE decision_id = ?",
            (json.dumps(dict(settled, official_result=40)), identifier),
        )
        with self.assertRaisesRegex(ValueError, "Settlement snapshot integrity"):
            self.ledger.get_run(receipt["run_id"])

    def test_existing_database_is_upgraded_without_changing_standalone_decisions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "old-ledger.sqlite3")
            with Ledger(path) as ledger:
                decision_id = ledger.record(payload(), report())
                original = ledger.get(decision_id)
                ledger._connection.executescript("DROP TABLE run_decisions; DROP TABLE runs;")
            with Ledger(path) as ledger:
                self.assertEqual(ledger.get(decision_id), original)
                receipt = ledger.record_run(*sunday_run())
            with Ledger(path) as ledger:
                self.assertEqual(len(ledger.get_run(receipt["run_id"])["decisions"]), 2)
                self.assertEqual(ledger.performance()["total_decisions"], 3)

    def test_zero_candidate_run_is_a_snapshot_with_no_invented_decisions(self):
        receipt = self.ledger.record_run({"schedule": {}, "candidates": []}, {"decisions": [], "shortlist": []})
        self.assertEqual(receipt["decision_ids"], [])
        self.assertEqual(self.ledger.get_run(receipt["run_id"])["decisions"], [])
        self.assertEqual(self.counts(), (1, 0, 0))

    def test_synthetic_schedule_report_prevents_child_stake_accounting(self):
        manifest, result = sunday_run()
        result["decisions"][0]["report"].update(decision="QUALIFIED", synthetic=True)
        receipt = self.ledger.record_run(manifest, result)
        with self.assertRaisesRegex(ValueError, "nonsynthetic"):
            self.ledger.settle(receipt["decision_ids"][0], 60, "https://www.nfl.com/result", "2025-10-12T21:00:00Z", stake_units=1)

    def test_unknown_run_is_an_explicit_error(self):
        with self.assertRaisesRegex(ValueError, "Unknown run"):
            self.ledger.get_run("missing")


if __name__ == "__main__":
    unittest.main()
