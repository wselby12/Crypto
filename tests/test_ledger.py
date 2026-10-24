from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
