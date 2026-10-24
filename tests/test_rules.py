import copy
import unittest

from engine.rules import RULES, evaluate_evidence, exclusions


def payload():
    return {
        "mode": "pregame",
        "as_of": "2026-10-25T12:00:00Z",
        "game": {"kickoff": "2026-10-25T17:00:00Z", "spread": -3.5},
        "player": {
            "stable_games": 5,
            "risk": {
                "injury_designation_last_14_days": False,
                "missed_game_last_4_weeks": False,
                "final_status_confirmed": True,
                "qb_changed_last_2_weeks": False,
                "playcaller_changed_last_2_weeks": False,
                "usage_restriction_unresolved": False,
            },
        },
        "history": [
            {"played_at": f"2026-10-{day:02d}T17:00:00Z", "snap_share": share}
            for day, share in [(1, .72), (8, .73), (15, .72), (22, .74)]
        ],
        "evidence": [],
    }


def citation(rule_id=20):
    return {
        "rule_id": rule_id,
        "status": "supported",
        "summary": "Direct interview records no usage restriction; opposing evidence reviewed.",
        "sources": [{
            "url": "https://www.nfl.com/news/source-document",
            "published_at": "2026-10-25T11:50:00Z",
            "retrieved_at": "2026-10-25T11:55:00Z",
        }],
    }


class EvidenceTests(unittest.TestCase):
    def record(self, data, rule_id):
        return next(row for row in evaluate_evidence(data) if row["id"] == rule_id)

    def test_exactly_68_unique_rules_with_correct_stage_boundaries(self):
        self.assertEqual([rule["id"] for rule in RULES], list(range(1, 69)))
        self.assertEqual(RULES[49]["stage"], "pregame")
        self.assertEqual(RULES[50]["stage"], "live")
        self.assertEqual(RULES[58]["stage"], "live")
        self.assertEqual(RULES[59]["stage"], "results")
        self.assertIn("contradict", RULES[19]["title"])

    def test_absence_is_missing_and_later_stages_are_not_applicable(self):
        records = evaluate_evidence(payload())
        self.assertEqual(len(records), 68)
        self.assertEqual(records[19]["status"], "missing")
        self.assertTrue(all(row["status"] == "not_applicable" for row in records[50:]))

    def test_valid_citation_is_documented_not_verified(self):
        data = payload()
        data["evidence"] = [citation()]
        self.assertEqual(self.record(data, 20)["status"], "documented")

    def test_computed_rules_cannot_be_passed_by_attestation(self):
        data = payload()
        data["evidence"] = [citation(39)]
        record = self.record(data, 39)
        self.assertEqual(record["status"], "missing")
        self.assertIn("computation", record["reason"])

    def test_future_publication_or_retrieval_cannot_leak_into_a_decision(self):
        for field in ["published_at", "retrieved_at"]:
            with self.subTest(field=field):
                data = payload()
                item = citation()
                item["sources"][0][field] = "2026-10-25T12:00:01Z"
                data["evidence"] = [item]
                record = self.record(data, 20)
                self.assertEqual(record["status"], "missing")
                self.assertIn("after the decision", record["reason"])

    def test_stale_reporting_cannot_be_refreshed_by_retrieving_it_today(self):
        data = payload()
        item = citation()
        item["sources"][0]["published_at"] = "2026-10-20T11:55:00Z"
        data["evidence"] = [item]
        self.assertIn("publication is stale", self.record(data, 20)["reason"])

    def test_historical_statistics_may_have_older_publications_but_recent_retrieval(self):
        data = payload()
        item = citation(6)
        item["sources"][0]["published_at"] = "2026-09-20T11:55:00Z"
        data["evidence"] = [item]
        self.assertEqual(self.record(data, 6)["status"], "documented")
        item["sources"][0]["retrieved_at"] = "2026-10-20T11:55:00Z"
        self.assertIn("retrieval is stale", self.record(data, 6)["reason"])

    def test_market_quotes_expire_after_fifteen_minutes(self):
        data = payload()
        item = citation(43)
        item["sources"][0]["published_at"] = "2026-10-25T11:44:59Z"
        item["sources"][0]["retrieved_at"] = "2026-10-25T11:44:59Z"
        data["evidence"] = [item]
        self.assertEqual(self.record(data, 43)["status"], "missing")

    def test_live_state_expires_after_sixty_seconds(self):
        data = payload()
        data["mode"] = "live"
        item = citation(51)
        item["sources"][0]["published_at"] = "2026-10-25T11:58:59Z"
        item["sources"][0]["retrieved_at"] = "2026-10-25T11:58:59Z"
        data["evidence"] = [item]
        self.assertEqual(self.record(data, 51)["status"], "missing")
        self.assertNotEqual(self.record(data, 51)["status"], "not_applicable")

    def test_contradiction_is_preserved_even_when_unproven(self):
        data = payload()
        item = citation()
        item["status"] = "contradicted"
        item["sources"] = []
        data["evidence"] = [item]
        self.assertEqual(self.record(data, 20)["status"], "contradicted")

    def test_malformed_contradiction_summary_preserves_block_without_crashing(self):
        for summary in [123, [], {}, None, "  "]:
            with self.subTest(summary=summary):
                data = payload()
                item = citation()
                item.update(status="contradicted", summary=summary, sources=[])
                data["evidence"] = [item]
                record = self.record(data, 20)
                self.assertEqual(record["status"], "contradicted")
                self.assertIsInstance(record["reason"], str)
                self.assertIn("provenance", record["reason"])

    def test_unhashable_mode_or_evidence_status_raise_schema_error(self):
        data = payload()
        data["mode"] = []
        with self.assertRaisesRegex(ValueError, "mode"):
            evaluate_evidence(data)
        data = payload()
        item = citation()
        item["status"] = []
        data["evidence"] = [item]
        with self.assertRaisesRegex(ValueError, "status"):
            evaluate_evidence(data)

    def test_invalid_citations_do_not_pass(self):
        modifications = [
            ("url", "http://www.nfl.com/news/source"),
            ("url", "https://user:password@www.nfl.com/news/source"),
            ("url", "https:///missing-host"),
            ("published_at", None),
            ("retrieved_at", "2026-10-25T11:55:00"),
            ("retrieved_at", "2026-10-25T11:49:00Z"),
        ]
        for field, value in modifications:
            with self.subTest(field=field, value=value):
                data = payload()
                item = citation()
                item["sources"][0][field] = value
                data["evidence"] = [item]
                self.assertEqual(self.record(data, 20)["status"], "missing")

    def test_a_future_second_source_is_not_hidden_by_a_valid_first_source(self):
        data = payload()
        item = citation()
        future_source = copy.deepcopy(item["sources"][0])
        future_source["retrieved_at"] = "2026-10-25T13:00:00Z"
        item["sources"].append(future_source)
        data["evidence"] = [item]
        self.assertEqual(self.record(data, 20)["status"], "missing")

    def test_duplicate_and_invalid_ids_and_naive_decision_times_are_schema_errors(self):
        data = payload()
        data["evidence"] = [citation(), citation()]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            evaluate_evidence(data)
        for rule_id in [0, 69, True, "20"]:
            with self.subTest(rule_id=rule_id):
                data["evidence"] = [citation(rule_id)]
                with self.assertRaises(ValueError):
                    evaluate_evidence(data)
        data = payload()
        data["as_of"] = "2026-10-25T12:00:00"
        with self.assertRaisesRegex(ValueError, "timezone"):
            evaluate_evidence(data)


class ExclusionTests(unittest.TestCase):
    def test_clear_known_risks_and_stable_workload_have_no_hard_exclusion(self):
        self.assertEqual(exclusions(payload()), [])

    def test_usage_restriction_is_an_independent_hard_block(self):
        data = payload()
        data["player"]["risk"]["usage_restriction_unresolved"] = True
        self.assertTrue(any("restriction remains unresolved" in reason for reason in exclusions(data)))

    def test_unknown_risk_cannot_default_to_false(self):
        for unknown in [None, 0, "false"]:
            with self.subTest(unknown=unknown):
                data = payload()
                data["player"]["risk"]["injury_designation_last_14_days"] = unknown
                self.assertTrue(any("Unknown required risk" in reason for reason in exclusions(data)))

    def test_injury_missed_games_and_unconfirmed_final_status_are_blocks(self):
        for key, value in [("injury_designation_last_14_days", True),
                           ("missed_game_last_4_weeks", True),
                           ("final_status_confirmed", False),
                           ("qb_changed_last_2_weeks", True),
                           ("playcaller_changed_last_2_weeks", True)]:
            with self.subTest(key=key):
                data = payload()
                data["player"]["risk"][key] = value
                self.assertNotEqual(exclusions(data), [])

    def test_three_stable_games_and_spread_boundaries(self):
        data = payload()
        data["player"]["stable_games"] = 2
        self.assertTrue(any("Fewer than three" in reason for reason in exclusions(data)))
        for spread in [-10, 10, -14, 14]:
            data = payload()
            data["game"]["spread"] = spread
            self.assertTrue(any("spread is at least 10" in reason for reason in exclusions(data)))

    def test_volatile_or_missing_snap_shares_are_blocks(self):
        data = payload()
        for game, share in zip(data["history"], [.4, .9, .4, .9]):
            game["snap_share"] = share
        self.assertTrue(any("standard deviation" in reason for reason in exclusions(data)))
        del data["history"][0]["snap_share"]
        self.assertTrue(any("must be known" in reason for reason in exclusions(data)))

    def test_chronological_last_four_snap_shares_used(self):
        data = payload()
        data["history"].append({"played_at": "2026-09-01T17:00:00Z", "snap_share": .1})
        self.assertEqual(exclusions(data), [])


if __name__ == "__main__":
    unittest.main()
