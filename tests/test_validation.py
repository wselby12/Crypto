import copy
from pathlib import Path
import tempfile
import unittest

from engine.validation import load_json, validate
from tests.test_evaluation import eligible_fixture, live_fixture


class ValidationTests(unittest.TestCase):
    def test_valid_snapshot_is_not_mutated(self):
        data = eligible_fixture()
        original = copy.deepcopy(data)
        validate(data)
        self.assertEqual(data, original)

    def test_malformed_enums_and_extreme_numbers_are_value_errors(self):
        mutations = [
            lambda d: d.update(mode=[]),
            lambda d: d["player"].update(position=[]),
            lambda d: d["market"].update(stat=[]),
            lambda d: d["market"].update(american_odds=10**100),
            lambda d: d["game"].update(spread=10**400),
            lambda d: d["market"].update(american_odds=-110.5),
        ]
        for mutate in mutations:
            data = eligible_fixture()
            mutate(data)
            with self.subTest(data=data):
                with self.assertRaises(ValueError):
                    validate(data)

    def test_future_history_and_prior_season_are_rejected(self):
        for field, value in (("played_at", "2099-01-01T00:00:00Z"), ("season", 2025)):
            data = eligible_fixture()
            data["history"][0][field] = value
            with self.assertRaises(ValueError):
                validate(data)

    def test_observed_nfl_totals_and_opportunities_are_integer_counts(self):
        for field in ("production", "opportunities"):
            data = eligible_fixture()
            data["history"][0][field] = 5.4
            with self.assertRaisesRegex(ValueError, "integers"):
                validate(data)

    def test_zero_opportunity_games_and_negative_net_yards_are_supported(self):
        data = eligible_fixture()
        data["history"][0].update(opportunities=0, production=0)
        data["history"][1]["production"] = -2
        validate(data)

    def test_live_score_clock_and_possession_are_validated(self):
        for key, value in (("possession", []), ("quarter", 1.5), ("clock_seconds", 1.5)):
            data = live_fixture()
            data["live"][key] = value
            with self.assertRaises(ValueError):
                validate(data)

    def test_ambiguous_or_nonfinite_json_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            for raw in ('{"as_of":1,"as_of":2}', '{"value":NaN}', '{"value":Infinity}'):
                path.write_text(raw)
                with self.assertRaises(ValueError):
                    load_json(path)
