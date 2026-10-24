from copy import deepcopy
import unittest

from engine.slate import filter_and_rank


AS_OF = "2026-10-24T12:00:00Z"


def game(game_id="early", kickoff="2026-10-25T17:00:00Z", **metrics):
    return {"id": game_id, "kickoff": kickoff, "home_team": "HOME", "away_team": "AWAY",
            "spread": -3, "total": 45, "pace": 65, "data_quality": 0.9, **metrics}


def schedule(*games, complete=False):
    return {"season": 2026, "complete": complete,
            "source": {"url": "https://example.com/official-schedule",
                       "published_at": "2026-05-15T12:00:00Z",
                       "retrieved_at": "2026-10-24T11:00:00Z"},
            "games": list(games)}


class SlateTests(unittest.TestCase):
    def test_only_sunday_morning_and_one_pm_eastern(self):
        value = schedule(game("morning", "2026-10-25T13:30:00Z"), game(),
                         game("late", "2026-10-25T20:05:00Z"),
                         game("saturday", "2026-10-24T17:00:00Z"),
                         game("end", "2026-10-25T18:00:00Z"))
        result = filter_and_rank(value, AS_OF)
        self.assertEqual({g["id"] for g in result["games"]}, {"morning", "early"})
        self.assertEqual(result["windows"]["morning"]["count"], 1)
        self.assertEqual(result["windows"]["early"]["count"], 1)
        self.assertFalse(result["ranking_method"]["is_win_probability"])

    def test_eastern_dst_conversion(self):
        winter = schedule(game("early", "2026-11-08T18:00:00Z"),
                          game("before", "2026-11-08T17:59:00Z"),
                          game("after", "2026-11-08T19:00:00Z"))
        winter["source"]["retrieved_at"] = "2026-11-07T12:00:00Z"
        result = filter_and_rank(winter, "2026-11-07T13:00:00Z")
        self.assertEqual({g["id"]: g["window"] for g in result["games"]},
                         {"early": "early", "before": "morning"})
        self.assertTrue(next(g for g in result["games"] if g["id"] == "early")["kickoff_et"].endswith("-05:00"))

    def test_already_started_is_excluded_including_exact_kickoff(self):
        result = filter_and_rank(schedule(game()), "2026-10-25T17:00:00Z")
        self.assertEqual(result["games"], [])
        self.assertEqual(result["excluded_already_started"], 1)

    def test_no_morning_not_assumed_without_complete_schedule(self):
        result = filter_and_rank(schedule(game()), AS_OF)
        self.assertEqual(result["windows"]["morning"]["status"], "unknown_incomplete_schedule")
        complete_result = filter_and_rank(schedule(game(), complete=True), AS_OF)
        self.assertEqual(complete_result["windows"]["morning"]["status"],
                         "none_upcoming_in_supplied_complete_schedule")
        self.assertEqual(complete_result["source_verification"], "user_supplied_not_independently_verified")

    def test_eligible_games_rank_ahead_of_heavy_spreads(self):
        result = filter_and_rank(schedule(game("bad", spread=-10, total=60),
                                          game("good", spread=-2, total=48),
                                          game("average", spread=5, total=40)), AS_OF)
        self.assertEqual([g["id"] for g in result["games"]], ["good", "average", "bad"])
        self.assertFalse(result["games"][-1]["eligible"])
        self.assertEqual([g["rank"] for g in result["games"]], [1, 2, 3])

    def test_missing_metrics_remain_unknown_not_fabricated(self):
        value = schedule({"id": "missing", "kickoff": "2026-10-25T17:00:00Z",
                          "home_team": "HOME", "away_team": "AWAY"})
        selected = filter_and_rank(value, AS_OF)["games"][0]
        self.assertFalse(selected["eligible"])
        self.assertIsNone(selected["spread"])
        self.assertIsNone(selected["score_components"]["pace"])
        self.assertEqual(set(selected["missing_metrics"]), {"spread", "total", "pace", "data_quality"})

    def test_rejects_future_stale_and_reversed_source_timestamps(self):
        for field, timestamp in (("published_at", "2026-10-24T13:00:00Z"),
                                 ("retrieved_at", "2026-10-24T13:00:00Z"),
                                 ("retrieved_at", "2026-10-20T13:00:00Z")):
            with self.subTest(field=field, timestamp=timestamp):
                value = schedule(game())
                value["source"][field] = timestamp
                with self.assertRaises(ValueError):
                    filter_and_rank(value, AS_OF)
        value = schedule(game())
        value["source"]["published_at"] = "2026-10-24T11:30:00Z"
        with self.assertRaises(ValueError):
            filter_and_rank(value, AS_OF)

    def test_validation_covers_duplicates_naive_dates_and_nonfinite_metrics(self):
        cases = [schedule(game(), game()), schedule(game(kickoff="2026-10-25T13:00:00")),
                 schedule(game(spread=float("nan"))), schedule(game(total=float("inf"))),
                 schedule(game(data_quality=1.1)), schedule(game(pace=-1)),
                 schedule(game(spread=True))]
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    filter_and_rank(value, AS_OF)

    def test_deterministic_ties_do_not_depend_on_input_order(self):
        first = schedule(game("b"), game("a"))
        second = deepcopy(first)
        second["games"].reverse()
        self.assertEqual(filter_and_rank(first, AS_OF), filter_and_rank(second, AS_OF))

    def test_default_target_date_does_not_blend_sundays(self):
        value = schedule(game("this_week"), game("next_week", "2026-11-01T18:00:00Z"))
        result = filter_and_rank(value, AS_OF)
        self.assertEqual(result["target_date"], "2026-10-25")
        self.assertEqual([g["id"] for g in result["games"]], ["this_week"])
        self.assertFalse(result["synthetic"])

    def test_explicit_target_date_and_synthetic_flag(self):
        value = schedule(game("this_week"), game("next_week", "2026-11-01T18:00:00Z"))
        value.update(target_date="2026-11-01", synthetic=True)
        result = filter_and_rank(value, AS_OF)
        self.assertTrue(result["synthetic"])
        self.assertEqual([g["id"] for g in result["games"]], ["next_week"])

    def test_target_date_must_be_a_nonpast_sunday(self):
        for target_date in ("2026-10-24", "2026-10-18", "20261025", "bad", 20261025):
            with self.subTest(target_date=target_date):
                value = schedule(game())
                value["target_date"] = target_date
                with self.assertRaises(ValueError):
                    filter_and_rank(value, AS_OF)


if __name__ == "__main__":
    unittest.main()
