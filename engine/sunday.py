"""A provenance-preserving, single-selection Sunday research workflow.

Games are ranked before any player is evaluated. A shortlist is conditional on
the supplied schedule and research snapshot; no bet or joint probability is
created, and citation contents remain externally researched inputs.
"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import math

from .evaluation import evaluate
from .slate import filter_and_rank
from .validation import object_value, timestamp, validate


def _limit(value: object, name: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def _identity(payload: dict) -> tuple:
    market = payload["market"]
    return (payload["game"]["id"].strip(), payload["player"]["id"].strip(),
            market["stat"], market["line"], market["sportsbook"].strip().casefold())


def _ranking_number(value: object) -> float:
    # Unknown support never ranks ahead of a measured, finite value.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return -math.inf
    try:
        return float(value) if math.isfinite(float(value)) else -math.inf
    except OverflowError:
        return -math.inf


def _candidate_order(entry: dict, candidates: list[dict]) -> tuple:
    model = entry["report"].get("model", {})
    support = model.get("calibration", {}).get("conservative_probability_floor")
    value = model.get("conservative_unit_expected_value")
    return (entry["game_rank"] if entry["game_rank"] is not None else math.inf,
            -_ranking_number(support), -_ranking_number(value),
            _identity(candidates[entry["candidate_index"]]))


def _reconcile(payload: dict, ranked_game: dict | None, slate: dict,
               cutoff) -> list[str]:
    problems = []
    if timestamp(payload["as_of"], "candidate.as_of") != cutoff:
        problems.append("Candidate research cutoff does not match the shared Sunday cutoff")
    if payload["game"]["season"] != slate["season"]:
        problems.append("Candidate NFL season does not match the Sunday schedule")
    if ranked_game is None:
        problems.append("Candidate game is absent from the selected Sunday morning/early slate")
        return problems
    game = payload["game"]
    if timestamp(game["kickoff"], "candidate.game.kickoff") != timestamp(ranked_game["kickoff"], "schedule.kickoff"):
        problems.append("Candidate kickoff does not match the Sunday schedule")
    for side in ("home_team", "away_team"):
        if game[side].strip() != ranked_game[side]:
            problems.append(f"Candidate {side} does not match the Sunday schedule")
    if game.get("spread") != ranked_game.get("spread"):
        problems.append("Candidate spread does not match the Sunday schedule snapshot")
    if not ranked_game["eligible"]:
        problems.extend("Scheduled game is excluded: " + reason for reason in ranked_game["exclusions"])
    return problems


def _selection_freshness(payload: dict, report: dict, selected_at) -> tuple[object, str | None]:
    """Refresh liveness after the entire batch, rather than trust an earlier bool."""
    if not report.get("current_snapshot", False):
        return None, "Historical or noncurrent snapshot is research-only"
    try:
        deadline = timestamp(report.get("current_until"), "report.current_until")
        cutoff = timestamp(payload["as_of"], "candidate.as_of")
    except ValueError:
        return None, "Current snapshot expiry is missing or invalid; selection cannot be confirmed"
    if not cutoff <= selected_at < deadline:
        return deadline, "Snapshot expired before shortlist selection or is not current at selection time"
    return deadline, None


def run_sunday(manifest: dict) -> dict:
    """Rank a slate, evaluate every supplied candidate, and diversify a shortlist.

    Invalid input or duplicate imports fail the entire batch before evaluation.
    Valid but inconsistent snapshots remain in the returned decision reports as
    NO_BET, so the CLI can preserve their original input and rejection reasons.
    Historical qualifications are retained for research but are never selected
    as current actions. Schedule completeness remains a caller-supplied claim.
    """
    object_value(manifest, "manifest")
    cutoff = timestamp(manifest.get("as_of"), "manifest.as_of")
    synthetic = manifest.get("synthetic", False)
    if not isinstance(synthetic, bool):
        raise ValueError("manifest.synthetic must be a boolean")
    max_selections = _limit(manifest.get("max_selections", 3), "max_selections", 10)
    max_per_game = _limit(manifest.get("max_per_game", 1), "max_per_game", 3)
    schedule = object_value(manifest.get("schedule"), "manifest.schedule")

    # This must happen before candidate evaluation or selection.
    slate = filter_and_rank(schedule, as_of=manifest["as_of"])
    synthetic = synthetic or slate["synthetic"]
    candidates = manifest.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError("manifest.candidates must be an array")
    seen = set()
    for payload in candidates:
        validate(payload)
        if payload.get("mode", "pregame") != "pregame":
            raise ValueError("Sunday batch selection supports pregame candidates only")
        identity = _identity(payload)
        if identity in seen:
            raise ValueError("Duplicate Sunday candidate game/player/stat/line/sportsbook import")
        seen.add(identity)

    by_game = {game["id"]: game for game in slate["games"]}
    decisions = []
    for index, original in enumerate(candidates):
        payload = deepcopy(original)
        inherited_synthetic = synthetic or payload.get("synthetic", False)
        if inherited_synthetic:
            payload["synthetic"] = True
        report = deepcopy(evaluate(payload))
        report["synthetic"] = inherited_synthetic
        game = by_game.get(payload["game"]["id"])
        problems = _reconcile(payload, game, slate, cutoff)
        if inherited_synthetic:
            problems.append("Synthetic Sunday inputs cannot qualify a current selection")
        if problems:
            report["blockers"] = list(dict.fromkeys([*report.get("blockers", []), *problems]))
            report["decision"] = "NO_BET"
        report["sunday_context"] = {
            "as_of": manifest["as_of"], "target_date": slate["target_date"],
            "game_rank": game["rank"] if game else None,
            "schedule_complete_claim": slate["schedule_complete"],
            "schedule_source": deepcopy(slate["source"]),
            "synthetic_inherited": inherited_synthetic,
            "reconciliation_problems": problems,
        }
        decisions.append({"candidate_index": index,
                          "game_rank": game["rank"] if game else None,
                          "report": report})

    decisions.sort(key=lambda entry: _candidate_order(entry, candidates))
    # A long later simulation must not keep an earlier, now-expired quote alive.
    selected_at = datetime.now(timezone.utc)
    shortlist, deferred = [], []
    used_players = set()
    game_counts = Counter()
    for entry in decisions:
        index, report = entry["candidate_index"], entry["report"]
        payload = candidates[index]
        player_id, game_id = payload["player"]["id"].strip(), payload["game"]["id"]
        deadline, freshness_problem = _selection_freshness(payload, report, selected_at)
        report["sunday_context"].update(
            selection_checked_at=selected_at.isoformat(),
            current_at_selection=freshness_problem is None,
        )
        reason = None
        if report["decision"] != "QUALIFIED":
            reason = "Candidate did not qualify; see its decision report"
        elif freshness_problem:
            reason = freshness_problem
        elif report["synthetic"]:
            reason = "Synthetic inputs cannot create a current shortlist"
        elif not slate["schedule_complete"]:
            reason = "Schedule completeness is unconfirmed; no current shortlist can be produced"
        elif player_id in used_players:
            reason = "Player concentration limit: this player already has a shortlisted selection"
        elif game_counts[game_id] >= max_per_game:
            reason = "Game concentration limit reached"
        elif len(shortlist) >= max_selections:
            reason = "Shortlist selection limit reached"
        if reason:
            entry["selection_status"] = "deferred"
            entry["selection_reason"] = reason
            deferred.append({"candidate_index": index, "reason": reason})
            continue
        model = report.get("model", {})
        entry["selection_status"] = "shortlisted"
        entry["selection_reason"] = "Qualified current snapshot within research concentration limits"
        shortlist.append({
            "candidate_index": index, "game_id": game_id, "game_rank": entry["game_rank"],
            "player_id": player_id, "player": payload["player"]["name"],
            "market": deepcopy(payload["market"]), "selection_rank": len(shortlist) + 1,
            "current_until": deadline.isoformat(),
            "conservative_probability_floor": model.get("calibration", {}).get("conservative_probability_floor"),
            "conservative_unit_expected_value": model.get("conservative_unit_expected_value"),
        })
        used_players.add(player_id)
        game_counts[game_id] += 1

    candidate_game_ids = {payload["game"]["id"] for payload in candidates}
    eligible_ids = [game["id"] for game in slate["games"] if game["eligible"]]
    return {
        "as_of": manifest["as_of"], "selection_at": selected_at.isoformat(),
        "synthetic": synthetic or any(payload.get("synthetic", False) for payload in candidates),
        "ranked_slate": slate, "decisions": decisions, "shortlist": shortlist, "deferred": deferred,
        "coverage": {
            "eligible_game_count": len(eligible_ids),
            "games_with_candidates": [game_id for game_id in eligible_ids if game_id in candidate_game_ids],
            "games_without_candidates": [game_id for game_id in eligible_ids if game_id not in candidate_game_ids],
        },
        "overview": {
            "candidate_count": len(candidates), "qualified_research_count": sum(entry["report"]["decision"] == "QUALIFIED" for entry in decisions),
            "no_bet_count": sum(entry["report"]["decision"] == "NO_BET" for entry in decisions),
            "shortlisted_count": len(shortlist), "deferred_count": len(deferred),
        },
        "policy": {"max_selections": max_selections, "max_per_game": max_per_game,
                   "max_per_player": 1, "requires_current_snapshot": True,
                   "requires_complete_schedule_claim": True,
                   "ranking": "Game rank, conservative historical support floor, conservative unit EV, stable candidate identity"},
        "provenance": {
            "source_verification": "Supplied schedule and research citations are not independently fetched",
            "shortlist_scope": "Individual research selections; no joint or parlay probability is calculated",
            "wagers_placed": 0,
        },
    }
