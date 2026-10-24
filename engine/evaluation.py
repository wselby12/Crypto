"""Execute modeling and enforce evidence, exclusion, and value gates."""

from collections import Counter
from datetime import datetime, timezone
import statistics

from . import __version__
from .probability import analyze
from .rules import evaluate_evidence, exclusions
from .slate import EASTERN
from .validation import number, object_value, timestamp, validate

SCORE_WEIGHTS = {
    "workload_role": 0.25, "recent_usage": 0.15, "matchup": 0.15,
    "script_pace": 0.15, "injuries_line": 0.10,
    "historical_volatility": 0.10, "price_value": 0.10,
}


def _heuristic_score(payload):
    if "scores" not in payload:
        return {"value": None, "is_probability": False,
                "description": "Optional heuristic research rating was not supplied",
                "weights": SCORE_WEIGHTS}
    scores = object_value(payload["scores"], "scores")
    for name in SCORE_WEIGHTS:
        number(scores.get(name), f"scores.{name}", minimum=0, maximum=10)
    return {"value": round(sum(scores[name] * weight for name, weight in SCORE_WEIGHTS.items()), 3),
            "out_of": 10, "is_probability": False, "weights": SCORE_WEIGHTS,
            "description": "Uncalibrated research rating; never a win probability or gate override"}


def evaluate(payload):
    """Return a research report; no wager is ever placed by this function."""
    validate(payload)
    audit = evaluate_evidence(payload)
    model = analyze(payload)
    blockers = exclusions(payload)
    mode = payload.get("mode", "pregame")
    cutoff = timestamp(payload["as_of"], "as_of")
    if cutoff > datetime.now(timezone.utc):
        blockers.append("Future research snapshots cannot qualify a recommendation")
    observed = timestamp(payload["market"]["observed_at"], "market.observed_at")
    if (cutoff - observed).total_seconds() > (60 if mode == "live" else 900):
        blockers.append("Sportsbook quote is stale for this decision timestamp")
    local_kickoff = timestamp(payload["game"]["kickoff"], "game.kickoff").astimezone(EASTERN)
    expected_season = local_kickoff.year - (1 if local_kickoff.month <= 2 else 0)
    if payload["game"]["season"] != expected_season:
        blockers.append("Selected season does not match the game's NFL season")
    window = ("morning" if local_kickoff.hour < 13 else "early")
    if local_kickoff.weekday() != 6 or local_kickoff.hour >= 14:
        blockers.append("Game is outside the Sunday morning and 1 p.m. Eastern target windows")
        window = "outside_target"
    if payload.get("synthetic", False):
        blockers.append("Synthetic demonstration data cannot qualify a recommendation")
    if not model["calibration"]["qualified"]:
        blockers.append(f"Probability calibration is not qualified: {model['calibration']['reason']}")
    if not model["recommended_value"]:
        blockers.append("A probability edge did not survive conservative value and sensitivity checks")

    history = sorted(payload["history"], key=lambda row: timestamp(row["played_at"], "played_at"))
    summaries = {}
    for count in (4, 5, 10):
        rows = history[-count:]
        values = [row["production"] for row in rows]
        summaries[f"last_{count}"] = {
            "requested_games": count, "available_games": len(rows),
            "complete_window": len(rows) == count, "mean": statistics.mean(values),
            "population_stddev": statistics.pstdev(values),
            "mean_opportunities": statistics.mean(row["opportunities"] for row in rows),
        }
    computed = {
        7: "Calculated last-4/5/10 means, sample availability, and population variance",
        8: "Counted exact-line historical wins, losses, and pushes",
        34: "Sampled the three explicitly supplied game-script scenarios",
        36: "Built an empirical opportunity-and-efficiency outcome distribution",
        37: "Decomposed historical production into paired opportunities and efficiency",
        38: "Sampled historical workload and efficiency variation with script uncertainty",
        39: f"Executed {model['simulation']['draws']} seeded Monte Carlo draws",
        40: "Counted simulated exact-line wins, losses, and pushes",
        41: "Executed a separate lower-opportunity/lower-efficiency sensitivity scenario",
        48: "Calculated the sportsbook break-even probability from American odds",
        49: "Calculated unit expected value with push refunds",
        50: "Required calibration qualification and a conservative probability edge",
    }
    if mode == "live":
        live = payload["live"]
        if (cutoff - timestamp(live["observed_at"], "live.observed_at")).total_seconds() > 60:
            blockers.append("Live game snapshot is stale (older than 60 seconds)")
        if not live["player_active"]:
            blockers.append("Player is not confirmed active in the live game")
        computed.update({
            54: "Calculated remaining exact-line production from the supplied current total",
            55: "Simulated only remaining opportunities, adding already accrued production",
            58: "Calculated live expected value using the current quoted price",
        })
    for row in audit:
        if row["id"] in computed and row["status"] != "contradicted":
            row.update(status="computed", reason=computed[row["id"]])
            if row["id"] == 50 and (not model["calibration"]["qualified"] or not model["recommended_value"]):
                row.update(status="contradicted", reason="No reliable, calibrated, stress-resistant probability edge established")
        if row["status"] in {"missing", "contradicted"}:
            blockers.append(f"Rule {row['id']}: {row['reason']}")
    blockers = list(dict.fromkeys(blockers))
    executed_at = datetime.now(timezone.utc)
    age_seconds = (executed_at - cutoff).total_seconds()
    quote_max_age = 60 if mode == "live" else 900
    is_current = (0 <= age_seconds <= quote_max_age
                  and (mode == "live" or timestamp(payload["game"]["kickoff"], "kickoff") > executed_at))
    return {
        "engine_version": __version__, "decision": "NO_BET" if blockers else "QUALIFIED",
        "mode": mode, "as_of": payload["as_of"],
        "executed_at": executed_at.isoformat(),
        "snapshot_scope": "current_snapshot" if is_current else "historical_or_noncurrent_snapshot",
        "current_snapshot": is_current,
        "synthetic": payload.get("synthetic", False),
        "game_id": payload["game"]["id"], "player": payload["player"]["name"],
        "market": dict(payload["market"]), "target_window": window,
        "blockers": blockers, "audit": audit,
        "audit_counts": dict(Counter(row["status"] for row in audit)),
        "historical_windows": summaries, "model": model,
        "heuristic_score": _heuristic_score(payload),
        "source_verification": "Supplied citations checked for provenance and freshness; source contents not independently fetched",
        "limitations": [
            "A bootstrap simulation is conditional on supplied history and scenario assumptions",
            "Monte Carlo intervals measure simulation sampling error, not total prediction uncertainty",
            "Calibration checks use supplied holdout records and do not independently authenticate them",
            "Single props only; no independent-leg multiplication or joint parlay probability",
            "No automatic sportsbook submission or live data feed is connected",
            "Lifecycle steps 60–68 are tracked separately; no claim that all 68 checks passed",
        ],
    }
