"""Conservative scenario and workload screens for a supplied research snapshot.

These screens are explicit operating policies, not fitted predictors. They use
simulation counts and supplied historical metadata; source contents are not
independently authenticated here.
"""

from __future__ import annotations

import math
from statistics import fmean

from .validation import number, object_value, timestamp


SCRIPT_NAMES = ("early_lead", "competitive", "early_deficit")
MIN_SCRIPT_DRAWS = 30
MIN_WORKLOAD_RATIO = 0.75


def _wilson(successes: int, count: int) -> dict[str, float]:
    """Two-sided 95% Wilson interval for simulation sampling frequency only."""
    z = 1.959963984540054
    proportion = successes / count
    denominator = 1 + z * z / count
    center = (proportion + z * z / (2 * count)) / denominator
    radius = z * math.sqrt(proportion * (1 - proportion) / count + z * z / (4 * count * count)) / denominator
    return {"low": max(0.0, center - radius), "high": min(1.0, center + radius)}


def _count(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _workload(payload: dict, blockers: list[str], warnings: list[str]) -> dict:
    history = payload.get("history", [])
    if not isinstance(history, list):
        raise ValueError("history must be an array")
    ordered = []
    for index, row in enumerate(history):
        object_value(row, f"history[{index}]")
        played_at = timestamp(row.get("played_at"), f"history[{index}].played_at")
        opportunities = number(row.get("opportunities"), f"history[{index}].opportunities", minimum=0)
        ordered.append((played_at, float(opportunities), row))
    ordered.sort(key=lambda entry: entry[0])

    player = object_value(payload.get("player"), "player")
    context = player.get("context", {})
    if not isinstance(context, dict):
        raise ValueError("player.context must be an object when supplied")
    expected = {"team": player.get("team"),
                "quarterback_id": context.get("quarterback_id"),
                "playcaller_id": context.get("playcaller_id")}
    metadata = {}
    for field, current in expected.items():
        if current is not None and (not isinstance(current, str) or not current.strip()):
            raise ValueError(f"current {field} must be a nonempty string when supplied")
        supplied = 0
        mismatched_games = []
        for _, _, row in ordered:
            recorded = row.get(field)
            if recorded is None:
                continue
            if not isinstance(recorded, str) or not recorded.strip():
                raise ValueError(f"history.{field} must be a nonempty string when supplied")
            supplied += 1
            if current is not None and recorded != current:
                mismatched_games.append(row.get("game_id", "unknown_game"))
        metadata[field] = {"current": current, "historical_games_with_metadata": supplied,
                           "mismatched_games": mismatched_games}
        if mismatched_games:
            blockers.append(f"Historical {field} differs from the current offensive context in {len(mismatched_games)} game(s); mixed regimes cannot establish stable workload")
        if current is None:
            warnings.append(f"Current {field} metadata is unavailable; historical regime continuity was not checked")
        elif supplied < len(ordered):
            warnings.append(f"Historical {field} metadata is missing in {len(ordered) - supplied} game(s); regime continuity is incompletely documented")

    trend = {
        "available": len(ordered) >= 6,
        "historical_games": len(ordered),
        "recent_games": 3,
        "comparison_games": 3,
        "recent_mean_opportunities": None,
        "previous_mean_opportunities": None,
        "recent_to_previous_ratio": None,
        "minimum_accepted_ratio": MIN_WORKLOAD_RATIO,
        "decline_flag": False,
        "context": metadata,
        "interpretation": "A 25% workload decline is a conservative policy flag, not a learned or causal prediction.",
    }
    if len(ordered) < 6:
        warnings.append("Workload trend unavailable: six historical games are needed to compare recent three with previous three")
        return trend
    recent = fmean(entry[1] for entry in ordered[-3:])
    previous = fmean(entry[1] for entry in ordered[-6:-3])
    # A zero-to-zero history has no finite ratio; do not invent one.
    ratio = recent / previous if previous > 0 else None
    decline = previous > 0 and recent < MIN_WORKLOAD_RATIO * previous
    trend.update(recent_mean_opportunities=recent, previous_mean_opportunities=previous,
                 recent_to_previous_ratio=ratio, decline_flag=decline)
    if decline:
        blockers.append("Recent-three-game mean opportunities fell by more than 25% versus the previous three; the historical stable-workload assumption is unsupported")
    return trend


def assess(payload: dict, model: dict) -> dict:
    """Require every supplied game script to retain conservative positive EV.

    Wilson limits describe numerical sampling error in each simulated script,
    not uncertainty about future football outcomes. Workload and context flags
    compare supplied history without filling missing metadata or filtering away
    contradictory observations.
    """
    object_value(payload, "payload")
    object_value(model, "model")
    scripts = object_value(model.get("scripts"), "model.scripts")
    decimal_odds = float(number(model.get("decimal_odds"), "model.decimal_odds", minimum=1))
    if decimal_odds <= 1:
        raise ValueError("model.decimal_odds must exceed one")
    simulation = object_value(model.get("simulation"), "model.simulation")
    blockers: list[str] = []
    warnings: list[str] = []
    if simulation.get("executed") is not True:
        blockers.append("Robustness requires an actually executed simulation")
    total_draws = _count(simulation.get("draws"), "model.simulation.draws")
    details = []
    counted_draws = 0
    for name in SCRIPT_NAMES:
        data = scripts.get(name)
        if not isinstance(data, dict):
            blockers.append(f"Required game script {name} is missing")
            details.append({"name": name, "draws": 0, "qualified": False,
                            "unit_expected_value": None, "conservative_unit_expected_value": None,
                            "reason": "Required script was not supplied"})
            continue
        draws = _count(data.get("draws"), f"model.scripts.{name}.draws")
        counted_draws += draws
        if draws == 0:
            blockers.append(f"Game script {name} has fewer than {MIN_SCRIPT_DRAWS} actual draws")
            details.append({"name": name, "draws": 0, "qualified": False,
                            "unit_expected_value": None, "conservative_unit_expected_value": None,
                            "reason": "No outcomes were simulated for this script"})
            continue
        wins = _count(data.get("wins"), f"model.scripts.{name}.wins")
        losses = _count(data.get("losses"), f"model.scripts.{name}.losses")
        pushes = _count(data.get("pushes"), f"model.scripts.{name}.pushes")
        if wins + losses + pushes != draws:
            raise ValueError(f"Game script {name} settlement counts must sum to its draw count")
        win_interval = _wilson(wins, draws)
        loss_interval = _wilson(losses, draws)
        expected_value = (wins / draws) * (decimal_odds - 1) - losses / draws
        conservative = win_interval["low"] * (decimal_odds - 1) - loss_interval["high"]
        if not math.isfinite(expected_value) or not math.isfinite(conservative):
            raise ValueError("Script expected value must be finite")
        enough_draws = draws >= MIN_SCRIPT_DRAWS
        positive = conservative > 0
        if not enough_draws:
            blockers.append(f"Game script {name} has fewer than {MIN_SCRIPT_DRAWS} actual draws")
        if not positive:
            blockers.append(f"Game script {name} fails conservative positive expected value")
        details.append({
            "name": name, "draws": draws, "wins": wins, "losses": losses, "pushes": pushes,
            "unit_expected_value": expected_value,
            "conservative_unit_expected_value": conservative,
            "win_probability_interval_95": win_interval,
            "loss_probability_interval_95": loss_interval,
            "qualified": enough_draws and positive,
            "reason": "Conservative value survives this script" if enough_draws and positive
                      else "Insufficient script draws or nonpositive conservative value",
        })
    if counted_draws != total_draws:
        blockers.append("Per-script draw counts do not match the executed simulation draw count")

    trend = _workload(payload, blockers, warnings)
    missing = [row for row in details if row["conservative_unit_expected_value"] is None]
    worst = missing[0] if missing else min(details, key=lambda row: row["conservative_unit_expected_value"])
    return {
        "qualified": not blockers,
        "blockers": blockers,
        "warnings": warnings,
        "game_scripts": details,
        "worst_script": worst["name"],
        "worst_script_unit_ev": worst["conservative_unit_expected_value"],
        "worst_script_conservative_unit_ev": worst["conservative_unit_expected_value"],
        "minimum_script_draws": MIN_SCRIPT_DRAWS,
        "workload_trend": trend,
        "provenance_scope": "Executed simulation counts and supplied historical metadata; source contents and context claims are not independently authenticated.",
        "uncertainty_scope": "Per-script Wilson limits measure Monte Carlo sampling error only, not total predictive uncertainty.",
        "policy_scope": "All three modeled scripts must retain conservative positive EV; the workload decline threshold is an explicit provisional operating policy.",
    }
