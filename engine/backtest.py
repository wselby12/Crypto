"""Execute chronological held-out research cases using frozen input snapshots.

The fixed bootstrap algorithm is run for every case; no retrospective strategy
selection, parameter optimization, or fictitious strategy count is performed.
Chronology is checked from supplied timestamps, not independent source archives.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from statistics import fmean
from urllib.parse import urlsplit

from .probability import MODEL_ID, analyze
from .validation import SUPPORTED_STATS, number, object_value, text, timestamp, validate


def _https_source(value: object, name: str) -> str:
    url = text(value, name)
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError(f"{name} must be an HTTPS URL without credentials")
    return url


def run_backtest(dataset: dict) -> dict:
    """Execute each case as of its pre-result snapshot and return calibration.

    The caller must supply historical snapshots, official results with cited
    provenance, and the algorithm's training/cutoff date. The result preserves
    those timestamps and marks synthetic calibration so it cannot qualify a
    real-data recommendation. Cases are processed chronologically.
    """
    object_value(dataset, "dataset")
    cutoff = timestamp(dataset.get("as_of"), "dataset.as_of")
    training_end = timestamp(dataset.get("training_ended_at"), "dataset.training_ended_at")
    if training_end > cutoff:
        raise ValueError("training_ended_at cannot be after dataset.as_of")
    if dataset.get("model_id") != MODEL_ID:
        raise ValueError(f"dataset.model_id must be {MODEL_ID}")
    market_stat = dataset.get("market_stat")
    if not isinstance(market_stat, str) or market_stat not in SUPPORTED_STATS:
        raise ValueError("dataset.market_stat must identify a supported NFL prop category")
    mode = dataset.get("mode", "pregame")
    if not isinstance(mode, str) or mode not in {"pregame", "live"}:
        raise ValueError("dataset.mode must be pregame or live")
    cases = dataset.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("dataset.cases must be a nonempty array")

    seen: set[tuple] = set()
    prepared = []
    for index, case in enumerate(cases):
        prefix = f"dataset.cases[{index}]"
        object_value(case, prefix)
        # Do not mutate a caller's retained historical snapshot during a run.
        snapshot = deepcopy(object_value(case.get("snapshot"), f"{prefix}.snapshot"))
        try:
            validate(snapshot)
        except TypeError as exc:
            raise ValueError(f"{prefix}.snapshot contains an invalid field type") from exc
        predicted_at = timestamp(snapshot["as_of"], f"{prefix}.snapshot.as_of")
        settled_at = timestamp(case.get("settled_at"), f"{prefix}.settled_at")
        kickoff = timestamp(snapshot["game"]["kickoff"], f"{prefix}.snapshot.game.kickoff")
        if not training_end <= predicted_at < settled_at <= cutoff:
            raise ValueError("backtest has future leakage or invalid training/prediction/settlement chronology")
        if settled_at <= kickoff:
            raise ValueError("official settlement must be after game kickoff")
        if snapshot.get("mode", "pregame") != mode:
            raise ValueError("all cases must match dataset.mode")
        if snapshot["market"]["stat"] != market_stat:
            raise ValueError("all cases must match dataset.market_stat")
        if snapshot["projection"].get("model_id") != MODEL_ID:
            raise ValueError("all cases must match dataset.model_id")
        result = number(case.get("official_result"), f"{prefix}.official_result")
        if not market_stat.endswith("_yards") and result < 0:
            raise ValueError("official count results cannot be negative")
        if not float(result).is_integer():
            raise ValueError("official NFL yard and count results must be integers")
        source = _https_source(case.get("source_url"), f"{prefix}.source_url")
        identity = (snapshot["game"]["id"], snapshot["player"]["id"], market_stat,
                    snapshot["market"]["line"])
        if identity in seen:
            raise ValueError("duplicate backtest game/player/stat/line case")
        seen.add(identity)
        prepared.append((predicted_at, identity, snapshot, result, settled_at, source))

    all_synthetic = all(case[2].get("synthetic", False) for case in prepared)
    if cutoff > datetime.now(timezone.utc) and not all_synthetic:
        raise ValueError("dataset.as_of cannot be in the future for nonsynthetic backtests")

    prepared.sort(key=lambda case: (case[0], case[1]))
    predictions = []
    excluded_predictions = []
    total_draws = 0
    synthetic_cases = 0
    wins = losses = pushes = 0
    for predicted_at, identity, snapshot, result, settled_at, source in prepared:
        projection = analyze(snapshot)
        if projection["simulation"].get("executed") is not True:
            raise ValueError("backtest projection did not execute a simulation")
        total_draws += projection["simulation"]["draws"]
        synthetic_cases += bool(snapshot.get("synthetic", False))
        line = snapshot["market"]["line"]
        outcome = 1 if result > line else 0 if result < line else None
        wins += outcome == 1
        losses += outcome == 0
        pushes += outcome is None
        prediction_id = hashlib.sha256(json.dumps(
            [*identity, predicted_at.isoformat()], separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest()
        record = {
            "prediction_id": prediction_id,
            "game_id": snapshot["game"]["id"],
            "player_id": snapshot["player"]["id"], "line": line,
            "side": snapshot["market"]["side"],
            "synthetic": bool(snapshot.get("synthetic", False)),
            "predicted_at": predicted_at.isoformat(), "settled_at": settled_at.isoformat(),
            "outcome": outcome, "official_result": result, "source_url": source,
            "simulation_draws": projection["simulation"]["draws"],
            "snapshot_sha256": hashlib.sha256(json.dumps(
                snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ).encode("utf-8")).hexdigest(),
        }
        nonpush_probability = projection["probability_win"] + projection["probability_loss"]
        if nonpush_probability == 0:
            excluded_predictions.append({
                **record,
                "reason": "Every simulation draw pushed; conditional non-push probability is undefined.",
            })
            continue
        predictions.append({
            **record, "probability": projection["probability_win"] / nonpush_probability,
        })

    pairs = [(row["probability"], row["outcome"]) for row in predictions if row["outcome"] is not None]
    calibration = {
        "model_id": MODEL_ID, "market_stat": market_stat, "mode": mode,
        "training_ended_at": training_end.isoformat(), "predictions": predictions,
        "synthetic": synthetic_cases > 0,
    }
    return {
        "as_of": cutoff.isoformat(),
        "synthetic": synthetic_cases > 0,
        "calibration": calibration,
        "excluded_predictions": excluded_predictions,
        "summary": {
            "cases_executed": len(prepared), "total_simulation_draws": total_draws,
            "wins": wins, "losses": losses, "pushes": pushes,
            "nonpush_observations": len(pairs), "synthetic_cases": synthetic_cases,
            "brier_score": fmean((probability - outcome) ** 2 for probability, outcome in pairs) if pairs else None,
            "limitations": [
                "Held-out chronology is checked from supplied timestamps; source contents and archived availability are not independently verified.",
                "The fixed model is executed without parameter tuning on these cases; retrospective snapshot selection can still introduce bias.",
                "Simulation draws are numerical samples, not independent historical validation observations.",
                "Several players or lines from the same game may be correlated; case counts do not establish independent sample size.",
                "Results cover supplied cases only, not 500 strategies or an independently validated profitable system.",
                "Synthetic cases cannot qualify real-data calibration.",
            ],
        },
    }
