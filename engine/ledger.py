"""Append-only decision evidence and explicit, source-attributed result accounting.

Recording a decision never records a wager. A positive stake is included in
financial results only when the caller subsequently settles a qualified decision
with that stake. Supplied result URLs are retained; they are not fetched or
independently verified by this module.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4


FAILURE_CAUSES = frozenset({
    "workload", "injury", "coaching", "matchup", "script",
    "statistical_variance", "bad_information", "unclassified",
})


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("Ledger records must contain finite JSON data") from exc


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a timezone-aware ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be a timezone-aware ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _number(value: Any, field: str, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite number")
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field} must be a finite number")
    if minimum is not None and result < minimum:
        raise ValueError(f"{field} must be at least {minimum:g}")
    return result


def _odds(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or abs(value) < 100:
        raise ValueError(f"{field} must be integer American odds of -100 or lower, or +100 or higher")
    if not -(2 ** 63) <= value <= 2 ** 63 - 1:
        raise ValueError(f"{field} must fit a signed 64-bit integer")
    return value


def _decimal(odds: int) -> float:
    return 1.0 + (odds / 100.0 if odds > 0 else 100.0 / -odds)


def _https_url(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("source_url must be an HTTPS URL")
    try:
        parsed = urlsplit(value)
        valid = parsed.scheme == "https" and bool(parsed.hostname) and not parsed.username and not parsed.password
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("source_url must be an HTTPS URL without embedded credentials")
    return value


def _synthetic(payload: dict, report: dict) -> bool:
    metadata = payload.get("metadata", {})
    return bool(payload.get("synthetic") or report.get("synthetic") or
                (isinstance(metadata, dict) and metadata.get("synthetic")))


def _nonempty(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value


class Ledger:
    """SQLite-backed immutable decision snapshots with one settlement per record."""

    def __init__(self, path: str):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
            self.path = str(Path(self.path).expanduser())
        self._connection = sqlite3.connect(self.path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.executescript("""
            CREATE TABLE IF NOT EXISTS decisions (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                as_of TEXT NOT NULL,
                kickoff TEXT NOT NULL,
                game_id TEXT NOT NULL,
                player_id TEXT NOT NULL,
                market_stat TEXT NOT NULL,
                sportsbook TEXT NOT NULL,
                line REAL NOT NULL,
                american_odds INTEGER NOT NULL,
                decision TEXT NOT NULL,
                synthetic INTEGER NOT NULL,
                input_json TEXT NOT NULL,
                report_json TEXT NOT NULL,
                input_sha256 TEXT NOT NULL,
                report_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS settlements (
                decision_id TEXT PRIMARY KEY REFERENCES decisions(id),
                settlement_json TEXT NOT NULL
            );
        """)
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def record(self, payload: dict, report: dict) -> str:
        """Save the original evidence and report, including rejected decisions."""
        if not isinstance(payload, dict) or not isinstance(report, dict):
            raise ValueError("payload and report must be JSON objects")
        try:
            game, player, market = payload["game"], payload["player"], payload["market"]
            as_of = _timestamp(payload["as_of"], "as_of")
            kickoff = _timestamp(game["kickoff"], "game.kickoff")
            game_id = _nonempty(game["id"], "game.id")
            player_id = _nonempty(player["id"], "player.id")
            stat = _nonempty(market["stat"], "market.stat")
            sportsbook = _nonempty(market["sportsbook"], "market.sportsbook")
            line = _number(market["line"], "market.line", 0.0)
            odds = _odds(market["american_odds"], "market.american_odds")
            decision = _nonempty(report["decision"], "report.decision")
            if market["side"] != "over":
                raise ValueError("The engine ledger currently supports over markets only")
        except (KeyError, TypeError) as exc:
            raise ValueError("Missing or invalid game, player, market, as_of, or report decision") from exc
        input_json, report_json = _canonical(payload), _canonical(report)
        decision_id = str(uuid4())
        with self._connection:
            self._connection.execute(
                "INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (decision_id, datetime.now(timezone.utc).isoformat(), as_of.isoformat(), kickoff.isoformat(),
                 game_id, player_id, stat, sportsbook, line, odds, decision, int(_synthetic(payload, report)),
                 input_json, report_json, _digest(input_json), _digest(report_json)),
            )
        return decision_id

    def get(self, decision_id: str) -> dict:
        """Read a record and verify stored snapshot hashes before using it."""
        row = self._connection.execute("SELECT * FROM decisions WHERE id = ?", (decision_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown decision: {decision_id}")
        if _digest(row["input_json"]) != row["input_sha256"] or _digest(row["report_json"]) != row["report_sha256"]:
            raise ValueError(f"Decision snapshot integrity check failed: {decision_id}")
        settlement_row = self._connection.execute(
            "SELECT settlement_json FROM settlements WHERE decision_id = ?", (decision_id,)
        ).fetchone()
        return {
            "decision_id": row["id"], "created_at": row["created_at"], "as_of": row["as_of"],
            "game_id": row["game_id"], "player_id": row["player_id"], "decision": row["decision"],
            "input_sha256": row["input_sha256"], "report_sha256": row["report_sha256"],
            "payload": json.loads(row["input_json"]), "report": json.loads(row["report_json"]),
            "settlement": json.loads(settlement_row["settlement_json"]) if settlement_row else None,
        }

    def settle(
        self, decision_id: str, official_result: float, source_url: str, settled_at: str,
        stake_units: float = 1.0, closing_line: float | None = None,
        closing_american_odds: int | None = None,
        failure_cause: str | None = None,
    ) -> dict:
        """Attach a result once. A supplied stake reports accounting, never places a bet.

        Rejected and synthetic decisions can be settled for research with zero
        stake only. A duplicate settlement is rejected rather than overwritten.
        """
        record = self.get(decision_id)
        if record["settlement"] is not None:
            raise ValueError("Decision is already settled; original accounting is immutable")
        payload, report = record["payload"], record["report"]
        result = _number(official_result, "official_result")
        stat = payload["market"]["stat"]
        is_yards = stat == "yards" or stat.endswith("_yards")
        is_count = stat in {
            "receptions", "targets", "attempts", "rushing_attempts", "passing_attempts",
            "passing_completions", "completions", "interceptions", "touchdowns",
        }
        if result < 0 and not is_yards:
            raise ValueError("official_result may be negative only for yardage markets")
        if (is_yards or is_count) and not result.is_integer():
            raise ValueError("official_result must be an integer for NFL yardage and count markets")
        stake = _number(stake_units, "stake_units", 0.0)
        source = _https_url(source_url)
        timestamp = _timestamp(settled_at, "settled_at")
        if timestamp < _timestamp(payload["game"]["kickoff"], "game.kickoff"):
            raise ValueError("settled_at cannot precede kickoff")
        if timestamp < _timestamp(payload["as_of"], "as_of"):
            raise ValueError("settled_at cannot precede the original decision")
        if timestamp > datetime.now(timezone.utc):
            raise ValueError("settled_at cannot be in the future")
        if stake > 0 and (report["decision"] != "QUALIFIED" or _synthetic(payload, report)):
            raise ValueError("Only nonsynthetic QUALIFIED decisions may report positive stakes; use zero for research")
        market = payload["market"]
        line = _number(market["line"], "market.line", 0.0)
        odds = _odds(market["american_odds"], "market.american_odds")
        outcome = "win" if result > line else "loss" if result < line else "push"
        if failure_cause is not None and failure_cause not in FAILURE_CAUSES:
            raise ValueError("failure_cause must be one of: " + ", ".join(sorted(FAILURE_CAUSES)))
        if failure_cause is not None and outcome != "loss":
            raise ValueError("failure_cause applies only to a losing result")
        net = stake * (_decimal(odds) - 1.0) if outcome == "win" else -stake if outcome == "loss" else 0.0
        closing = None if closing_line is None else _number(closing_line, "closing_line", 0.0)
        close_odds = None if closing_american_odds is None else _odds(closing_american_odds, "closing_american_odds")
        if close_odds is not None and closing is None:
            raise ValueError("closing_line is required with closing_american_odds so prices are compared at the same threshold")
        settlement = {
            "decision_id": decision_id, "official_result": result, "source_url": source,
            "source_verification": "user_supplied_not_independently_verified",
            "settled_at": timestamp.isoformat(), "outcome": outcome, "stake_units": stake,
            "failure_cause": (failure_cause or "unclassified") if outcome == "loss" else None,
            "net_units": net, "roi": net / stake if stake > 0 else None,
            "closing_line": closing, "closing_american_odds": close_odds,
            "closing_line_value_points": closing - line if closing is not None else None,
            "closing_implied_probability_minus_initial": (
                1.0 / _decimal(close_odds) - 1.0 / _decimal(odds)
                if close_odds is not None and closing == line else None
            ),
        }
        try:
            with self._connection:
                self._connection.execute(
                    "INSERT INTO settlements (decision_id, settlement_json) VALUES (?, ?)",
                    (decision_id, _canonical(settlement)),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError("Decision is already settled; original accounting is immutable") from exc
        return settlement

    def performance(self) -> dict:
        """Report staked settlements separately from rejected or research decisions."""
        identifiers = [row["id"] for row in self._connection.execute("SELECT id FROM decisions ORDER BY created_at, id")]
        records = [self.get(identifier) for identifier in identifiers]
        staked = [record for record in records if record["settlement"] and record["settlement"]["stake_units"] > 0]
        rejected = [record for record in records if record["decision"] == "NO_BET"]
        reasons: Counter[str] = Counter()
        for record in rejected:
            report = record["report"]
            supplied = report.get("blocking_reasons", report.get("reasons", report.get("blockers", report.get("rejection_reasons", []))))
            if isinstance(supplied, str):
                supplied = [supplied]
            if not isinstance(supplied, list):
                supplied = []
            labels = set()
            for reason in supplied:
                if isinstance(reason, str):
                    labels.add(reason)
                elif isinstance(reason, dict):
                    labels.add(str(reason.get("reason", reason.get("message", _canonical(reason)))))
            reasons.update(labels)
        categories = sorted({record["payload"]["market"]["stat"] for record in staked})
        totals = self._summarize(staked)
        return {
            **totals, "total_decisions": len(records), "rejected_decisions": len(rejected),
            "unsettled_decisions": sum(record["settlement"] is None for record in records),
            "research_settlements": sum(bool(record["settlement"]) and record["settlement"]["stake_units"] == 0 for record in records),
            "rejection_reasons": dict(sorted(reasons.items())),
            "failure_causes": dict(sorted(Counter(
                record["settlement"].get("failure_cause") or "unclassified"
                for record in staked if record["settlement"]["outcome"] == "loss"
            ).items())),
            "by_category": {
                category: self._summarize([record for record in staked if record["payload"]["market"]["stat"] == category])
                for category in categories
            },
            "accounting_basis": "explicit_positive_stakes_at_settlement; recording_never_places_a_wager",
        }

    @staticmethod
    def _summarize(records: list[dict]) -> dict:
        settlements = [record["settlement"] for record in records]
        counts = Counter(result["outcome"] for result in settlements)
        total_stake = sum(result["stake_units"] for result in settlements)
        net = sum(result["net_units"] for result in settlements)
        decided = counts["win"] + counts["loss"]
        return {
            "settled_bets": len(settlements), "wins": counts["win"], "losses": counts["loss"], "pushes": counts["push"],
            "total_stake_units": total_stake, "net_units": net,
            "roi": net / total_stake if total_stake else None,
            "hit_rate": counts["win"] / decided if decided else None,
        }
