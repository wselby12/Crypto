"""Command-line entry point for auditable NFL research snapshots."""

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys

from . import __version__
from .evaluation import evaluate
from .ledger import Ledger
from .rules import RULES
from .slate import filter_and_rank
from .validation import load_json


def _same_file(first, second):
    left, right = Path(first), Path(second)
    return left.resolve() == right.resolve() or (
        left.exists() and right.exists() and os.path.samefile(left, right))


def _output_options(parser):
    parser.add_argument("--json", action="store_true", help="print complete JSON")
    parser.add_argument("--output", type=Path, help="also save complete JSON")


def _db_option(parser):
    parser.add_argument("--db", default=".engine/engine.sqlite3", help="SQLite audit ledger")


def parser():
    root = argparse.ArgumentParser(prog="engine", description="Evidence-gated NFL player-prop research engine")
    root.add_argument("--version", action="version", version=__version__)
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("evaluate", "demo"):
        command = commands.add_parser(name, help="evaluate an input snapshot" if name == "evaluate" else "run a synthetic NO BET demonstration")
        if name == "evaluate":
            command.add_argument("input", type=Path)
        _output_options(command)
        _db_option(command)
        command.add_argument("--require-qualified", action="store_true", help="exit 3 on NO_BET")
    command = commands.add_parser("slate", help="rank Sunday morning and 1 p.m. Eastern games before props")
    command.add_argument("input", type=Path)
    command.add_argument("--as-of", help="timezone-aware cutoff; defaults to current UTC")
    _output_options(command)
    command = commands.add_parser("backtest", help="execute chronological holdout snapshots")
    command.add_argument("input", type=Path)
    _output_options(command)
    command = commands.add_parser("sunday", help="rank games, evaluate candidates, and save a diversified Sunday research run")
    command.add_argument("input", type=Path, help="manifest containing schedule and candidates")
    _db_option(command)
    _output_options(command)
    command.add_argument("--require-qualified", action="store_true", help="exit 3 if no current selection qualifies")
    command = commands.add_parser("show-run", help="verify and retrieve a complete Sunday run")
    command.add_argument("run_id")
    _db_option(command)
    _output_options(command)
    command = commands.add_parser("policy", help="show versioned operating gates and their fingerprint")
    _output_options(command)
    command = commands.add_parser("rules", help="show all 68 audit steps")
    _output_options(command)
    command = commands.add_parser("show", help="verify hashes and retrieve an original decision")
    command.add_argument("decision_id")
    _db_option(command)
    _output_options(command)
    command = commands.add_parser("performance", help="report financial results separately from rejected research")
    _db_option(command)
    _output_options(command)
    command = commands.add_parser("settle", help="record an official result and a reported stake")
    command.add_argument("decision_id")
    command.add_argument("--result", type=float, required=True)
    command.add_argument("--source", required=True, help="HTTPS official-result source")
    command.add_argument("--at", required=True, help="timezone-aware settlement timestamp")
    command.add_argument("--stake", type=float, default=0.0, help="reported units; default zero for research, never places a bet")
    command.add_argument("--closing-line", type=float)
    command.add_argument("--closing-odds", type=int)
    command.add_argument("--failure-cause")
    _db_option(command)
    _output_options(command)
    return root


def _print_result(result, args):
    encoded = json.dumps(result, indent=2, sort_keys=True, allow_nan=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    if args.json:
        print(encoded)
    elif args.command in {"evaluate", "demo"}:
        report = result["report"]
        model = report["model"]
        label = "SYNTHETIC DEMO | " if report["synthetic"] else ""
        print(f"{label}{report['decision']} | {report['player']} | {report['market']['stat']} over {report['market']['line']}")
        print(f"Research cutoff: {report['as_of']} | target: {report['target_window']}")
        print("Scope: " + report["snapshot_scope"])
        print("Current snapshot expires: " + report["current_until"])
        print(f"Executed {model['simulation']['draws']:,} draws with seed {model['simulation']['seed']}; model remains unvalidated")
        print(f"Model estimate under supplied assumptions: win {model['probability_win']:.1%}, push {model['probability_push']:.1%}; EV {model['unit_expected_value']:+.3f} units per unit")
        print("Calibration: " + model["calibration"]["reason"])
        robustness = report["robustness"]
        print(f"Script/workload checks: {'qualified' if robustness['qualified'] else 'blocked'} | worst script: {robustness['worst_script']}")
        print("Audit: " + ", ".join(f"{count} {name}" for name, count in report["audit_counts"].items()))
        for reason in report["blockers"]:
            print("- " + reason)
        print("Decision saved: " + result["decision_id"])
        print("Supplied citations were not independently fetched.")
    elif args.command == "sunday":
        run = result["result"]
        print(f"Sunday {run['ranked_slate']['target_date']} | morning + 1 p.m. Eastern")
        if run["synthetic"]:
            print("SYNTHETIC RESEARCH RUN")
        counts = run["overview"]
        print(f"{counts['candidate_count']} candidates | {counts['no_bet_count']} NO BET | {counts['shortlisted_count']} current selections")
        for row in run["shortlist"]:
            market = row["market"]
            print(f"{row['selection_rank']}. {row['player']} | {market['sportsbook']} | {market['stat']} over {market['line']} ({market['american_odds']:+d})")
        if not run["shortlist"]:
            print("NO BET: no current selection met every research and slate requirement.")
        for row in run["decisions"]:
            report = row["report"]
            reason = report["blockers"][0] if report.get("blockers") else row.get("selection_reason", "")
            print(f"- {report['player']}: {report['decision']} | {reason}")
        print("Games needing candidate research: " + str(len(run["coverage"]["games_without_candidates"])))
        print("Complete run saved: " + result["run_id"])
    elif args.command == "slate":
        print(f"Sunday {result['target_date']} | America/New_York | morning + 1 p.m. Eastern")
        if result["synthetic"]:
            print("SYNTHETIC SCHEDULE: demonstration only")
        for game in result["games"]:
            print(f"{game['rank']}. {game['away_team']} at {game['home_team']} | {game['kickoff_et']} | {game['window']} | suitability {game['suitability_score']:.1f} | {'eligible for research' if game['eligible'] else 'excluded'}")
        for window, value in result["windows"].items():
            print(f"{window}: {value['count']} games ({value['status']})")
        print("Suitability is a research heuristic; supplied source contents are not independently fetched.")
    elif args.command == "rules":
        for rule in result["rules"]:
            print(f"{rule['id']:02d}. [{rule['stage']}] {rule['title']}")
    else:
        print(encoded)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        source = getattr(args, "input", None)
        if args.command == "demo":
            source = Path(__file__).resolve().parent.parent / "examples" / "synthetic_candidate.json"
        if args.output and source and _same_file(args.output, source):
            raise ValueError("output must not overwrite the input snapshot")
        if args.output and getattr(args, "db", None) and _same_file(args.output, args.db):
            raise ValueError("output must not overwrite the audit ledger")
        if source and getattr(args, "db", None) and _same_file(source, args.db):
            raise ValueError("audit ledger must not overwrite the input snapshot")
        if args.command in {"evaluate", "demo"}:
            path = args.input if args.command == "evaluate" else Path(__file__).resolve().parent.parent / "examples" / "synthetic_candidate.json"
            payload = load_json(path)
            if args.command == "demo" and payload.get("synthetic") is not True:
                raise ValueError("demo input must be explicitly synthetic")
            report = evaluate(payload)
            with Ledger(args.db) as ledger:
                decision_id = ledger.record(payload, report)
                stored = ledger.get(decision_id)
            result = {"decision_id": decision_id, "input_sha256": stored["input_sha256"],
                      "report_sha256": stored["report_sha256"], "report": report}
            _print_result(result, args)
            return 3 if args.require_qualified and report["decision"] != "QUALIFIED" else 0
        if args.command == "slate":
            result = filter_and_rank(load_json(args.input), as_of=args.as_of)
        elif args.command == "sunday":
            from .sunday import run_sunday
            manifest = load_json(args.input)
            workflow = run_sunday(manifest)
            with Ledger(args.db) as ledger:
                receipt = ledger.record_run(manifest, workflow)
            result = {**receipt, "result": workflow}
            _print_result(result, args)
            return 3 if args.require_qualified and not workflow["shortlist"] else 0
        elif args.command == "backtest":
            from .backtest import run_backtest
            result = run_backtest(load_json(args.input))
        elif args.command == "rules":
            result = {"count": len(RULES), "rules": RULES}
        elif args.command == "policy":
            from .policy import snapshot
            result = snapshot()
        else:
            with Ledger(args.db) as ledger:
                if args.command == "show":
                    result = ledger.get(args.decision_id)
                elif args.command == "show-run":
                    result = ledger.get_run(args.run_id)
                elif args.command == "performance":
                    result = ledger.performance()
                else:
                    result = ledger.settle(args.decision_id, args.result, args.source, args.at,
                                           stake_units=args.stake, closing_line=args.closing_line,
                                           closing_american_odds=args.closing_odds,
                                           failure_cause=args.failure_cause)
        _print_result(result, args)
        return 0
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(f"engine: {exc}", file=sys.stderr)
        return 2
