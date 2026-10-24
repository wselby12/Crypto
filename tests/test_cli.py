import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class CLITests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run([sys.executable, "-B", "-m", "engine", *map(str, args)],
                              cwd=ROOT, text=True, capture_output=True, timeout=30)

    def test_demo_executes_simulation_and_preserves_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "ledger.sqlite3"
            result = self.run_cli("demo", "--json", "--db", db)
            self.assertEqual(result.returncode, 0, result.stderr)
            data = json.loads(result.stdout)
            self.assertEqual(data["report"]["decision"], "NO_BET")
            self.assertTrue(data["report"]["synthetic"])
            self.assertEqual(data["report"]["model"]["simulation"]["draws"], 10000)
            stored = self.run_cli("show", data["decision_id"], "--db", db, "--json")
            self.assertEqual(stored.returncode, 0, stored.stderr)
            self.assertEqual(json.loads(stored.stdout)["report_sha256"], data["report_sha256"])
            performance = self.run_cli("performance", "--db", db, "--json")
            summary = json.loads(performance.stdout)
            self.assertEqual(summary["rejected_decisions"], 1)
            self.assertEqual(summary["settled_bets"], 0)

    def test_gate_exit_code_and_human_output(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_cli("demo", "--db", Path(directory) / "audit.sqlite3", "--require-qualified")
            self.assertEqual(result.returncode, 3, result.stderr)
            self.assertIn("SYNTHETIC DEMO | NO_BET", result.stdout)
            self.assertIn("10,000", result.stdout)

    def test_slate_filters_out_later_games(self):
        result = self.run_cli("slate", "examples/synthetic_schedule.json", "--as-of", "2026-10-24T12:00:00Z", "--json")
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual({row["window"] for row in data["games"]}, {"morning", "early"})
        self.assertEqual(len(data["games"]), 2)
        self.assertTrue(data["synthetic"])

    def test_live_and_backtest_paths_execute(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_cli("evaluate", "examples/synthetic_live.json", "--json", "--db", Path(directory) / "audit.sqlite3")
            self.assertEqual(result.returncode, 0, result.stderr)
            data = json.loads(result.stdout)
            self.assertTrue(data["report"]["model"]["live"]["executed"])
            self.assertEqual(data["report"]["decision"], "NO_BET")
            backtest = self.run_cli("backtest", "examples/synthetic_backtest.json", "--json")
            self.assertEqual(backtest.returncode, 0, backtest.stderr)
            report = json.loads(backtest.stdout)
            self.assertEqual(report["summary"]["cases_executed"], 2)
            self.assertEqual(report["summary"]["total_simulation_draws"], 2000)
            self.assertTrue(report["calibration"]["synthetic"])

    def test_output_cannot_destroy_source_or_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.json"
            source.write_text((ROOT / "examples/synthetic_candidate.json").read_text())
            original = source.read_bytes()
            for args in (("--output", source), ("--db", source)):
                result = self.run_cli("evaluate", source, *args)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(source.read_bytes(), original)
            db = Path(directory) / "audit.sqlite3"
            result = self.run_cli("demo", "--db", db, "--output", db)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(db.exists())
            alias = Path(directory) / "input-alias.json"
            alias.hardlink_to(source)
            result = self.run_cli("evaluate", source, "--output", alias)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(source.read_bytes(), original)
        demo = ROOT / "examples/synthetic_candidate.json"
        original = demo.read_bytes()
        for option in ("--output", "--db"):
            result = self.run_cli("demo", option, demo)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(demo.read_bytes(), original)

    def test_rules_cover_all_68_and_legacy_entry_is_nfl_cli(self):
        result = self.run_cli("rules", "--json")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["count"], 68)
        legacy = subprocess.run([sys.executable, "-B", "neo.py", "--help"], cwd=ROOT,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(legacy.returncode, 0)
        self.assertIn("NFL", legacy.stdout)

    def test_sunday_command_persists_the_complete_run_and_rejections(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "audit.sqlite3"
            command = self.run_cli("sunday", "examples/synthetic_sunday.json", "--json", "--db", db)
            self.assertEqual(command.returncode, 0, command.stderr)
            result = json.loads(command.stdout)
            self.assertEqual(result["result"]["overview"]["no_bet_count"], 2)
            self.assertEqual(result["result"]["shortlist"], [])
            self.assertEqual(len(result["decision_ids"]), 2)
            stored = self.run_cli("show-run", result["run_id"], "--db", db, "--json")
            self.assertEqual(stored.returncode, 0, stored.stderr)
            record = json.loads(stored.stdout)
            self.assertEqual(record["report_sha256"], result["report_sha256"])
            self.assertEqual(record["result"], result["result"])
            self.assertEqual(len(record["manifest"]["candidates"]), 2)
            self.assertTrue(all(row["decision"] == "NO_BET" for row in record["decisions"]))
            summary = json.loads(self.run_cli("performance", "--db", db, "--json").stdout)
            self.assertEqual(summary["rejected_decisions"], 2)
            self.assertEqual(summary["settled_bets"], 0)

    def test_empty_sunday_shortlist_has_explicit_gate_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            command = self.run_cli("sunday", "examples/synthetic_sunday.json", "--require-qualified", "--db", Path(directory) / "audit.sqlite3")
            self.assertEqual(command.returncode, 3, command.stderr)
            self.assertIn("0 current selections", command.stdout)
            self.assertIn("Complete run saved", command.stdout)

    def test_versioned_policy_is_available_from_the_cli(self):
        command = self.run_cli("policy", "--json")
        self.assertEqual(command.returncode, 0, command.stderr)
        policy = json.loads(command.stdout)
        self.assertEqual(policy["id"], "audit-68-v2")
        self.assertEqual(policy["calibration"]["minimum_distinct_games"], 30)
        self.assertEqual(len(policy["sha256"]), 64)
