# ENGINE — NFL research engine

An executable implementation of the supplied 68-step NFL betting audit. ENGINE ranks Sunday morning and 1 p.m. Eastern games, evaluates individual player overs, enforces evidence and exclusion gates, runs reproducible simulations, and preserves decisions and results.

The original crypto worker has been replaced. `neo.py` remains a compatibility entry point for the NFL CLI.

## Run it

Python **3.12+** is required. There are no third-party runtime dependencies, API keys, or services to install.

```bash
python3 -m engine --help
python3 -m engine demo
python3 -m engine rules
python3 -m unittest discover -s tests -v
```

The demo executes **10,000 actual seeded draws** using fictional inputs. Its decision is **NO_BET**: synthetic data, unverified availability, missing research evidence, and absent model calibration must block approval. No real NFL schedule, player statistics, odds, or profitable strategy is asserted by these examples.

## Sunday workflow

1. Import a current, cited schedule. Rank games before investigating players.
2. Prepare a candidate snapshot with current-season history, exact sportsbook quote, workload assumptions, risk flags, and timestamped research for each required rule.
3. Evaluate the snapshot. Resolve every reported blocker; a numerical simulation or high research rating cannot override missing evidence.
4. Preserve the decision. After the game, record the official result and any separately reported stake.

```bash
# Demonstration schedule; the cutoff is explicitly historical/reproducible.
python3 -m engine slate examples/synthetic_schedule.json --as-of 2026-10-24T12:00:00Z

# Each evaluation is automatically recorded in .engine/engine.sqlite3.
python3 -m engine evaluate examples/synthetic_candidate.json --output .engine/report.json
python3 -m engine evaluate examples/synthetic_live.json --json
python3 -m engine performance
```

Replace example data with sourced inputs for a real run. Sunday is interpreted in `America/New_York`, including daylight saving time. Morning means kickoff before 13:00 Eastern; the early window is 13:00–13:59. The selector uses the next/current Sunday or an explicit `target_date`. It does not assume a morning fixture exists.

## Enforced decisions

`QUALIFIED` means the **supplied snapshot** meets the implemented research gates. It is not a guaranteed result or independent source authentication. `NO_BET` lists the exact missing evidence, contradictory information, exclusion, or value requirement. Invalid input exits with code 2. A valid NO_BET evaluation exits 0; use `--require-qualified` to exit 3 instead.

Hard exclusions include unknown required risk flags, fewer than three stable-role games, any injury designation in the last 14 days, a missed game within four weeks, unconfirmed final status, recent QB/play-caller changes, unresolved usage restrictions, an absolute spread of at least 10, and last-four-game snap-share population standard deviation above 0.10. Every active evidence rule must be documented or actually computed. Contradictions remain blocking even when a simulation succeeds.

Evidence timestamps cannot leak past the snapshot cutoff. Recent reporting expires after 48 hours, sportsbook quotes after 15 minutes, and live observations after 60 seconds. The cutoff itself cannot qualify a future recommendation. NFL season consistency and current-season historical games are checked.

The model resamples paired historical opportunities and efficiency, applies three explicit game scripts, and recomputes outcomes under reduced workload and efficiency. It counts exact-line wins/losses/pushes, models refunds, and calculates EV at the actual supplied price. Its 95% interval describes Monte Carlo sampling error; model and input uncertainty remain. It is an initial, **unvalidated** empirical model.

Probability qualification requires at least 100 chronological, settled, non-push holdout records for the same model, prop category, and pregame/live phase, expected calibration error at most 0.10, and at least 20 observations near the candidate's estimated probability. These are conservative operational defaults, not empirically proven guarantees. Duplicate events, synthetic calibration, and future outcomes are rejected. Positive EV must survive a conservative sampling-error screen and the sensitivity scenario.

The original weighted 0–10 research score is optional and explicitly labeled heuristic. It never becomes a win probability or bypasses a gate. Single selections are supported; the engine does not multiply same-game leg probabilities to construct parlays.

## Evidence, backtests, and results

See [input format](docs/INPUT_FORMAT.md), the complete [68-step rulebook](docs/RULEBOOK.md), and the preserved [source audit](docs/SOURCE_AUDIT.md).

```bash
python3 -m engine backtest examples/synthetic_backtest.json --output .engine/backtest.json
python3 -m engine show DECISION_ID
python3 -m engine settle DECISION_ID --result 72 --source https://YOUR-OFFICIAL-SOURCE/result --at 2026-10-25T22:00:00Z --stake 0
```

Backtests execute each historical snapshot chronologically and export calibration records with snapshot hashes and draw counts. The sample backtest is synthetic and cannot qualify a real candidate. No 500-strategy search or real historical validation has been performed.

The SQLite ledger stores original inputs, reports, hashes, rejected decisions, and one settlement per decision. Results distinguish hit rate, net units, ROI, pushes, prop categories, rejection reasons, failure causes, and closing-line value. `--stake` records a declared stake for accounting; it never places a bet. Rejected and synthetic decisions accept zero-stake research results only. Hashes detect accidental snapshot changes; this is not an externally signed ledger.

## Data integration status

This version accepts structured JSON evidence. It **does not yet fetch or independently verify** schedules, injury reports, coach interviews, sportsbook quotes, or live game feeds. Citation provenance and freshness are checked; source contents must be researched and reviewed externally. Real-data evaluation needs a chosen sportsbook, verified morning/early schedule, current player evidence, and suitable historical validation data. The local application and test suite run without network access.

## Repository layout

| Path | Purpose |
| --- | --- |
| `engine/rules.py` | 68-step catalog, evidence gates, hard exclusions |
| `engine/slate.py` | Sunday game selection and transparent suitability ranking |
| `engine/probability.py` | Executed simulations, odds/EV, sensitivity, calibration metrics |
| `engine/evaluation.py` | Final decision gates and complete audit report |
| `engine/backtest.py` | Chronological held-out snapshot execution |
| `engine/ledger.py` | Decision preservation and result accounting |
| `engine/cli.py` | CLI workflow |
| `examples/` | Explicitly fictional, reproducible inputs |
| `tests/` | Simulation, provenance, gate, live, ledger, and CLI regressions |

## Development

Use the existing checkout. Each cloud task already has its own isolated environment; do not create a worktree unless explicitly requested. Run `python3 -m unittest discover -s tests -v` before submitting changes. Keep real odds, research snapshots, private data, and generated ledgers in the ignored `.engine/` directory. Model changes need new out-of-sample evidence; a single winning Sunday is not validation.
