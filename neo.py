"""Compatibility entry point. Prefer ``python -m engine --help``."""

from engine.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
