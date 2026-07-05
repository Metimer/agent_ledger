"""Console entrypoint for AgentLedger."""

from __future__ import annotations

import sys

from . import _native


def main() -> int:
    return int(_native.run_cli(sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
