"""Drive every scenario under load and assert convergence + attribution.

Shape of a run: start load, wait for steady state, inject the fault, hold, stop
injecting, wait one full settlement window, then query Postgres and assert.
Output is a table of scenario / requests / residual drift / reasons seen, which
is the artifact that goes in the README.
"""

from __future__ import annotations

from chaos.scenarios import SCENARIOS


def main() -> int:
    raise NotImplementedError(f"chaos runner — milestone 2 ({len(SCENARIOS)} scenarios declared)")


if __name__ == "__main__":
    raise SystemExit(main())
