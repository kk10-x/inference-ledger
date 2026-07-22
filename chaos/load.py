"""Poisson-burst load generator with configurable mid-stream disconnects.

Bursty arrivals matter: a uniform request rate never produces the overlapping
in-flight streams that make shutdown and rebalance interesting.
"""

from __future__ import annotations


def main() -> int:
    raise NotImplementedError("load generator — milestone 1")


if __name__ == "__main__":
    raise SystemExit(main())
