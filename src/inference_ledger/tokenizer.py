"""Token counting for Ledger A.

``tiktoken`` is used when it is installed and its encoding file is already
cached; otherwise a length heuristic is substituted. The fallback is deliberately
allowed rather than fatal: a gateway that refuses to start because it cannot
reach a tokenizer CDN is worse than one that counts approximately and reports the
disagreement. Approximate counts surface honestly as ``TOKENIZER_MISMATCH`` drift
instead of hiding inside a number nobody checks.
"""

from __future__ import annotations

import functools
from collections.abc import Callable

_CHARS_PER_TOKEN = 4


def _heuristic(text: str) -> int:
    return max(1, len(text) // _CHARS_PER_TOKEN) if text else 0


@functools.lru_cache(maxsize=8)
def encoder_for(model: str) -> Callable[[str], int]:
    """Return a token counter for ``model``, falling back when unavailable."""
    try:
        import tiktoken

        try:
            encoding = tiktoken.encoding_for_model(model)
        except KeyError:
            encoding = tiktoken.get_encoding("o200k_base")
    except Exception:
        return _heuristic

    def count(text: str) -> int:
        return len(encoding.encode(text)) if text else 0

    return count


def is_exact(model: str) -> bool:
    """Whether counts for ``model`` come from a real tokenizer.

    Exposed as a metric: a fleet silently running on the heuristic explains a
    baseline of tokenizer-mismatch drift that would otherwise look like a bug.
    """
    return encoder_for(model) is not _heuristic
