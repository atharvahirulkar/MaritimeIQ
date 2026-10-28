"""Load project-root `.env` into os.environ (does not override existing vars)."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from maritimeiq.utils.paths import ROOT


@lru_cache(maxsize=1)
def load_env(dotenv_path: Path | None = None) -> Path | None:
    """Load `.env` from the repo root. Safe to call multiple times."""
    path = dotenv_path or (ROOT / ".env")
    try:
        from dotenv import load_dotenv
    except ImportError:
        return None
    if path.is_file():
        load_dotenv(path, override=False)
        return path
    return None
