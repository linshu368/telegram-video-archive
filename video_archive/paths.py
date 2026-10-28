"""Stable project root for configuration, runtime data and packaged resources."""
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]


def cli_path(value):
    """Make a user path absolute without following links before safety checks."""
    return Path(value).expanduser().absolute()
