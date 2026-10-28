#!/usr/bin/env python3
"""Compatibility entry point; implementation lives in video_archive.pipeline.runner."""
import sys
from importlib import import_module

_implementation = import_module('video_archive.pipeline.runner')
if __name__ == '__main__':
    raise SystemExit(_implementation.main())
else:
    # Preserve module identity for callers importing the historical entry name.
    sys.modules[__name__] = _implementation
