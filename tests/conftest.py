"""Put the repo root on sys.path so tests can `import config` / `import live.main`."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
