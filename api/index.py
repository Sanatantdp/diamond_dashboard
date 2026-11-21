"""Vercel serverless entrypoint — Diamond Price Dashboard (read-only viewer).

Vercel's Python runtime imports this module and serves the module-level `app`
(an ASGI application) for every incoming request. All HTTP paths are rewritten
to this function in ``vercel.json``.

The FastAPI application itself lives in ``main.py`` at the repository root, so we
put the root on ``sys.path`` before importing it. Only the read-only dashboard +
JSON API endpoints work here; the scraping / compare / upload endpoints shell out
to subprocesses and write to disk, which serverless cannot do — they keep running
wherever your pipeline runs today and write to the same Neon database this viewer
reads from.
"""

import os
import sys

# Put the repo root (parent of this /api directory) on the import path so that
# `import main` resolves to the FastAPI app, and main.py's BASE_DIR resolves to
# the bundled project root (where the static HTML files are included).
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from main import app  # noqa: E402  — re-exported for Vercel's ASGI handler

# `app` is intentionally left in the module namespace for Vercel to detect.
__all__ = ["app"]
