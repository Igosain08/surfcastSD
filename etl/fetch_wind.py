"""
Week 1 — Wind ETL (your team implements this file)

Goal
----
For each surf break in ``etl.config.BREAKS``, call **api.weather.gov** and build a **long-format**
DataFrame of **hourly** wind suitable for merging with buoy data.

Suggested output columns (names matter for ``merge_hourly``)
------------------------------------------------------------
- ``timestamp_utc``
- ``break_id`` — same keys as in ``BREAKS`` (e.g. ``la_jolla_shores``)
- ``wind_speed_mph`` — numeric (or pick one unit and **document it** in README)
- ``wind_direction_degrees`` — numeric degrees, or document if you keep compass strings

API flow (typical)
------------------
1. ``GET https://api.weather.gov/points/{lat},{lon}`` with headers including **User-Agent**
   (set ``NWS_USER_AGENT`` in ``etl/config.py``).
2. Read ``properties.forecastHourly`` from the JSON; ``GET`` that URL for hourly periods.
3. Parse ``windSpeed`` (often a string like ``"5 mph"``) and ``windDirection`` (often ``"NW"``).

Gotchas (read before coding)
-----------------------------
- Some **beach-only** coordinates return **404** / “Marine Forecast Not Supported” for hourly.
  You may need slightly **inland** proxy coordinates — validate with your mentor what “close enough” means.
- ``forecast/hourly`` is a **forecast**, not historical observations. If the club’s Week 1 spec
  requires *past* wind only, ask the mentor which archive to use instead (METAR, etc.).

Checklist
---------
- [ ] Respect NWS ``User-Agent`` policy (descriptive string, contact or repo URL).
- [ ] Optional: save raw JSON under ``data/raw/nws/`` for debugging (gitignored).
- [ ] ``python -m etl.fetch_wind`` prints a sensible ``head()`` from repo root.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.config import BREAKS, NWS_CACHE_DIR


def fetch_wind_all_breaks(
    breaks: dict[str, tuple[float, float]] | None = None,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Return concatenated long-format hourly wind for every break in ``breaks``
    (default: ``BREAKS``). ``cache_dir`` may be ``NWS_CACHE_DIR`` to stash JSON snapshots.
    """
    raise NotImplementedError("Implement: points → forecastHourly → parse periods → DataFrame.")


def main() -> None:
    try:
        NWS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df = fetch_wind_all_breaks(cache_dir=NWS_CACHE_DIR)
        print(df.head(12))
        print("rows:", len(df))
    except NotImplementedError:
        print("Implement this module — start at the docstring at the top of etl/fetch_wind.py")


if __name__ == "__main__":
    main()
