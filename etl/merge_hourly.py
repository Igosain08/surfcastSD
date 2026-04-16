"""
Week 1 — Merge buoy + wind on a common hourly clock (your team implements this file)

Inputs (agree on these with the rest of the club)
-------------------------------------------------
**Buoy** DataFrame from ``fetch_buoy.fetch_buoy_swell`` — long format, includes at least
``timestamp_utc``, ``station``, and swell columns.

**Wind** DataFrame from ``fetch_wind.fetch_wind_all_breaks`` — long format, includes at least
``timestamp_utc``, ``break_id``, and wind columns.

Output
------
One table keyed by **hour** (UTC), typically:

- Either **wide**: one row per ``hour_utc``, columns for each station’s swell metrics and each
  break’s wind metrics — **prefix column names** so nothing collides.
- Or **long** — if you choose long, document it and update ``scripts/run_week1.py`` + README.

Implementation notes
----------------------
- Align timestamps to a **common hour** (e.g. ``dt.floor("h")``) before joining.
- ``how="outer"`` is useful while debugging missing overlap; switch to ``inner`` if that matches
  the Week 1 spec.

Checklist
---------
- [ ] ``merge_hourly(buoy, wind)`` runs without error once upstream ETL works.
- [ ] Add a small **unit test** in ``tests/`` using fake tiny DataFrames (no network).
"""

from __future__ import annotations

import pandas as pd


def merge_hourly(
    buoy: pd.DataFrame,
    wind: pd.DataFrame,
    how: str = "outer",
) -> pd.DataFrame:
    """Join buoy and wind on aligned hourly timestamps; return a single DataFrame."""
    raise NotImplementedError("Implement pivot and/or join logic; see module docstring.")


def main() -> None:
    try:
        from etl.fetch_buoy import fetch_buoy_swell
        from etl.fetch_wind import fetch_wind_all_breaks

        buoy = fetch_buoy_swell(days=30, use_cache=True)
        wind = fetch_wind_all_breaks()
        merged = merge_hourly(buoy, wind)
        print(merged.head())
        print("shape:", merged.shape)
    except NotImplementedError:
        print(
            "A Week 1 module is still a stub — implement fetch_buoy, fetch_wind, and merge_hourly "
            "(see docstrings in etl/)."
        )


if __name__ == "__main__":
    main()
