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

- Either **wide**: one row per ``hour_utc``, columns for each station's swell metrics and each
  break's wind metrics — **prefix column names** so nothing collides.
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
    """Join buoy and wind on aligned hourly timestamps; return a wide DataFrame.

    Output shape: one row per ``hour_utc``.
    Buoy columns prefixed  ``buoy_{station}_{metric}``  (e.g. ``buoy_46232_WVHT``).
    Wind columns prefixed  ``wind_{break}_{metric}``    (e.g. ``wind_la_jolla_shores_speed_mph``).

    ``how`` controls the merge join type:
      - "outer"  keeps every hour that appears in either source (NaN where one is missing).
      - "inner"  keeps only hours present in both buoy and wind data.
    """
    buoy = buoy.copy()
    wind = wind.copy()

    buoy["hour_utc"] = buoy["timestamp_utc"].dt.floor("h")
    wind["hour_utc"] = wind["timestamp_utc"].dt.floor("h")

    # --- buoy: pivot to wide ---
    swell_cols = [c for c in ["WVHT", "DPD", "MWD", "APD"] if c in buoy.columns]
    buoy_wide = buoy.pivot_table(
        index="hour_utc",
        columns="station",
        values=swell_cols,
        aggfunc="first",
    )
    # MultiIndex (metric, station) → buoy_{station}_{metric}
    buoy_wide.columns = [f"buoy_{station}_{metric}" for metric, station in buoy_wide.columns]
    buoy_wide = buoy_wide.reset_index()

    # --- wind: pivot to wide ---
    wind_cols = [c for c in ["wind_speed_mph", "wind_direction_degrees"] if c in wind.columns]
    wind_wide = wind.pivot_table(
        index="hour_utc",
        columns="break_id",
        values=wind_cols,
        aggfunc="first",
    )
    # MultiIndex (metric, break_id) → wind_{break_id}_{metric stripped of "wind_"}
    wind_wide.columns = [
        f"wind_{break_id}_{metric.replace('wind_', '')}"
        for metric, break_id in wind_wide.columns
    ]
    wind_wide = wind_wide.reset_index()

    return buoy_wide.merge(wind_wide, on="hour_utc", how=how)


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
