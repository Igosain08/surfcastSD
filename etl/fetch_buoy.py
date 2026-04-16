"""
Week 1 — Buoy ETL (your team implements this file)

Goal
----
Download NDBC **stdmet realtime2** text for stations **46232** (Point Loma South) and **46254**
(Mission Bay West), **cache** the raw `.txt` under ``data/raw/ndbc/``, parse into a **pandas**
DataFrame.

Output contract (``fetch_buoy_swell``)
--------------------------------------
Return one DataFrame (all stations concatenated) with at least:

- ``timestamp_utc`` — timezone-aware UTC (use ``pd.Timestamp`` with ``tz`` or ``utc=True``)
- ``station`` — string, e.g. ``"46232"``
- ``WVHT``, ``DPD``, ``MWD``, ``APD`` — numeric where possible (NDBC uses ``MM`` / sentinels for missing)

Filter to roughly the **last ``days``** of rows (based on timestamps in the file).

Hints
-----
- Feed URL pattern is in ``etl/config.NDBC_REALTIME2_URL`` / ``NDBC_STATIONS``.
- File format: comment lines start with ``#``; the **column header** line also starts with ``#``
  and begins with ``YY`` or ``YYYY``. Data rows may use **4-digit years** even when the header
  says ``YY``.
- Parse carefully: buoy data can have gaps; decide how you handle missing rows (document in PR).

References
----------
- https://www.ndbc.noaa.gov/measdes.shtml
- https://www.ndbc.noaa.gov/faq/stdmet.shtml (stdmet)

Checklist before opening a PR
-----------------------------
- [ ] Raw files cached locally under ``data/raw/ndbc/`` (paths in ``.gitignore`` / README — do not commit huge caches unless mentor says so)
- [ ] ``python -m etl.fetch_buoy`` prints a sensible ``head()`` when run from repo root
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.config import NDBC_CACHE_DIR, NDBC_STATIONS


def download_station_txt(station: str, cache_dir: Path | None = None) -> Path:
    """Download realtime2 stdmet for ``station`` and save under ``cache_dir`` (default: ``NDBC_CACHE_DIR``)."""
    raise NotImplementedError("Implement: HTTP GET, write file, return path.")


def parse_ndbc_stdmet_txt(path: Path) -> pd.DataFrame:
    """Parse one cached NDBC ``realtime2`` file into a raw column DataFrame (as strings or mixed)."""
    raise NotImplementedError("Implement: skip ``#`` header noise, build datetime + keep swell cols.")


def fetch_buoy_swell(
    stations: tuple[str, ...] = NDBC_STATIONS,
    days: int = 30,
    use_cache: bool = False,
) -> pd.DataFrame:
    """
    End state for Week 1 buoy work: download (or use cache), parse, concatenate, filter by ``days``.

    ``use_cache``: if True and a cached file exists, skip re-downloading.
    """
    raise NotImplementedError("Wire up download + parse + concat + date filter.")


def main() -> None:
    try:
        df = fetch_buoy_swell(days=30, use_cache=False)
        print(df.head())
        print("rows:", len(df))
    except NotImplementedError:
        print("Implement this module — start at the docstring at the top of etl/fetch_buoy.py")


if __name__ == "__main__":
    main()
