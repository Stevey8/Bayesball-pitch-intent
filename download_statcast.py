"""

download_statcast.py

Overview:
Statcast Data Lake Ingestion Pipeline (Bronze/Raw Layer). 
This script downloads pitch-level MLB Statcast data in monthly batches, 
processes it into highly compressed Parquet files, and stores it in a 
Hive-partitioned directory structure on Google Drive.

Design Choices:
- Environment Configured: Securely loads root storage paths via .env files.
- Fail-Fast Validation: Verifies Google Drive connectivity before attempting any downloads.
- Hive-Partitioned Storage: Organizes files by `year=YYYY/month=MM/` to enable performance gains 
  (partition pruning) when reading data in Pandas/PyArrow later.
- Atomic Writes & Failsafes: Writes data to temporary files first (`.tmp.parquet`) 
  to guarantee that network failures don't result in corrupted, half-written files.
- Smart Resumption: Automatically skips already-downloaded months to save time and API calls.
- Targeted Extraction: Filters out Spring Training and Postseason pitches, saving 
  only Regular Season (game_type = 'R') data.
- Robust Logging: Utilizes Python's native logging to track process flow, skips, 
  and errors for easy debugging.

Future Additions: 
- Fetch and append daily/monthly game metadata.

"""


import os
import logging
import calendar
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import Iterable, Optional
from pybaseball import statcast


# ============
# || Config ||
# ============

load_dotenv()

logging.basicConfig(
    level=logging.INFO, # shows INFO, WARNING, and ERROR messages
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

YEARS = range(2015, 2026)
MONTHS = [3, 4, 5, 6, 7, 8, 9, 10]
SOURCE_NAME = "statcast"

DRIVE_ROOT = Path(os.environ["DRIVE_ROOT"])
RAW_ROOT = DRIVE_ROOT / "data" / "raw" / SOURCE_NAME

DEFAULT_REDO = False
DEFAULT_CONTINUE_ON_ERROR = True


# =============
# || Helpers ||
# =============

def ensure_storage_access():
    """
    Validate that the storage location is accessible and required folders exist.
    """

    if not DRIVE_ROOT.exists():
        raise FileNotFoundError(f"Drive root does not exist: {DRIVE_ROOT}. Check your .env or connection.")
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    logging.info(f"Storage verified. Raw data root: {RAW_ROOT}")


def get_month_date_range(year: int, month: int):
    """
    Return (start_date, end_date) as YYYY-MM-DD strings for the given year/month.
    """
    last_day = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"
    return start_date, end_date


def build_output_path(year: int, month: int, raw_root = RAW_ROOT):
    """
    Return the final Parquet output path for a given year/month chunk.
    """
    return raw_root / f"year={year}" / f"month={month:02d}" / f"statcast_{year}_{month:02d}.parquet"


def build_temp_output_path(year: int, month: int, raw_root = RAW_ROOT):
    """
    Return a temp output path so incomplete writes do not masquerade as finished files.
    """
    return raw_root / f"year={year}" / f"month={month:02d}" / f"statcast_{year}_{month:02d}.tmp.parquet"


# ===================
# || Core pipeline ||
# ===================

def process_chunk(
    year: int,
    month: int,
    redo: bool,
):
    """
    Process a single year/month chunk.

    Returns an outcome_string ("success" or "skipped"; raise error if failed)
    """
    start_date, end_date = get_month_date_range(year, month)

    # paths: use temp_path first (in case writing fails). if succeed, transfer to output_path (so guaranteed to only have complete files)
    output_path = build_output_path(year, month) 
    temp_path = build_temp_output_path(year, month) 

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    

    # check: skip download if output path exists
    file_exists = output_path.exists()
    if file_exists and not redo:
        logging.info(f"[SKIP] year={year}, month={month:02d} -> file already exists")
        return "skipped"


    # start download process
    logging.info(f"[DOWNLOAD] year={year}, month={month:02d} ({start_date} to {end_date})")

    df = statcast(start_dt=start_date, end_dt=end_date)
    
    # Filter for regular season data
    if df is not None and not df.empty:
        df = df[df['game_type']=="R"].copy()

    # check emptyness
    if df is None or df.empty:
        logging.info(f"[SKIP] year={year}, month={month:02d} -> No regular season data found.")
        return "skipped"


    # write df to temp_path in parquet format
    # first make sure temp_path exists and is fresh 
    if temp_path.exists():
        temp_path.unlink()
    
    df.to_parquet(temp_path, index=False)

    # verify it was created
    if not temp_path.exists():
        raise RuntimeError(f"Temp parquet was not created: {temp_path}")

    # if succeed: replace with output_path
    temp_path.replace(output_path)
    logging.info(f"[OK] wrote {output_path} | {len(df)} rows (pitches)")
    return "success"



def run_pipeline(
    years: Iterable[int],
    months: Iterable[int],
    redo: bool = DEFAULT_REDO,
    continue_on_error: bool = DEFAULT_CONTINUE_ON_ERROR,
):
    """
    Run the monthly ingestion loop for the requested years and months.

    Returns a dict (summary)
    """

    summary = {
        "success":[],
        "skipped":[],
        "failed":[],
    }

    for year in years:
        for month in months:
            try:
                outcome = process_chunk(year, month, redo=redo) # "success" or "skipped"
                summary[outcome].append((year, month))
            except Exception as e:
                logging.error(f"[FAIL] year={year}, month={month:02d}: {e}")
                summary["failed"].append((year, month, str(e)))
                if not continue_on_error:
                    raise
    return summary 



# ============
# || main() ||
# ============

def main() -> None:

    ensure_storage_access()

    redo = DEFAULT_REDO
    continue_on_error = DEFAULT_CONTINUE_ON_ERROR

    logging.info("Starting Statcast ingestion pipeline...")

    summary = run_pipeline(
        years=YEARS,
        months=MONTHS,
        redo=redo,
        continue_on_error=continue_on_error,
    )

    logging.info("Pipeline finished.")
    logging.info(f"Success: {len(summary['success'])}")
    logging.info(f"Skipped: {len(summary['skipped'])}")
    logging.info(f"Failed: {len(summary['failed'])}")

    if summary["failed"]:
        logging.error("Failed chunks:")
        for year, month, error_message in summary["failed"]:
            logging.error(f"  - year={year}, month={month:02d}, error={error_message}")


if __name__ == "__main__":
    main()