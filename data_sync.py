"""Scheduler runner to execute Snowflake→Postgres sync daily at 11:00 AM IST.

Run with: python data_sync.py
This process stays running and triggers the sync each day at 11:00 Asia/Kolkata time.
"""

import os
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone

# Prefer IANA timezone if available; fallback to fixed offset (+05:30) for IST
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    TZ_IST = ZoneInfo("Asia/Kolkata")
except Exception:
    TZ_IST = timezone(timedelta(hours=5, minutes=30))

# Logging setup for scheduler
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    filename="logs/scheduler.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Import the main sync function
from daily.snowflake_postgres_sync import main as run_sync


def next_run_time_ist(hour: int = 11, minute: int = 0) -> datetime:
    """Compute the next run time in UTC corresponding to hour:minute in IST."""
    now_local = datetime.now(TZ_IST)
    target_today = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now_local >= target_today:
        target_local = target_today + timedelta(days=1)
    else:
        target_local = target_today
    return target_local.astimezone(timezone.utc)


def run_scheduler():
    logger.info("Scheduler started. Target time: 11:00 AM IST daily.")
    try:
        while True:
            next_run_utc = next_run_time_ist(11, 0)
            now_utc = datetime.now(timezone.utc)
            sleep_seconds = max(0.0, (next_run_utc - now_utc).total_seconds())
            next_run_local_str = next_run_utc.astimezone(TZ_IST).strftime("%Y-%m-%d %H:%M:%S %Z")
            logger.info(f"Next run scheduled at {next_run_local_str} (IST)")

            # Sleep until next run time
            time.sleep(sleep_seconds)

            # Execute the sync
            start = datetime.now(timezone.utc)
            logger.info(f"Starting sync at {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            try:
                run_sync()
            except Exception as e:
                logger.error(f"Sync execution error: {e}")
            end = datetime.now(timezone.utc)
            logger.info(f"Sync completed at {end.strftime('%Y-%m-%d %H:%M:%S')} UTC; duration {(end - start).total_seconds():.2f}s")
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user (KeyboardInterrupt).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-now", action="store_true")
    args = parser.parse_args()
    if args.run_now:
        run_sync()
    else:
        run_scheduler()
