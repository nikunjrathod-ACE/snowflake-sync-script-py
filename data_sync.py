"""
Entrypoint script for daily Snowflake→Postgres sync.
Runs the main sync routine from daily/snowflake_postgres_sync.py.
Ensures the working directory is the project root so relative paths work
for config/ and logs/ when launched via Task Scheduler.
"""

import logging
import os

from daily.snowflake_postgres_sync import main as run_sync


def main():
    # Ensure we run from the project root regardless of scheduler context
    project_root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_root)
    logging.info("Launching data_sync entrypoint...")
    run_sync()


if __name__ == "__main__":
    main()