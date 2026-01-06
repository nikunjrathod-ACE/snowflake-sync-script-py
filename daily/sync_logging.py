import pandas as pd
import logging
import os
from datetime import datetime, timezone
from sqlalchemy import text

logger = logging.getLogger(__name__)
import numpy as np

class SyncLogger:
    def __init__(self, engine, cfg, log_config):
        self.engine = engine
        self.cfg = cfg
        self.log_config = log_config
        self.log_columns = [c.lower() for c in log_config.get("columns", [])]
        self.dfs = {}

    def log_fetched(self, df: pd.DataFrame):
        """Capture fetched columns."""
        if df.empty:
            return
            
        # Filter columns if specified, otherwise log all
        cols_to_log = [c for c in df.columns if c.lower() in self.log_columns]
        if not cols_to_log:
             cols_to_log = df.columns
             
        self.dfs["Fetched"] = df[cols_to_log].copy()
        logger.info(f"[{self.cfg['entity_name']}] Logged {len(df)} fetched rows for Excel report.")

    def log_inserted_candidates(self):
        """Identify rows that will be inserted (New keys)."""
        stage = self.cfg["temp_table"]
        target = self.cfg["target_table"]
        keys = self.cfg["key_columns"]
        
        # Build join condition
        join_cond = " AND ".join([f"s.{k} = t.{k}" for k in keys])
        
        # Select columns to log from stage
        # We need to ensure we select columns that exist in stage
        # For now, we'll try to select the configured log columns. 
        # If they aren't in stage, this query might fail. 
        # But usually log columns should be source columns.
        cols_select = ", ".join([f"s.{c}" for c in self.log_columns]) if self.log_columns else "s.*"

        sql = f"""
            SELECT {cols_select}
            FROM {stage} s
            LEFT JOIN {target} t ON {join_cond}
            WHERE t.{keys[0]} IS NULL
        """
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            self.dfs["Inserted"] = df
            logger.info(f"[{self.cfg['entity_name']}] Logged {len(df)} inserted candidate rows.")
        except Exception as e:
            logger.error(f"[{self.cfg['entity_name']}] Failed to log inserted candidates: {e}")

    def log_upserted_candidates(self):
        """Identify rows that will be upserted (Existing keys, different hash)."""
        stage = self.cfg["temp_table"]
        target = self.cfg["target_table"]
        keys = self.cfg["key_columns"]
        
        bk = self.cfg.get("bookkeeping", {})
        row_hash_col = bk.get("row_hash_col", "row_hash")

        join_cond = " AND ".join([f"s.{k} = t.{k}" for k in keys])
        
        cols_select = ", ".join([f"s.{c}" for c in self.log_columns]) if self.log_columns else "s.*"

        sql = f"""
            SELECT {cols_select}
            FROM {stage} s
            JOIN {target} t ON {join_cond}
            WHERE s.{row_hash_col} <> t.{row_hash_col}
        """
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            self.dfs["Upserted"] = df
            logger.info(f"[{self.cfg['entity_name']}] Logged {len(df)} upserted candidate rows.")
        except Exception as e:
            logger.error(f"[{self.cfg['entity_name']}] Failed to log upserted candidates: {e}")

    def log_deleted_candidates(self):
        """Identify rows that will be soft deleted."""
        target = self.cfg["target_table"]
        stage_keys = self.cfg["temp_keys_table"]
        keys = self.cfg["key_columns"]
        
        bk = self.cfg.get("bookkeeping", {})
        if not bk.get("has_lineage", False):
            return

        is_active_col = bk.get("is_active_col", "is_active")
        
        join_cond = " AND ".join([f"t.{k} = k.{k}" for k in keys])
        
        cols_select = ", ".join([f"t.{c}" for c in self.log_columns]) if self.log_columns else "t.*"

        sql = f"""
            SELECT {cols_select}
            FROM {target} t
            WHERE t.{is_active_col} = TRUE
            AND NOT EXISTS (
                SELECT 1 FROM {stage_keys} k
                WHERE {join_cond}
            )
        """
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            self.dfs["Deleted"] = df
            logger.info(f"[{self.cfg['entity_name']}] Logged {len(df)} deleted candidate rows.")
        except Exception as e:
            logger.error(f"[{self.cfg['entity_name']}] Failed to log deleted candidates: {e}")

    def save_report(self):
        """Save the collected dataframes to an Excel file."""
        if not self.dfs:
            return

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        filename = f"logs/sync_log_{self.cfg['entity_name']}_{timestamp}.xlsx"
        
        try:
            # Check if openpyxl is installed
            import openpyxl
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                non_empty = {}
                for k, v in self.dfs.items():
                    if v.empty:
                        continue
                    df = v.copy()
                    for col in df.columns:
                        s = df[col]
                        if pd.api.types.is_datetime64tz_dtype(s):
                            df[col] = s.dt.tz_convert(None)
                        elif pd.api.types.is_object_dtype(s):
                            df[col] = s.apply(
                                lambda x: x.replace(tzinfo=None) if isinstance(x, datetime) and getattr(x, "tzinfo", None) is not None else x
                            )
                        elif pd.api.types.is_integer_dtype(s):
                            df[col] = s.astype(np.int64)
                    non_empty[k] = df
                if non_empty:
                    for sheet_name, df in non_empty.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    summary_rows = []
                    for name in ["Fetched", "Inserted", "Upserted", "Deleted"]:
                        count = len(self.dfs.get(name, pd.DataFrame()))
                        summary_rows.append({
                            "sheet": name,
                            "rows": count,
                            "entity": self.cfg["entity_name"],
                            "timestamp_utc": timestamp
                        })
                    pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)
                else:
                    pd.DataFrame(
                        [
                            {"sheet": "Fetched", "rows": 0, "entity": self.cfg["entity_name"], "timestamp_utc": timestamp},
                            {"sheet": "Inserted", "rows": 0, "entity": self.cfg["entity_name"], "timestamp_utc": timestamp},
                            {"sheet": "Upserted", "rows": 0, "entity": self.cfg["entity_name"], "timestamp_utc": timestamp},
                            {"sheet": "Deleted", "rows": 0, "entity": self.cfg["entity_name"], "timestamp_utc": timestamp}
                        ]
                    ).to_excel(writer, sheet_name="Summary", index=False)
            
            logger.info(f"[{self.cfg['entity_name']}] Saved sync log report to {filename}")
        except ImportError:
            logger.error("openpyxl is not installed. Cannot save Excel report.")
        except Exception as e:
            logger.error(f"[{self.cfg['entity_name']}] Failed to save sync log report: {e}")
