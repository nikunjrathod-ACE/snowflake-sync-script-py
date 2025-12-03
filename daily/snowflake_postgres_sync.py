import snowflake.connector
import hashlib
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import logging
import json
import os
import traceback
from mail.send_email import send_email
from datetime import datetime, timedelta, timezone
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# ---------------- Logging ----------------
# Ensure logs directory exists before configuring logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    filename="logs/snowflake_postgress_sync_logs.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
LOG_FILE_PATH = "logs/snowflake_postgress_sync_logs.log"

# Initialize per-run logging per execution (not at import time)
RUN_LOG_FILE_PATH = None
_run_handler = None

def setup_per_run_log_handler():
    global RUN_LOG_FILE_PATH, _run_handler
    RUN_LOG_FILE_PATH = os.path.join(
        "logs",
        f"sync_run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
    )
    _run_handler.setLevel(logging.INFO)
    _run_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    # Attach to root so all loggers contribute
    logging.getLogger().addHandler(_run_handler)


def tail_log_lines(path: str, max_lines: int = 200) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        return "".join(lines[-max_lines:])
    except Exception:
        return ""


def send_error_alert(title: str, exc: Exception):
    """Send an email alert with traceback and recent logs."""
    tb = traceback.format_exc()
    log_snippet = tail_log_lines(LOG_FILE_PATH, 200)
    subject = f"Snowflake→Postgres Sync ERROR: {title}"
    body = (
        f"Time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Error: {exc}\n\n"
        f"Traceback:\n{tb}\n\n"
        f"Recent Logs (tail):\n{log_snippet}"
    )
    try:
        send_email(["arjav@acedataanalytics.com"], subject, body, attachments=[RUN_LOG_FILE_PATH])
    except Exception as send_exc:
        logger.error(f"Failed to dispatch error alert email: {send_exc}")


def send_run_log_email(start_time: datetime, end_time: datetime, had_error: bool) -> bool:
    duration = end_time - start_time
    status = "ERROR" if had_error else "SUCCESS"
    subject = f"Snowflake→Postgres Sync LOG: {status} ({end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC)"
    body = (
        f"Run Status: {status}\n"
        f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"Ended:   {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"Duration: {duration.total_seconds():.2f}s ({duration})\n\n"
        f"Attached: per-run log file {os.path.basename(RUN_LOG_FILE_PATH)}\n"
    )
    try:
        ok = send_email(["arjav@acedataanalytics.com"], subject, body, attachments=[RUN_LOG_FILE_PATH])
        if ok:
            return True
        else:
            logger.error("Failed to send run log email: transport returned False")
            return False
    except Exception as send_exc:
        logger.error(f"Failed to send run log email: {send_exc}")
        return False

# --------------- Utilities ---------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].replace('null', np.nan)
    return df

def apply_casts(df: pd.DataFrame, casts: dict | None):
    if not casts:
        return df
    for col, cast_type in casts.items():
        if col in df.columns:
            if cast_type == "float":
                df[col] = df[col].astype(float)
            elif cast_type == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif cast_type == "str":
                df[col] = df[col].astype(str)
    return df

def split_schema_table(fully_qualified: str) -> tuple[str, str]:
    if "." in fully_qualified:
        return tuple(fully_qualified.split(".", 1))
    return "public", fully_qualified

def now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def generate_surrogate_key(row_data: dict, key_columns: list[str], method: str = "hash") -> str:
    """
    Generate a surrogate key for a record based on the specified method.
    
    Args:
        row_data: Dictionary containing the row data
        key_columns: List of columns to use for key generation
        method: 'hash' for deterministic hash, 'sequence' for sequence-based
    
    Returns:
        Generated surrogate key as string
    """
    if method == "hash":
        # Create deterministic hash from key columns
        key_parts = []
        for col in key_columns:
            value = str(row_data.get(col, '')) if row_data.get(col) is not None else ''
            key_parts.append(value)
        
        combined_key = '|'.join(key_parts)
        return hashlib.md5(combined_key.encode('utf-8')).hexdigest()
    
    elif method == "sequence":
        # This will be handled at the database level
        return None
    
    return None

def add_surrogate_key_to_dataframe(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Add surrogate key column to dataframe based on configuration.
    """
    surrogate_config = cfg.get("surrogate_key", {})
    if not surrogate_config.get("enabled", False):
        return df
    
    method = surrogate_config.get("method", "hash")
    source_columns = surrogate_config.get("source_columns", cfg.get("key_columns", []))
    surrogate_column = surrogate_config.get("column_name", "surrogate_key")
    
    if method == "hash":
        # Generate hash-based surrogate keys
        surrogate_keys = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            surrogate_key = generate_surrogate_key(row_dict, source_columns, method)
            surrogate_keys.append(surrogate_key)
        
        df[surrogate_column] = surrogate_keys
        logger.info(f"[{cfg['entity_name']}] Generated {len(surrogate_keys)} hash-based surrogate keys")
    
    return df

# ------------- SQL Builders --------------
def build_hash_object_sql(hash_columns: list[str], hash_default_values: dict | None = None) -> str:
    """
    Build OBJECT_CONSTRUCT_KEEP_NULL('col1', col1, 'col2', col2, ...)
    - Prevent default values from overriding actual column values
    - Only include defaults for keys not present in hash_columns
    """
    parts = []
    hash_cols_set = set(hash_columns or [])
    for c in hash_columns:
        parts.append(f"'{c}', {c}")

    # Add default values only for keys not in hash_columns
    if hash_default_values:
        for col_name, default_value in hash_default_values.items():
            if col_name not in hash_cols_set:
                parts.append(f"'{col_name}', {default_value}")
    
    obj = ",\n                       ".join(parts)
    return f"OBJECT_CONSTRUCT_KEEP_NULL({obj})"

def build_payload_select_sql(cfg: dict, last_successful_time: str) -> str:
    """
    SELECT <source_columns>, MD5(TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(...))) AS row_hash,
           <source_updated_at_expr> AS source_updated_at
    FROM <source_table> [filter_cond]
    """
    # Build source columns with explicit timestamp casts to stabilize Arrow schema
    # Known issue: Snowflake pandas fetch can return mixed timestamp units (us/ns) across batches
    # which triggers "Schema at index N was different". We normalize to TIMESTAMP_NTZ(9).
    source_cols = cfg["source_columns"]
    # Allow config override to specify which columns to cast; default to common timestamp fields
    cast_to_ntz_cols = set([c.lower() for c in cfg.get("timestamp_ntz_columns", [
        "received", "modified", "created", "datalake_start_ts"
    ])])
    src_exprs = []
    for col in source_cols:
        if col.lower() in cast_to_ntz_cols:
            src_exprs.append(f"CAST({col} AS TIMESTAMP_NTZ(9)) AS {col}")
        else:
            src_exprs.append(col)
    src_cols = ", ".join(src_exprs)
    hash_default_values = cfg.get("hash_default_values", {})
    hash_obj = build_hash_object_sql(cfg["hash_columns"], hash_default_values)
    where_clause = ""
    if cfg.get("filter_cond"):
        where_clause = " " + cfg["filter_cond"].replace("{last_24_hours}", last_successful_time)

    source_updated_at_expr = cfg.get("source_updated_at_expr", "CURRENT_TIMESTAMP")
    # Handle empty source_updated_at_expr
    if not source_updated_at_expr or source_updated_at_expr.strip() == "":
        source_updated_at_expr = "CURRENT_TIMESTAMP"
    
    # Surrogate keys are intentionally NOT added in the Snowflake payload.
    # They are generated later during stage → target upsert (see build_select_from_stage).
    # This avoids staging schema drift and duplicate-column conflicts with natural keys.

    sql = f"""
        SELECT {src_cols},
               HEX_ENCODE(MD5(TO_JSON({hash_obj}))) AS row_hash,
               {source_updated_at_expr} AS source_updated_at
        FROM {cfg['source_table']}{where_clause}
    """
    return sql

def build_keys_select_sql(cfg: dict) -> str:
    """
    Build keys select SQL (natural keys only, v1 behavior).
    """
    keys = ", ".join(cfg["key_columns"]) 
    return f"SELECT DISTINCT {keys} FROM {cfg['source_table']}"



def build_on_clause(alias_left: str, alias_right: str, key_columns: list[str]) -> str:
    conds = [f"{alias_left}.{k} = {alias_right}.{k}" for k in key_columns]
    return " AND ".join(conds)

def build_insert_column_list(cfg: dict) -> list[str]:
    """
    Insert list = payload columns + surrogate key (if enabled) + bookkeeping (if present).
    """
    payload_cols = cfg["source_columns"].copy()
    
    # Add surrogate key column if enabled
    surrogate_config = cfg.get("surrogate_key", {})
    if surrogate_config.get("enabled", False):
        surrogate_column = surrogate_config.get("column_name", "surrogate_key")
        # Avoid duplicate column if surrogate column collides with payload
        if surrogate_column not in payload_cols:
            payload_cols.append(surrogate_column)
        else:
            logger.warning(
                f"[{cfg['entity_name']}] Surrogate key column '{surrogate_column}' already exists in payload; skipping duplicate in INSERT list."
            )
    
    bk = cfg.get("bookkeeping", {})
    if bk.get("has_lineage", False):
        payload_cols.extend([
            bk.get("row_hash_col", "row_hash"),
            bk.get("source_updated_at_col", "source_updated_at"),
            bk.get("last_seen_at_col", "last_seen_at"),
            bk.get("is_active_col", "is_active"),
            bk.get("deleted_at_col", "deleted_at"),
            bk.get("created_on_col", "created_on"),
            bk.get("updated_on_col", "updated_on")
        ])
    return payload_cols

def build_select_from_stage(cfg: dict) -> str:
    """
    Build SELECT clause for stage → target upsert, including surrogate key handling
    """
    src_cols = cfg["source_columns"].copy()
    
    # Add surrogate key if enabled
    surrogate_config = cfg.get("surrogate_key", {})
    if surrogate_config.get("enabled", False):
        surrogate_column = surrogate_config.get("column_name", "surrogate_key")
        # If surrogate column already exists among payload columns, do not generate again
        if surrogate_column in src_cols:
            logger.warning(
                f"[{cfg['entity_name']}] Surrogate key column '{surrogate_column}' exists in stage payload; skipping generation to avoid duplicates."
            )
        else:
            if surrogate_config.get("method") == "sequence":
                # For sequence-based keys, generate directly from sequence (no staging column needed)
                sequence_name = surrogate_config.get("sequence_name", f"{cfg['entity_name']}_surrogate_seq")
                src_cols.append(f"nextval('{sequence_name}') AS {surrogate_column}")
            else:
                # For hash-based keys, generate from source columns
                hash_source_columns = surrogate_config.get("source_columns", cfg.get("key_columns", []))
                if hash_source_columns:
                    concat_parts = []
                    for col in hash_source_columns:
                        concat_parts.append(f"COALESCE(CAST(s.{col} AS TEXT), '')")
                    
                    # Add default values to surrogate key hash if configured
                    hash_default_values = cfg.get("hash_default_values", {})
                    if hash_default_values:
                        for col_name, default_value in hash_default_values.items():
                            concat_parts.append(f"CAST({default_value} AS TEXT)")

                    surrogate_expr = f"MD5(CONCAT({', \'|\', '.join(concat_parts)})) AS {surrogate_column}"
                    src_cols.append(surrogate_expr)
    
    bk = cfg.get("bookkeeping", {})
    if bk.get("has_lineage", False):
        src_cols.extend([
            "s.row_hash",
            "s.source_updated_at",
            "NOW() AS last_seen_at",
            "TRUE AS is_active",
            "NULL AS deleted_at",
            "NOW() AS created_on",
            "NOW() AS updated_on"
        ])
    
    return ", ".join(src_cols)

def build_update_set_list(cfg: dict) -> str:
    """
    Build UPDATE SET clause for upserts, including surrogate key considerations
    """
    src_cols = cfg["source_columns"].copy()
    set_clauses = [f"{col} = EXCLUDED.{col}" for col in src_cols]
    
    bk = cfg.get("bookkeeping", {})
    if bk.get("has_lineage", False):
        # row_hash updated from EXCLUDED
        set_clauses.append(
            f"{bk.get('row_hash_col', 'row_hash')} = EXCLUDED.{bk.get('row_hash_col', 'row_hash')}"
        )
        # source_updated_at if present comes from EXCLUDED
        src_upd_col = bk.get("source_updated_at_col", "source_updated_at")
        if src_upd_col:
            set_clauses.append(f"{src_upd_col} = EXCLUDED.{src_upd_col}")
        # last_seen_at NOW(), is_active TRUE, deleted_at NULL, updated_on NOW()
        set_clauses.extend([
            f"{bk.get('last_seen_at_col', 'last_seen_at')} = NOW()",
            f"{bk.get('is_active_col', 'is_active')} = TRUE",
            f"{bk.get('deleted_at_col', 'deleted_at')} = NULL",
            f"{bk.get('updated_on_col', 'updated_on')} = NOW()"
        ])
    
    return ",\n        ".join(set_clauses)

def create_surrogate_key_sequence(engine, cfg: dict):
    """
    Create sequence for surrogate key generation if needed
    """
    surrogate_config = cfg.get("surrogate_key", {})
    if not surrogate_config.get("enabled", False) or surrogate_config.get("method") != "sequence":
        return
    
    sequence_name = surrogate_config.get("sequence_name", f"{cfg['entity_name']}_surrogate_seq")
    
    sql = f"""
        CREATE SEQUENCE IF NOT EXISTS {sequence_name}
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    """
    
    with engine.begin() as conn:
        conn.execute(text(sql))
        logger.info(f"[{cfg['entity_name']}] Created/verified surrogate key sequence: {sequence_name}")

def ensure_surrogate_key_column(engine, cfg: dict):
    """
    Ensure surrogate key column exists in target table
    """
    surrogate_config = cfg.get("surrogate_key", {})
    if not surrogate_config.get("enabled", False):
        return
    
    target_table = cfg["target_table"]
    surrogate_column = surrogate_config.get("column_name", "surrogate_key")
    column_type = surrogate_config.get("column_type", "VARCHAR(32)")
    
    # Check if column exists and add if not
    schema, table = split_schema_table(target_table)
    
    check_sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table}' 
        AND column_name = '{surrogate_column}'
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(check_sql)).fetchone()
        if not result:
            alter_sql = f"ALTER TABLE {target_table} ADD COLUMN {surrogate_column} {column_type}"
            conn.execute(text(alter_sql))
            logger.info(f"[{cfg['entity_name']}] Added surrogate key column: {surrogate_column}")

# ------------- Data Fetching --------------
def fetch_dataframe_from_snowflake(sf_conn, sql: str, cols_lower=True) -> pd.DataFrame:
    cur = sf_conn.cursor()
    try:
        cur.execute(sql)
        columns = [c[0].lower() if cols_lower else c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        cur.close()

def fetch_payload(sf_conn, cfg, last_successful_time: str) -> pd.DataFrame:
    sql = build_payload_select_sql(cfg, last_successful_time)
    logger.info(f"Snowflake payload SQL for {cfg['entity_name']}:\n{sql}")
    return fetch_dataframe_from_snowflake(sf_conn, sql)

def fetch_keys(sf_conn, cfg) -> pd.DataFrame:
    sql = build_keys_select_sql(cfg)
    logger.info(f"Snowflake keys SQL for {cfg['entity_name']}:\n{sql}")
    return fetch_dataframe_from_snowflake(sf_conn, sql)

# ------------- Data Loading ---------------
def load_keys_stage(engine, df_keys: pd.DataFrame, cfg: dict):
    schema, table = split_schema_table(cfg["temp_keys_table"])
    logger.info(f"[{cfg['entity_name']}] Loading keys into {schema}.{table} ({len(df_keys)} rows)...")

    # Match v1: apply optional casts for keys; no cleaning
    df_keys = apply_casts(df_keys, cfg.get("casts"))

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {schema}.{table}"))
    with engine.begin() as conn:
        df_keys.to_sql(name=table, con=conn, schema=schema,
                       if_exists="append", index=False, method="multi",
                       chunksize=cfg.get("chunksize", 10000))
    logger.info(f"[{cfg['entity_name']}] Keys stage loaded.")

def load_payload_stage(engine, df_payload: pd.DataFrame, cfg: dict):
    schema, table = split_schema_table(cfg["temp_table"])
    logger.info(f"[{cfg['entity_name']}] Loading payload into {schema}.{table} ({len(df_payload)} rows)...")

    df_payload = clean_dataframe(df_payload)
    df_payload.columns = df_payload.columns.str.lower()

    # Skip surrogate key handling in staging table - only handle in target table
    # df_payload = add_surrogate_key_to_dataframe(df_payload, cfg)

    # Apply casts
    df_payload = apply_casts(df_payload, cfg.get("casts"))

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {schema}.{table}"))
    with engine.begin() as conn:
        df_payload.to_sql(name=table, con=conn, schema=schema,
                          if_exists="append", index=False, method="multi",
                          chunksize=cfg.get("chunksize", 10000))
    logger.info(f"[{cfg['entity_name']}] Payload stage loaded.")

# -------- Target upsert / soft delete / snapshot --------
def upsert_stage_to_target(engine, cfg: dict):
    target = cfg["target_table"]
    stage = cfg["temp_table"]
    
    # Determine conflict resolution key
    surrogate_config = cfg.get("surrogate_key", {})
    if surrogate_config.get("enabled", False) and surrogate_config.get("use_for_conflict", True):
        # Use surrogate key for conflict resolution
        conflict_keys = [surrogate_config.get("column_name", "surrogate_key")]
    else:
        # Use original key columns
        conflict_keys = cfg["key_columns"]
    
    insert_cols = build_insert_column_list(cfg)
    select_cols = build_select_from_stage(cfg)
    update_set = build_update_set_list(cfg)
    key_list = ", ".join(conflict_keys)
    
    # Deterministic ordering for DISTINCT ON to choose latest per key
    bk = cfg.get("bookkeeping", {})
    src_upd_col = bk.get("source_updated_at_col", "source_updated_at") if bk.get("has_lineage", False) else None
    if src_upd_col:
        order_by = f"{key_list}, s.{src_upd_col} DESC"
    else:
        order_by = key_list
    # Return number of rows affected by the upsert
    sql = f"""
      WITH upsert AS (
        INSERT INTO {target} ({", ".join(insert_cols)})
        SELECT DISTINCT ON ({key_list}) {select_cols}
        FROM {stage} s
        ORDER BY {order_by}
        ON CONFLICT ({key_list}) DO UPDATE SET
          {update_set}
          WHERE {target}.row_hash <> EXCLUDED.row_hash
        RETURNING 1
      )
      SELECT COUNT(*) AS affected_rows FROM upsert;
    """
    with engine.begin() as conn:
        logger.info(f"[{cfg['entity_name']}] Upserting stage → target...")
        logger.info(f"[{cfg['entity_name']}] ON CONFLICT keys: {key_list}")
        res = conn.execute(text(sql))
        affected = res.scalar() or 0
    logger.info(f"[{cfg['entity_name']}] Upsert complete. rows_upserted={affected}")

def soft_delete_missing(engine, cfg: dict):
    target = cfg["target_table"]
    stage_keys = cfg["temp_keys_table"]
    
    # Always use natural key columns for soft delete matching
    # The keys stage table only contains natural keys, not surrogate keys
    key_cols = cfg["key_columns"]

    bk = cfg.get("bookkeeping", {})
    if not bk.get("has_lineage", False):
        logger.info(f"[{cfg['entity_name']}] Skipping soft delete (no lineage configured).")
        return

    is_active_col = bk.get("is_active_col", "is_active")
    deleted_at_col = bk.get("deleted_at_col", "deleted_at")
    updated_on_col = bk.get("updated_on_col", "updated_on")

    join_cond = " AND ".join([f"k.{k} = t.{k}" for k in key_cols])

    sql = f"""
      WITH updated AS (
        UPDATE {target} t
        SET {is_active_col} = FALSE,
            {deleted_at_col} = NOW(),
            {updated_on_col} = NOW()
        WHERE {is_active_col} = TRUE
          AND NOT EXISTS (
            SELECT 1 FROM {stage_keys} k
            WHERE {join_cond}
          )
        RETURNING 1
      )
      SELECT COUNT(*) AS deleted_rows FROM updated;
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql))
        deleted = res.scalar() or 0
    logger.info(f"[{cfg['entity_name']}] Soft deletes applied. rows_soft_deleted={deleted}")

def refresh_snapshot_from_target(engine, cfg: dict):
    snapshot = f"{cfg['target_table']}_snapshot"
    target = cfg["target_table"]
    row_hash_col = cfg.get("bookkeeping", {}).get("row_hash_col", "row_hash")

    key_cols = cfg["key_columns"]
    
    keys_select = ", ".join(key_cols)
    keys_conflict = ", ".join(key_cols)

    sql_upsert = f"""
      WITH upsert AS (
        INSERT INTO {snapshot} ({keys_select}, {row_hash_col}, last_seen_at)
        SELECT {keys_select}, {row_hash_col}, NOW()
        FROM {target}
        ON CONFLICT ({keys_conflict}) DO UPDATE
          SET {row_hash_col} = EXCLUDED.{row_hash_col},
              last_seen_at = EXCLUDED.last_seen_at
        RETURNING 1
      )
      SELECT COUNT(*) AS upserted_rows FROM upsert;
    """
    
    join_cond = " AND ".join([f"s.{k} = t.{k}" for k in key_cols])
    sql_prune = f"""
      WITH pruned AS (
        DELETE FROM {snapshot} s
        WHERE NOT EXISTS (
          SELECT 1 FROM {target} t WHERE {join_cond}
        )
        RETURNING 1
      )
      SELECT COUNT(*) AS pruned_rows FROM pruned;
    """

    with engine.begin() as conn:
        upsert_res = conn.execute(text(sql_upsert))
        upserted = upsert_res.scalar() or 0
        prune_res = conn.execute(text(sql_prune))
        pruned = prune_res.scalar() or 0
    logger.info(f"[{cfg['entity_name']}] Snapshot refreshed. rows_upserted={upserted}, rows_pruned={pruned}")

# ---------------- Main per-entity sync ----------------
def sync_table(sf_conn, engine, cfg: dict, last_success_ts: str, is_recon: bool = False):
    mode = cfg.get("mode", "B1")

    # Setup surrogate key infrastructure
    create_surrogate_key_sequence(engine, cfg)
    ensure_surrogate_key_column(engine, cfg)

    # Log surrogate key configuration if enabled
    sk = cfg.get("surrogate_key", {})
    if sk.get("enabled", False):
        logger.info(
            f"[{cfg['entity_name']}] Surrogate key enabled: method={sk.get('method','hash')}, "
            f"column={sk.get('column_name','surrogate_key')}, use_for_conflict={sk.get('use_for_conflict', True)}"
        )

    # Always load keys stage (for delete detection)
    keys_df = fetch_keys(sf_conn, cfg)
    if not keys_df.empty:
        load_keys_stage(engine, keys_df, cfg)
    else:
        logger.warning(f"[{cfg['entity_name']}] Source returned 0 keys.")

    # Load payload depending on mode
    load_payload = (mode == "B1") or is_recon
    if load_payload:
        df_payload = fetch_payload(sf_conn, cfg, last_success_ts)
        if not df_payload.empty:
            # No need to ensure staging table has surrogate key column since we handle it only in target
            load_payload_stage(engine, df_payload, cfg)
            upsert_stage_to_target(engine, cfg)
        else:
            logger.info(f"[{cfg['entity_name']}] No payload rows to upsert for this run.")

    # Soft delete anything missing today
    soft_delete_missing(engine, cfg)

    # Snapshot refresh
    refresh_snapshot_from_target(engine, cfg)

# -------------------- Entrypoint ----------------------
def main():
    # Initialize fresh per-run log file for this execution
    setup_per_run_log_handler()
    start_time = datetime.now(timezone.utc)
    logger.info(f"Job started at {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    had_error = False
    try:
        with open('config/snowflake_postgres_sync_tables_config.json', 'r') as f:
            tables = json.load(f)

        # Snowflake connection
        with open("config/snowflake-credentials.json", "r") as f:
            sf_creds = json.load(f)
        with open("snowflake_key.pem", "rb") as key_file:
            private_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())

        sf_conn = snowflake.connector.connect(
            user=sf_creds["user"],
            private_key=private_key,
            account=sf_creds["account"],
            database=sf_creds["database"],
            schema=sf_creds["schema"],
            warehouse=sf_creds["warehouse"],
            authenticator=sf_creds["authenticator"]
        )

        # Postgres engine with keepalives
        with open("config/postgres-credentials.json", "r") as f:
            pg = json.load(f)

        engine = create_engine(
            f"postgresql://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}",
            pool_pre_ping=True,
            connect_args={"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 30, "keepalives_count": 5}
        )

        # Watermark - use latest successful job end time with overlap; fallback to now-25h
        OVERLAP_HOURS = 1
        JOB_ID = 5
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT ended_at
                    FROM public.jobdetails
                    WHERE job_id = :job_id
                      AND is_success = true
                    ORDER BY ended_at DESC
                    LIMIT 1
                    """
                ),
                {"job_id": JOB_ID}
            ).fetchone()

        if row and row[0]:
            ended_at = row[0]
            if getattr(ended_at, "tzinfo", None) is not None:
                ended_at_utc = ended_at.astimezone(timezone.utc)
            else:
                ended_at_utc = ended_at.replace(tzinfo=timezone.utc)
            last_success_ts = (ended_at_utc - timedelta(hours=OVERLAP_HOURS)).strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Watermark from jobdetails ended_at: {last_success_ts} (job_id {JOB_ID}, overlap {OVERLAP_HOURS}h)")
        else:
            last_success_ts = (datetime.now(timezone.utc) - timedelta(hours=25)).strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Watermark fallback: {last_success_ts} (now - 25h)")

        # Decide recon day
        with engine.connect() as conn:
            ename = tables[0]["entity_name"]
            ctl = conn.execute(text("""
                SELECT last_full_recon_ts FROM etl_job_control WHERE entity_name=:name
            """), {"name": ename}).fetchone()
            recon = (ctl is None or ctl[0] is None or (datetime.now(timezone.utc) - ctl[0]).days >= 15)

        # Run all entities
        for cfg in tables:
            _t0 = datetime.now(timezone.utc)
            logger.info(f"=== Sync start: {cfg['entity_name']} ===")
            try:
                sync_table(sf_conn, engine, cfg, last_success_ts, is_recon=recon)
            except Exception as e:
                logger.error(f"Entity sync error [{cfg['entity_name']}]: {e}")
                send_error_alert(f"Entity {cfg['entity_name']}", e)
                # continue with the next entity
            _t1 = datetime.now(timezone.utc)
            logger.info(f"=== Sync end:   {cfg['entity_name']} (duration {( _t1 - _t0).total_seconds():.2f}s) ===")

        sf_conn.close()
        logger.info("Data synchronization completed successfully.")
    except Exception as e:
        had_error = True
        logger.error(f"An error occurred: {e}")
        send_error_alert("Global", e)
    finally:
        end_time = datetime.now(timezone.utc)
        elapsed = end_time - start_time
        logger.info(
            f"Job ended at {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC; duration: {elapsed.total_seconds():.2f}s ({elapsed})"
        )
        # Record job run in jobdetails for watermarking next day
        try:
            if 'engine' in locals() and engine is not None:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            INSERT INTO public.jobdetails (job_id, started_at, ended_at, is_success)
                            VALUES (:job_id, :started_at, :ended_at, :is_success)
                            """
                        ),
                        {
                            "job_id": 5,
                            "started_at": start_time,
                            "ended_at": end_time,
                            "is_success": (not had_error),
                        },
                    )
                logger.info("Inserted jobdetails row for job_id=5")
            else:
                logger.warning("Skipping jobdetails insert: Postgres engine not initialized.")
        except Exception as joblog_exc:
            logger.warning(f"Failed to insert jobdetails row: {joblog_exc}")

        # Email the full per-run log file after completion
        email_ok = send_run_log_email(start_time, end_time, had_error)

        # Detach and close the per-run handler before deleting the file (Windows file lock safety)
        try:
            if _run_handler is not None:
                logging.getLogger().removeHandler(_run_handler)
                _run_handler.flush()
                _run_handler.close()
        except Exception as close_exc:
            logger.warning(f"Failed to close per-run log handler: {close_exc}")

        # Delete the per-run log file only if email was sent successfully
        if email_ok:
            try:
                os.remove(RUN_LOG_FILE_PATH)
                logger.info(f"Deleted per-run log file {os.path.basename(RUN_LOG_FILE_PATH)} after email send.")
            except Exception as del_exc:
                logger.warning(f"Unable to delete per-run log file: {del_exc}")
        else:
            logger.info("Keeping per-run log file due to email failure for troubleshooting.")

if __name__ == "__main__":
    main()