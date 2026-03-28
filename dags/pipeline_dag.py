from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
INPUT_FILE    = "/opt/airflow/data/movie_ratings.csv"
OUTPUT_CLEAN  = "/opt/airflow/output/clean.parquet"
OUTPUT_ERRORS = "/opt/airflow/output/errors.parquet"
# ─────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def load_input(filepath):
    ext = os.path.splitext(filepath)[-1].lower()
    loaders = {
        ".csv":     pd.read_csv,
        ".xlsx":    pd.read_excel,
        ".json":    pd.read_json,
        ".parquet": pd.read_parquet,
    }
    loader = loaders.get(ext)
    if loader is None:
        raise ValueError(f"Unsupported file format: {ext}")
    return loader(filepath)

def split_record(df):
    mask_errors = df.isnull().any(axis=1)
    return df[~mask_errors].copy(), df[mask_errors].copy()

def save_parquet(df, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    log.info(f"  Saved -> {output_path}  ({len(df):,} rows)")

def task_check_input(**context):
    log.info("=" * 50)
    log.info("TASK: check_input")
    log.info("=" * 50)
    log.info(f"INPUT PATH : {INPUT_FILE}")
    df = load_input(INPUT_FILE)
    log.info(f"Total rows    : {len(df):,}")
    log.info(f"Total columns : {len(df.columns)}")
    log.info(f"Columns       : {list(df.columns)}")
    null_per_col = df.isnull().sum()
    log.info("Null count per column:")
    for col, cnt in null_per_col.items():
        log.info(f"  {col}: {cnt}")
    has_nulls = df.isnull().any(axis=1).any()
    null_rows = int(df.isnull().any(axis=1).sum())
    log.info(f"Rows with null : {null_rows:,}")
    log.info(f"Issue found   : {has_nulls}")
    context["ti"].xcom_push(key="has_nulls", value=has_nulls)

def task_branch(**context):
    log.info("=" * 50)
    log.info("TASK: issue_found (branch)")
    log.info("=" * 50)
    has_nulls = context["ti"].xcom_pull(key="has_nulls", task_ids="check_input")
    decision = "split_record" if has_nulls else "convert_to_parquet"
    log.info(f"Decision -> {decision}")
    return decision

def task_convert_clean(**context):
    log.info("=" * 50)
    log.info("TASK: convert_to_parquet")
    log.info("=" * 50)
    log.info("No null values found — converting full dataframe")
    df = load_input(INPUT_FILE)
    log.info(f"INPUT  : {INPUT_FILE}  ({len(df):,} rows)")
    save_parquet(df, OUTPUT_CLEAN)
    log.info(f"OUTPUT : {OUTPUT_CLEAN}")

def task_split_record(**context):
    log.info("=" * 50)
    log.info("TASK: split_record")
    log.info("=" * 50)
    df = load_input(INPUT_FILE)
    null_count = int(df.isnull().any(axis=1).sum())
    log.info(f"INPUT        : {INPUT_FILE}  ({len(df):,} rows)")
    log.info(f"Rows with null: {null_count:,}")
    df_clean, df_errors = split_record(df)
    log.info(f"Clean rows   : {len(df_clean):,}")
    log.info(f"Error rows   : {len(df_errors):,}")
    save_parquet(df_clean,  OUTPUT_CLEAN)
    save_parquet(df_errors, OUTPUT_ERRORS)
    log.info(f"OUTPUT clean  : {OUTPUT_CLEAN}")
    log.info(f"OUTPUT errors : {OUTPUT_ERRORS}")

with DAG(
    dag_id="movie_ratings_pipeline",
    default_args=default_args,
    description="Check input -> split or convert to parquet",
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_input = PythonOperator(
        task_id="check_input",
        python_callable=task_check_input,
    )

    branch = BranchPythonOperator(
        task_id="issue_found",
        python_callable=task_branch,
    )

    convert_to_parquet_task = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=task_convert_clean,
    )

    split_record_task = PythonOperator(
        task_id="split_record",
        python_callable=task_split_record,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # branch ไปทั้งสองทาง
    start >> check_input >> branch >> [split_record_task, convert_to_parquet_task]

    # เชื่อม split_record -> convert_to_parquet ในเชิง UI (แต่ convert จะ skipped ถ้า branch ไม่เลือก)
    split_record_task >> convert_to_parquet_task

    # ทั้งคู่วิ่งมาหา end
    convert_to_parquet_task >> end