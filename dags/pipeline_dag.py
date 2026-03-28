from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

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

def convert_to_parquet(df, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"  Saved → {output_path}  ({len(df):,} rows)")

def task_check_input(**context):
    print(f"[Check_Input] Loading '{INPUT_FILE}' ...")
    df = load_input(INPUT_FILE)
    print(f"  Loaded {len(df):,} rows x {len(df.columns)} columns")
    has_nulls = df.isnull().any(axis=1).any()
    print(f"  Null rows found: {int(df.isnull().any(axis=1).sum())}")
    context["ti"].xcom_push(key="has_nulls", value=has_nulls)

def task_branch(**context):
    has_nulls = context["ti"].xcom_pull(key="has_nulls", task_ids="check_input")
    return "split_record" if has_nulls else "convert_to_parquet"

def task_convert_clean(**context):
    df = load_input(INPUT_FILE)
    print("[Issue Found?] No  ->  Convert to parquet")
    convert_to_parquet(df, OUTPUT_CLEAN)

def task_split_record(**context):
    df = load_input(INPUT_FILE)
    null_count = int(df.isnull().any(axis=1).sum())
    print(f"[Issue Found?] Yes  ->  {null_count:,} row(s) contain null values")
    df_clean, df_errors = split_record(df)
    print(f"  Clean rows : {len(df_clean):,}")
    print(f"  Error rows : {len(df_errors):,}")
    convert_to_parquet(df_clean,  OUTPUT_CLEAN)
    convert_to_parquet(df_errors, OUTPUT_ERRORS)

with DAG(
    dag_id="movie_ratings_pipeline",
    default_args=default_args,
    description="Check input -> split or convert to parquet",
    schedule_interval="0 8 * * *",  # ทุกวัน 08:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pipeline"],
) as dag:

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

    check_input >> branch >> [convert_to_parquet_task, split_record_task]