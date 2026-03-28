import pandas as pd
import os

# ─────────────────────────────────────────────
# CONFIGURATION — edit these before running
# ─────────────────────────────────────────────
INPUT_FILE    = r"C:\Users\PC-1802\Downloads\movie_ratings.csv"
OUTPUT_CLEAN  = r"C:\Users\PC-1802\Downloads\output\clean.parquet"
OUTPUT_ERRORS = r"C:\Users\PC-1802\Downloads\output\errors.parquet"
# ─────────────────────────────────────────────


def load_input(filepath: str) -> pd.DataFrame:
    ext = os.path.splitext(filepath)[-1].lower()
    loaders = {
        ".csv":     pd.read_csv,
        ".xlsx":    pd.read_excel,
        ".xls":     pd.read_excel,
        ".json":    pd.read_json,
        ".parquet": pd.read_parquet,
    }
    loader = loaders.get(ext)
    if loader is None:
        raise ValueError(f"Unsupported file format: {ext}")
    return loader(filepath)


def split_record(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    mask_errors = df.isnull().any(axis=1)
    return df[~mask_errors].copy(), df[mask_errors].copy()


def convert_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"  Saved → {output_path}  ({len(df):,} rows)")


def run_pipeline(
    input_file: str = INPUT_FILE,
    output_clean: str = OUTPUT_CLEAN,
    output_errors: str = OUTPUT_ERRORS,
) -> None:

    print("▶ START")

    print(f"\n[Check_Input] Loading '{input_file}' ...")
    df = load_input(input_file)
    print(f"  Loaded {len(df):,} rows × {len(df.columns)} columns")

    has_nulls = df.isnull().any(axis=1).any()

    if not has_nulls:
        print("\n[Issue Found?] No  →  Convert to parquet")
        convert_to_parquet(df, output_clean)

    else:
        null_count = df.isnull().any(axis=1).sum()
        print(f"\n[Issue Found?] Yes  →  {null_count:,} row(s) contain null values")
        print("[Split_Record] Splitting dataframe ...")

        df_clean, df_errors = split_record(df)
        print(f"  Clean rows : {len(df_clean):,}")
        print(f"  Error rows : {len(df_errors):,}")

        print("\n[Split_Record] Saving both dataframes ...")
        convert_to_parquet(df_clean,  output_clean)
        convert_to_parquet(df_errors, output_errors)

    print("\n■ END")


if __name__ == "__main__":
    run_pipeline()