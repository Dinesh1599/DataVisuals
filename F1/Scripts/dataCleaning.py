import pandas as pd
from table_logger import TableLogger


def clean_column_names(df, log):
    before = df.columns.tolist()
    print(before)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace(r"[^a-zA-Z0-9_]", "", regex=True)
    )
    log.info(f"Column names cleaned. BEFORE={before}, AFTER={df.columns.tolist()}")
    return df


def remove_duplicates(df, log):
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    log.info(f"Duplicates removed: {removed}")
    return df


def clean_strings(df, log):
    for col in df.select_dtypes(include="object").columns:
        original = df[col].copy()
        df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        if not df[col].equals(original):
            log.info(f"String normalization applied to column: {col}")
    return df


def clean_missing(df, log):
    for col in df.columns:
        before = df[col].isna().sum()
        if df[col].dtype in ["float64", "int64"]:
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].fillna("unknown")
        after = df[col].isna().sum()

        if before != after:
            log.info(f"Missing values filled for '{col}'. BEFORE={before}, AFTER={after}")
    return df


def clean_dataframe(df, table_name):
    log = TableLogger(table_name)

    log.info("---- CLEANING STARTED ----")
    df = clean_column_names(df, log)
    #df = clean_strings(df, log)
    # df = clean_missing(df, log)
    # df = remove_duplicates(df, log)
    log.info(f"---- CLEANING FINISHED. Final rows = {len(df)} ----")

    return df
