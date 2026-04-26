from pathlib import Path
import pandas as pd
import numpy as np

# RegEx module used on 2nd func
import re

# 1. read (almost) any file
def read_data(file_path: str | Path) -> pd.DataFrame:
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path)
    elif suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)
    elif suffix == ".pqt":
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

path = "C:/Users/Arthu/Downloads/data.xlsx"
df = read_data(path)
df.info()



# 2. standardize column names
# clean and convert columns to uppercase
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def clean_col(col: str) -> str:
        col = str(col).strip().upper()
        col = re.sub(r"[^\w\s]", "", col)
        col = re.sub(r"\s+", "_", col)
        col = re.sub(r"_+", "_", col)
        return col.strip("_")
    
    df.columns = [clean_col(col) for col in df.columns]
    return df

df = standardize_columns(df)

df.info()



# 3. Null summary
# return count and percentage of nulls per column
def null_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = pd.DataFrame({
        "null_count": df.isna().sum(),
        "null_pct": (df.isna().mean() * 100).round(2),
        "dtype": df.dtypes.astype(str)
    }).sort_values(by="null_count", ascending=False)

    return summary.reset_index(names='column')

dfNullSumm = null_summary(df)

print(dfNullSumm)


# 4. duplicated key check
def find_duplicate(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    dupes = df[df.duplicated(subset=keys, keep=False)].copy()
    return dupes.sort_values(by=keys).reset_index(drop=True)

dupes = find_duplicate(df, ["SUBSCRIPTIONID"])
print(dupes)


# 5. create conditional column
def condi_column (df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # can be used for one or multiple columns
    cols = ['CUSTO_SCAN', 'CUSTO_MS']
    # regex is removing anything besides numbers
    for col in cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r'\D', "", regex=True)
            .astype(float)
        )

    # change conditions accordingly
    conditions = [
        df['CUSTO_SCAN'] >= 100.000,
        df['CUSTO_SCAN'] >= 50.000,
        df['CUSTO_SCAN'] < 50.000
    ]
    options = ['High', 'Medium', 'Low']

    df['SEGMENT'] = np.select(conditions, options, default='Unkown')
    return df

df_seg = condi_column(df)
print(df_seg['SEGMENT'])