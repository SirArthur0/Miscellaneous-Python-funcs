from pathlib import Path
import pandas as pd

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
