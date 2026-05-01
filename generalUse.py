from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import pandas as pd
import numpy as np

# RegEx module used on 2nd func
import re

# logging section

###### How it works ######
#
# 1. Defining timezone
# 2. Defining where the file will be saved
# 3. Create custom formatter East US timestamps 
# 4. Send log to both console and file
# 

# define TimeZone
# standard time and dayligh saving
east_tz = ZoneInfo("America/New_York")


# folder where log files will be stored
log_dir = Path(r'C:/Users/Arthu/Documents/Miscellaneous code/logs')
# if folder doesn't exist, creates a new one
log_dir.mkdir(parents=True, exist_ok=True)
# builds path+filename
log_file = log_dir / f"process_{datetime.now(east_tz).strftime('%Y-%m-%d_%H-%M-%S')}.log"

# class the will receive the expected format
class EastUSFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, east_tz)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
# sends the actual message
logger = logging.getLogger('process')
# tells the logger to accept messages at INFO level
logger.setLevel(logging.INFO)
# clean handlers (best practice)
logger.handlers.clear()

# defines the format of each log line
# asctime = timestamp
# lefvelname = INFO, WARNING, ERROR, etc.
# message = actual text
formatter = EastUSFormatter("%(asctime)s | %(levelname)s | %(message)s")

# handlers that sends message to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# send logs to the file path, keep the same console format
file_handler = logging.FileHandler(log_file, encoding="utf-8")
file_handler.setFormatter(formatter)

# links both handlers to the logger, before this nothing is linked
logger.addHandler(console_handler)
logger.addHandler(file_handler)

logger.info("Starting NOW!!")

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
logger.info(" ###### standardize_columns over ###### ")
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
logger.info(" ###### condi_column over, SEGMENT column added to df ###### ")


# 6. Change file to uppercase,keeping current format
def to_uppercase(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        text = file.read()

    with open(file_path, "w", encoding='utf-8') as file:
        file.write(text.upper())

txt = "C:/Users/Arthu/Downloads/text.txt"
to_uppercase(txt)
logger.info(" ###### to_uppercase over, txt converted to uppercase, format kept the same ######")