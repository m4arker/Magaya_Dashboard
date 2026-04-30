import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent
RAW_FILE = BASE_DIR / "Pickup_Order_List_3.csv"
DB_FILE = BASE_DIR / "magaya_analytics.db"


def load_raw_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-16",thousands=",",    )
    return df
def classify_carrier(name: str) -> str:
    if not isinstance(name, str):
        return "Other"
    
    n = name.lower()

    if any(x in n for x in ["american airlines", "british airways", "air canada", "copa", "delta"]):
        return "Commercial"
    if "conquest" in n:
        return "Conquest"
    if "ibc" in n:
        return "IBC"
    if "florida air" in n:
        return "Florida Air"
    
    return "Other"

def transform(df: pd.DataFrame) -> pd.DataFrame:
    print("Columns in raw df:", list(df.columns))
    cols_to_keep = [
        "Status",
        "Number",
        "Date",
        "Consignee",
        "Carrier",
        "Weight (lb)",
        "Pieces",
        "Booking #"

    ]

    df = df[cols_to_keep]

    df = df.rename(
        columns={
            "Status" :"status",
            "Number" : "number",
            "Date" : "date",
            "Consignee" : "consignee",
            "Carrier" : "carrier",
            "Weight (lb)" : "weight",
            "Pieces" : "pieces",
            "Booking #" : "booking_#"
            
        }
    )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["pieces", "weight"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")



    df["date_year"] = df["date"].dt.year
    df["date_month"] = df["date"].dt.month
    df["date_week"] = df["date"].dt.isocalendar().week
    df["date_quarter"] = df["date"].dt.quarter
    df["date_yyyy_mm"] = df["date"].dt.to_period("M").astype(str)
    df["etl_run_timestamp"] = datetime.now(timezone.utc)
    df["carrier_group"] = df["carrier"].apply(classify_carrier)
    return df


def load_to_sqlite(df: pd.DataFrame, db_path: Path, table_name: str = "shipments") -> None:
    conn = sqlite3.connect(db_path)

    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()


    

def main():
    print("Loading Raw CSV....")
    raw_df = load_raw_csv(RAW_FILE)

    print("Transforming....")
    clean_df = transform(raw_df)

    print(f"Loading into SQLite: {DB_FILE}")
    load_to_sqlite(clean_df, DB_FILE)

    print("Done.")

if __name__ == "__main__":
    main()