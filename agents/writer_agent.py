from typing import List, Dict, Any
from core.database import get_connection


CREATE_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LPS_ANNUITY_SUMMARY]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[LPS_ANNUITY_SUMMARY] (
        AadhaarNumber NVARCHAR(50),
        ApplicationNumber NVARCHAR(50),
        FarmerName NVARCHAR(255),
        LandCategory NVARCHAR(50),
        CreatedDate DATETIME,
        RowwiseExtent FLOAT,
        LotteryDate DATETIME,
        Total_Annuity_Amount FLOAT,
        Amount_Received FLOAT,
        Difference_Amount FLOAT
    );
END
"""


def create_table_if_not_exists():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()
    conn.close()


def insert_rows(rows: List[Dict[str, Any]]):
    if not rows:
        return
    conn = get_connection()
    cur = conn.cursor()
    insert_sql = (
        "INSERT INTO dbo.LPS_ANNUITY_SUMMARY (AadhaarNumber, ApplicationNumber, FarmerName, LandCategory, CreatedDate, RowwiseExtent, LotteryDate, Total_Annuity_Amount, Amount_Received, Difference_Amount)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)"
    )
    params = []
    import math
    from datetime import datetime, date
    import pandas as pd

    def clean_val(v):
        # None or NaN -> None
        if v is None:
            return None
        try:
            if isinstance(v, float) and math.isnan(v):
                return None
        except Exception:
            pass
        if isinstance(v, str) and v.strip() == '':
            return None
        return v

    def as_float(v):
        v = clean_val(v)
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    def as_datetime(v):
        v = clean_val(v)
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, date):
            # convert date to datetime
            return datetime(v.year, v.month, v.day)
        # handle pandas Timestamp
        try:
            if isinstance(v, pd.Timestamp):
                if pd.isna(v):
                    return None
                return v.to_pydatetime()
        except Exception:
            pass
        # fallback parse string
        try:
            return datetime.fromisoformat(str(v))
        except Exception:
            return None

    for r in rows:
        params.append([
            clean_val(r.get('AadhaarNumber')),
            clean_val(r.get('ApplicationNumber')),
            clean_val(r.get('FarmerName')),
            clean_val(r.get('LandCategory')),
            as_datetime(r.get('CreatedDate')),
            as_float(r.get('ROWWISEEXTENT')),
            as_datetime(r.get('LotteryDate')),
            as_float(r.get('Total_Annuity_Amount')),
            as_float(r.get('Amount_Received')),
            as_float(r.get('Difference_Amount')),
        ])

    cur.fast_executemany = True
    cur.executemany(insert_sql, params)
    conn.commit()
    cur.close()
    conn.close()
