from typing import List, Dict, Any
from core.database import get_connection


CREATE_NEW_ANNUITY_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LPS_New_Annuity_Summary]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[LPS_New_Annuity_Summary] (
        AadhaarNumber NVARCHAR(50),
        ApplicationNumber NVARCHAR(50),
        FarmerName NVARCHAR(255),
        LandType NVARCHAR(50),
        RowwiseExtent FLOAT,
        gardenextent FLOAT,
        Base_Annuity FLOAT,
        Total_Annuity_Amount FLOAT,
        Total_Annuity_From_Table FLOAT,
        Amount_Received FLOAT,
        Difference_Amount FLOAT,
        Year1_Annuity FLOAT,
        Year2_Annuity FLOAT,
        Year3_Annuity FLOAT,
        Year4_Annuity FLOAT,
        Year5_Annuity FLOAT,
        Year6_Annuity FLOAT,
        Year7_Annuity FLOAT,
        Year8_Annuity FLOAT,
        Year9_Annuity FLOAT,
        Year10_Annuity FLOAT,
        Year11_Annuity FLOAT,
        Year12_Annuity FLOAT,
        Year13_Annuity FLOAT,
        Year14_Annuity FLOAT,
        Year15_Annuity FLOAT
    );
END
ELSE
BEGIN
    IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[new_annuity_summary]') AND name = 'CreatedDate')
        ALTER TABLE dbo.new_annuity_summary DROP COLUMN CreatedDate;
    IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[new_annuity_summary]') AND name = 'LotteryDate')
        ALTER TABLE dbo.new_annuity_summary DROP COLUMN LotteryDate;
END
"""


def create_new_annuity_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(CREATE_NEW_ANNUITY_TABLE_SQL)
    conn.commit()
    cur.close()
    conn.close()


def insert_new_annuity_rows(rows: List[Dict[str, Any]]):
    if not rows:
        return
    conn = get_connection()
    cur = conn.cursor()
    
    insert_sql = """
    INSERT INTO dbo.LPS_New_Annuity_Summary (
        AadhaarNumber, ApplicationNumber, FarmerName, LandType, 
        RowwiseExtent, gardenextent, Base_Annuity, Total_Annuity_Amount, 
        Total_Annuity_From_Table, Amount_Received, Difference_Amount,
        Year1_Annuity, Year2_Annuity, Year3_Annuity, Year4_Annuity, Year5_Annuity,
        Year6_Annuity, Year7_Annuity, Year8_Annuity, Year9_Annuity, Year10_Annuity,
        Year11_Annuity, Year12_Annuity, Year13_Annuity, Year14_Annuity, Year15_Annuity
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    
    params = []
    import math
    from datetime import datetime, date
    import pandas as pd

    def clean_val(v):
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
            return datetime(v.year, v.month, v.day)
        try:
            if isinstance(v, pd.Timestamp):
                if pd.isna(v):
                    return None
                return v.to_pydatetime()
        except Exception:
            pass
        try:
            return datetime.fromisoformat(str(v))
        except Exception:
            return None

    for r in rows:
        yearly_annuities = r.get('Yearly_Annuities', [])
        # Pad with None if less than 15 years
        while len(yearly_annuities) < 15:
            yearly_annuities.append(None)
            
        params.append([
            clean_val(r.get('AadhaarNumber')),
            clean_val(r.get('ApplicationNumber')),
            clean_val(r.get('FarmerName')),
            clean_val(r.get('LandType')),
            as_float(r.get('ROWWISEEXTENT')),
            as_float(r.get('gardenextent')),
            as_float(r.get('Base_Annuity')),
            as_float(r.get('Total_Annuity_Amount')),
            as_float(r.get('Total_Annuity_From_Table')),
            as_float(r.get('Amount_Received')),
            as_float(r.get('Difference_Amount')),
            as_float(yearly_annuities[0]),
            as_float(yearly_annuities[1]),
            as_float(yearly_annuities[2]),
            as_float(yearly_annuities[3]),
            as_float(yearly_annuities[4]),
            as_float(yearly_annuities[5]),
            as_float(yearly_annuities[6]),
            as_float(yearly_annuities[7]),
            as_float(yearly_annuities[8]),
            as_float(yearly_annuities[9]),
            as_float(yearly_annuities[10]),
            as_float(yearly_annuities[11]),
            as_float(yearly_annuities[12]),
            as_float(yearly_annuities[13]),
            as_float(yearly_annuities[14]),
        ])

    cur.fast_executemany = True
    cur.executemany(insert_sql, params)
    conn.commit()
    cur.close()
    conn.close()