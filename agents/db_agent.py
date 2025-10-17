import pandas as pd
from core.database import get_connection


def fetch_lps_final() -> pd.DataFrame:
    """Fetch all columns from [dbo].[LPS_FINAL] into a DataFrame."""
    conn = get_connection()
    query = "SELECT * FROM dbo.LPS_FINAL"
    df = pd.read_sql(query, conn)
    conn.close()
    return df
