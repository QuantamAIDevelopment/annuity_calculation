from fastapi import APIRouter
from agents.db_agent import fetch_lps_final
from agents.annuity_agent import compute_annuity_for_row
from agents.writer_agent import create_table_if_not_exists, insert_rows

router = APIRouter()


@router.post('/execute')
def execute_workflow():
    # Fetch data
    df = fetch_lps_final()

    # Compute rows
    rows = []
    for _, r in df.iterrows():
        row = r.to_dict()
        computed = compute_annuity_for_row(row)
        row.update(computed)
        rows.append(row)

    # Create table and insert
    create_table_if_not_exists()
    # Insert in chunks
    from core.utils import chunked
    for chunk in chunked(rows, 500):
        insert_rows(chunk)

    return {"message": "✅ LPS_ANNUITY_SUMMARY table created and populated successfully!"}
