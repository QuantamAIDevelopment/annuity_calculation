from fastapi import APIRouter
from agents.db_agent import fetch_lps_final
from agents.annuity_agent import compute_annuity_for_row
from agents.new_annuity_writer import create_new_annuity_table, insert_new_annuity_rows

router = APIRouter()


@router.post('/execute_new_annuity')
def execute_new_annuity_workflow():
    # Fetch data
    df = fetch_lps_final()

    # Compute rows with new annuity calculation
    rows = []
    for _, r in df.iterrows():
        row = r.to_dict()
        computed = compute_annuity_for_row(row)
        row.update(computed)
        rows.append(row)

    # Create new table and insert
    create_new_annuity_table()
    # Insert in chunks
    from core.utils import chunked
    for chunk in chunked(rows, 500):
        insert_new_annuity_rows(chunk)

    return {"message": "✅ LPS_New_Annuity_Summary table created and populated successfully with updated annuity calculations!"}
