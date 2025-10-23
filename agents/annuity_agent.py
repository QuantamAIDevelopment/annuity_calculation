import datetime
from typing import Dict, Any


def compute_annuity_for_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Compute annuity values for a single record (dictionary-like row).

    Expects keys used in logic to exist in row. Returns a dict with computed
    Total_Annuity_Amount, Amount_Received, Difference_Amount and some metadata.
    """
    # Helper to safely fetch values
    def val(k, default=None):
        return row.get(k, default)

    OWNERTYPE = val('OWNERTYPE')
    Form_914_Agreement_Date = val('Form_914_Agreement_Date')
    # Handle pandas Timestamp / NaT and string/date inputs safely
    try:
        import pandas as _pd
        # If value is NaT or NA, normalize to None
        if _pd.isna(Form_914_Agreement_Date):
            Form_914_Agreement_Date = None
        elif isinstance(Form_914_Agreement_Date, _pd.Timestamp):
            Form_914_Agreement_Date = Form_914_Agreement_Date.date()
    except Exception:
        # fallback: try parsing string
        try:
            if isinstance(Form_914_Agreement_Date, str):
                Form_914_Agreement_Date = datetime.datetime.strptime(Form_914_Agreement_Date, '%Y-%m-%d').date()
        except Exception:
            # leave as-is if conversion fails
            pass

    ROWWISEEXTENT = float(val('ROWWISEEXTENT'))
    LandType = int(val('LandType'))
    MutationAppNo = val('MutationAppNo')
    MUTATIONJOINTAPPNO = val('MUTATIONJOINTAPPNO')
    gardenextent = float(val('gardenextent') or 0)

    def base_rate():
        if LandType == 1:
            return 30000
        elif LandType == 2 or LandType == 3:
            return 50000
        else:
            return 0

    Annuity_Clac = 0
    if OWNERTYPE == 'S':
        if Form_914_Agreement_Date and Form_914_Agreement_Date < datetime.date(2015, 2, 1):
            Annuity_Clac = base_rate() if ROWWISEEXTENT < 1 else ROWWISEEXTENT * base_rate()
        else:
            Annuity_Clac = ROWWISEEXTENT * base_rate()
    elif OWNERTYPE == 'M':
        cond = ((MutationAppNo != 'NULL' and MUTATIONJOINTAPPNO != 'NULL'))
        if cond:
            if Form_914_Agreement_Date and Form_914_Agreement_Date < datetime.date(2015, 2, 1):
                Annuity_Clac = base_rate() if ROWWISEEXTENT < 1 else ROWWISEEXTENT * base_rate()
            else:
                Annuity_Clac = ROWWISEEXTENT * base_rate()

    # Calculate annuity for each year (10% increase each year based on base annuity)
    base_annuity = float(Annuity_Clac)
    yearly_annuities = []
    total_annuity = 0.0
    
    for year in range(1, 16):
        if year <= 10:
            # Each year increases by 10% of the base annuity amount
            current_year_annuity = base_annuity + (base_annuity * 0.10 * (year - 1))
        else:
            # From 11th year onwards, maintain 10th year amount
            current_year_annuity = base_annuity + (base_annuity * 0.10 * 9)
        
        # Add garden_extent * 100,000 only to first annuity
        if year == 1:
            current_year_annuity += gardenextent * 100000
        
        yearly_annuities.append(current_year_annuity)
        total_annuity += current_year_annuity

    # Sum Amount_Received from individual annuity fields
    amount_fields = [
        'FIRST_ANNUITY', 'SECOND_ANNUITY', 'THIRD_ANNUITY', 'FOURTH_ANNUITY',
        'FIFTH_ANNUITY', 'SIXTH_ANNUITY', 'SEVENTH_ANNUITY', 'EIGTH_ANNUITY',
        'NINTH_ANNUITY', 'TENTH_ANNUITY', 'ELEVENTH_ANNUITY', 'TWELFTH_ANNUITY',
        'THIRTEENTH_ANNUITY', 'FOURTEENTH_ANNUITY', 'FIFTEENTH_ANNUITY'
    ]
    Amount_Received = 0.0
    for f in amount_fields:
        try:
            Amount_Received += float(row.get(f) or 0)
        except Exception:
            pass

    # Calculate total from individual annuity columns (as per dbo.final table)
    Total_Annuity_From_Table = Amount_Received
    
    Difference_Amount = total_annuity - Amount_Received

    return {
        'Total_Annuity_Amount': total_annuity,
        'Amount_Received': Amount_Received,
        'Difference_Amount': Difference_Amount,
        'Annuity_Clac': Annuity_Clac,
        'Total_Annuity_From_Table': Total_Annuity_From_Table,
        'Base_Annuity': base_annuity,
        'Yearly_Annuities': yearly_annuities,
        'gardenextent': gardenextent,
    }
