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

    ROWWISEEXTENT = float(val('ROWWISEEXTENT') or 0)
    LandType = int(val('LandType') or 0)
    MutationAppNo = val('MutationAppNo')
    MUTATIONJOINTAPPNO = val('MUTATIONJOINTAPPNO')

    def base_rate():
        return 30000 if LandType == 1 else 50000

    Annuity_Clac = 0
    if OWNERTYPE == 'S':
        if Form_914_Agreement_Date and Form_914_Agreement_Date < datetime.date(2015, 2, 1):
            if ROWWISEEXTENT < 1:
                Annuity_Clac = base_rate()
            else:
                Annuity_Clac = ROWWISEEXTENT * base_rate()
        else:
            Annuity_Clac = ROWWISEEXTENT * base_rate()
    elif OWNERTYPE == 'M':
        cond = ((MutationAppNo != 'NULL' and MUTATIONJOINTAPPNO != 'NULL') or
                (MutationAppNo == 'NULL' and MUTATIONJOINTAPPNO == 'NULL'))
        if cond:
            if Form_914_Agreement_Date and Form_914_Agreement_Date < datetime.date(2015, 2, 1):
                if ROWWISEEXTENT < 1:
                    Annuity_Clac = base_rate()
                else:
                    Annuity_Clac = ROWWISEEXTENT * base_rate()
            else:
                Annuity_Clac = ROWWISEEXTENT * base_rate()

    # Compute total annuity over 15 years
    total_annuity = 0.0
    current_annuity = float(Annuity_Clac)
    for year in range(1, 16):
        total_annuity += current_annuity
        if year < 10:
            current_annuity *= 1.10

    # Sum Amount_Received from fields
    amount_fields = [
        'FIRST_ANNUITY', 'SECOND_ANNUITY', 'THIRD_ANNUITY', 'FOURTH_ANNUITY',
        'FIFTH_ANNUITY', 'SIXTH_ANNUITY', 'SEVENTH_ANNUITY', 'EIGTH_ANNUITY',
        'NINTH_ANNUITY', 'TENTH_ANNUITY', 'ELEVENTH_ANNUITY'
    ]
    Amount_Received = 0.0
    for f in amount_fields:
        try:
            Amount_Received += float(row.get(f) or 0)
        except Exception:
            # ignore parse errors
            pass

    Difference_Amount = total_annuity - Amount_Received

    return {
        'Total_Annuity_Amount': total_annuity,
        'Amount_Received': Amount_Received,
        'Difference_Amount': Difference_Amount,
        'Annuity_Clac': Annuity_Clac,
    }
