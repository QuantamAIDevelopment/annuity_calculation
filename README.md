# sipl_annuity_agent

This project provides an agent that reads LPS data from SQL Server, computes annuity totals per AadhaarNumber, and writes results into `dbo.LPS_ANNUITY_SUMMARY`.

How to run

1. Install dependencies (preferably in a virtualenv). On Windows PowerShell:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
```

2. Ensure ODBC Driver 17 for SQL Server is installed and the database `SIPL_3TABLES` is accessible on localhost with Trusted Connection.

3. Run the FastAPI app:

```powershell
python main.py
```

4. POST to the endpoint:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8000/execute
```

Notes and caveats

- The project requires `pyodbc` and a working ODBC driver. If `pyodbc` installation fails, ensure Visual C++ Build Tools are available.
- The code assumes column names in `dbo.LPS_FINAL` match those referenced in the computation (case-sensitive depending on database driver). Adjust mappings if needed.
- Lint/type checks in the editor may show unresolved imports until dependencies are installed in the environment.
