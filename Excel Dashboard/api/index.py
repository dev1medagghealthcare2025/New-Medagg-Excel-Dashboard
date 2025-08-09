import io
import os
from datetime import datetime, timedelta, date
from typing import Optional

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
import re
from flask import Flask, jsonify, render_template, request, make_response
from datetime import timezone, time

# Add error handling for missing dependencies
try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    print("Warning: dateutil not installed. Install with: pip install python-dateutil")
    
try:
    import pandas as pd
except ImportError:
    print("Warning: pandas not installed. Install with: pip install pandas")
    
try:
    import requests
except ImportError:
    print("Warning: requests not installed. Install with: pip install requests")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
EXCEL_URL = os.getenv("EXCEL_URL", "https://docs.google.com/spreadsheets/d/1JJFy0SjFGoJuYPSjwg-toCXbptr9v1DeiglHtv7hoAU/edit?usp=sharing")
API_KEY = os.getenv("EXCEL_API_KEY", "YOUR_API_KEY")  # Ideally override via env var
DATE_COLUMN_NAME = os.getenv("DATE_COLUMN", "Date")  # Column containing dates

app = Flask(__name__, template_folder='../templates', static_folder='../static')

# In-memory cache for the DataFrame
_df_cache = None
_df_last_fetched = None
CACHE_DURATION = timedelta(minutes=5)

# ---------------------------------------------------------------------------
# Helper: DataFrame conversion
# ---------------------------------------------------------------------------

def _df_to_records(df: pd.DataFrame):
    """Convert DataFrame to list of JSON-serializable dicts."""
    if df.empty:
        return []
    df_copy = df.copy()
    for col in df_copy.select_dtypes(include=['datetime64[ns]']).columns:
        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
    return df_copy.replace({pd.NaT: None, pd.NA: None}).to_dict(orient='records')

# ---------------------------------------------------------------------------
# Helper: Google Sheets URL to direct export link
# ---------------------------------------------------------------------------

def _to_export_url(url: str):
    """Convert a Google-Sheets share link to its direct XLSX export link."""
    if 'google.com/spreadsheets/d/' in url and '/edit' in url:
        return url.replace('/edit', '/export?format=xlsx')
    return url

# ---------------------------------------------------------------------------
# Helper: Fetch and cache Excel file
# ---------------------------------------------------------------------------

def fetch_excel():
    """Download and parse the Excel file, with simple in-memory caching."""
    global _df_cache, _df_last_fetched
    if _df_cache is not None and _df_last_fetched and (datetime.now() - _df_last_fetched < CACHE_DURATION):
        return _df_cache.copy()
    sheet_url = os.environ.get('EXCEL_SHEET_URL')
    if not sheet_url:
        raise ValueError("EXCEL_SHEET_URL environment variable not set.")
    export_url = _to_export_url(sheet_url)
    response = requests.get(export_url, timeout=30)
    response.raise_for_status()
    with io.BytesIO(response.content) as f:
        df = pd.read_excel(f, engine='openpyxl')
    _df_cache = df
    _df_last_fetched = datetime.now()
    return df.copy()

# ---------------------------------------------------------------------------
# Helper: filter DataFrame by date range
# ---------------------------------------------------------------------------

def filter_df(df: pd.DataFrame, start: Optional[date], end: Optional[date]):
    """Filter DataFrame by date range"""
    if not start or not end:
        return df
    date_col = next((c for c in df.columns if 'date' in c.lower()), None)
    if not date_col:
        return df
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    except Exception:
        return df
    mask = (df[date_col].dt.date >= start) & (df[date_col].dt.date <= end)
    return df.loc[mask]

def get_range(filter_type: str):
    """Get start and end date for a given filter type"""
    today = date.today()
    if filter_type == 'today': return today, today
    if filter_type == 'yesterday':
        yesterday = today - timedelta(days=1)
        return yesterday, yesterday
    if filter_type == 'last_7_days': return today - timedelta(days=6), today
    if filter_type == 'last_30_days': return today - timedelta(days=29), today
    if filter_type == 'this_month': return date(today.year, today.month, 1), today
    if filter_type == 'this_year': return date(today.year, 1, 1), date(today.year, 12, 31)
    return None, None

# ---------------------------------------------------------------------------
# Helper: Calculate counts
# ---------------------------------------------------------------------------

def calculate_counts(df: pd.DataFrame):
    """Calculate counts for OPD, IPD, Surgery, and Diagnostics based on actual column names."""
    counts = {
        "opd_count": 0,
        "ipd_count": 0,
        "surgery_suggested": 0,
        "diagnostic_suggested": 0,
        "total_records": len(df)
    }

    # --- Use the correct column name 'OPD&IPD' for status ---
    status_col_name = 'OPD&IPD'
    if status_col_name in df.columns:
        # Clean the data for reliable matching (lowercase, strip whitespace)
        cleaned_series = df[status_col_name].astype(str).str.strip().str.lower()
        
        # Count occurrences of 'opd completed' and 'ipd completed'
        counts['opd_count'] = int((cleaned_series == 'opd completed').sum())
        counts['ipd_count'] = int((cleaned_series == 'ipd completed').sum())
        print(f"[DEBUG] Counts from '{status_col_name}' column: OPD={counts['opd_count']}, IPD={counts['ipd_count']}")
    else:
        print(f"[DEBUG] Warning: Status column '{status_col_name}' not found.")

    # --- Check for 'IPD Status' as a fallback for IPD count ---
    ipd_status_col = 'IPD Status'
    if ipd_status_col in df.columns and counts['ipd_count'] == 0:
        cleaned_ipd_status = df[ipd_status_col].astype(str).str.strip().str.lower()
        ipd_completed_count = int((cleaned_ipd_status == 'completed').sum())
        if ipd_completed_count > 0:
            counts['ipd_count'] = ipd_completed_count
            print(f"[DEBUG] Used fallback '{ipd_status_col}'. Found {ipd_completed_count} completed IPDs.")

    # --- NEW LOGIC for Surgery/Diagnostic Suggested based on 'Status' column ---
    # Always use the first column as Status, since your data puts status there
    status_col = df.columns[0]
    print(f"[DEBUG] Using column '{status_col}' as status column")
    print(f"[DEBUG] First 10 raw values: {df[status_col].head(10).tolist()}")
    
    cleaned_status = df[status_col].astype(str).str.strip().str.lower()
    print(f"[DEBUG] First 10 cleaned values: {cleaned_status.head(10).tolist()}")
    print(f"[DEBUG] Unique values in status column: {cleaned_status.unique()[:20]}")
    
    surgery_count = int((cleaned_status == 'surgery suggested').sum())
    diagnostic_count = int((cleaned_status == 'diagnostic suggested').sum())
    
    print(f"[DEBUG] Surgery suggested matches: {surgery_count}")
    print(f"[DEBUG] Diagnostic suggested matches: {diagnostic_count}")
    
    counts['surgery_suggested'] = surgery_count
    counts['diagnostic_suggested'] = diagnostic_count

    return counts

# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.route("/api/data")
def api_data():
    try:
        df = fetch_excel()
        counts = calculate_counts(df)
        return jsonify({"data": _df_to_records(df), "counts": counts, "success": True})
    except Exception as e:
        return jsonify({"error": str(e), "success": False}), 500

@app.route("/api/filter")
def api_filter():
    """Filter data by date range and field"""
    try:
        df = fetch_excel()
        if df is None:
            return jsonify({"error": "Could not fetch Excel data.", "success": False}), 500

        # --- Calculate total counts on the original DataFrame ---
        total_counts = calculate_counts(df)

        # --- Apply Filters ---
        out = df.copy()
        filter_type = request.args.get('type')
        start_date_str = request.args.get('start')
        end_date_str = request.args.get('end')
        field = request.args.get('field')
        query = request.args.get('q')

        # Date filtering
        start_date, end_date = None, None
        if filter_type == 'custom' and start_date_str and end_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        elif filter_type and filter_type != 'all':
            start_date, end_date = get_range(filter_type)
        
        if start_date and end_date:
            out = filter_df(out, start_date, end_date)

        # Field filtering
        if field and query and query.lower() != 'all_bd':
            target_column = next((col for col in out.columns if field.lower() in col.lower()), None)
            if target_column:
                out = out[out[target_column].astype(str).str.lower() == query.lower()]

        # --- Calculate counts on the filtered DataFrame ---
        filtered_counts = calculate_counts(out)

        return jsonify({
            "success": True,
            "total_counts": total_counts,      # For main summary cards
            "counts": filtered_counts,         # For the filtered results summary
            "data": _df_to_records(out)
        })

    except Exception as e:
        print(f"[ERROR] in /api/filter: {e}")
        return jsonify({"error": str(e), "success": False}), 500

@app.route('/api/debug/columns')
def debug_columns():
    """Debug endpoint to see column names and sample data from Excel."""
    try:
        df = fetch_excel()
        if df is None:
            return jsonify({"error": "Failed to fetch or read the Excel file."}), 500
        
        # Get column names and some sample data
        columns = df.columns.tolist()
        sample_data = _df_to_records(df.head())

        return jsonify({
            "success": True,
            "columns": columns,
            "sample_data": sample_data
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/counts")
def api_counts():
    try:
        df = fetch_excel()
        counts = calculate_counts(df)
        return jsonify(counts)
    except Exception as e:
        return jsonify({"error": str(e), "success": False}), 500

# ---------------------------------------------------------------------------
# Front-end & Error Handlers
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not Found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal Server Error"}), 500
