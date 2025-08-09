import io
import os
from datetime import datetime, timedelta, date
from typing import Optional
import logging

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
import re
from flask import Flask, jsonify, render_template, request, make_response
from datetime import timezone, time

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(filename='app.log', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

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


# ---------------------------------------------------------------------------
# Flask setup
# ---------------------------------------------------------------------------
app = Flask(__name__)

# ---------------------------------------------------------------------------
# Excel fetching & caching
# ---------------------------------------------------------------------------
_df_cache: Optional[pd.DataFrame] = None
_last_fetched: Optional[datetime] = None
CACHE_SECONDS = 60  # refresh every minute – adjust as needed

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _df_to_records(df: pd.DataFrame):
    """Convert DataFrame to list of JSON-serializable dicts.
    • All datetime columns → 'YYYY-MM-DD' strings
    • NaN/NaT → None for clean JSON"""
    out = df.copy()
    # Dates
    for col in out.select_dtypes(include=["datetime", "datetimetz", "datetime64[ns]"]).columns:
        out[col] = out[col].dt.strftime("%Y-%m-%d")
    # Times (python datetime.time)
    for col in out.columns:
        if out[col].dtype == "object":
            if out[col].apply(lambda v: isinstance(v, time)).any():
                out[col] = out[col].apply(lambda v: v.strftime("%H:%M:%S") if isinstance(v, time) else v)
    # Replace NaN/NaT with None
    out = out.astype(object).where(pd.notnull(out), None)
    return out.to_dict(orient="records")

def _to_export_url(url: str) -> str:
    """Convert a Google-Sheets share link to its direct XLSX export link.
    If the URL is already an export link or isn't a Google Sheet, it is returned unchanged."""
    if "docs.google.com/spreadsheets" not in url or "/export" in url:
        return url
    # Extract the sheet ID with regex
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", url)
    if m:
        sheet_id = m.group(1)
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    return url


def fetch_excel() -> pd.DataFrame:
    """Download and parse the Excel file, with simple in-memory caching."""
    global _df_cache, _last_fetched
    
    try:
        now = datetime.now(timezone.utc)
        if _df_cache is not None and _last_fetched and (now - _last_fetched).seconds < CACHE_SECONDS:
            return _df_cache

        headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY and API_KEY != "YOUR_API_KEY" else {}
        resolved_url = _to_export_url(EXCEL_URL)
        
        print(f"[DEBUG] Fetching data from: {resolved_url}")
        logging.debug(f"Fetching data from: {resolved_url}")
        
        resp = requests.get(resolved_url, headers=headers, timeout=30)
        resp.raise_for_status()
        logging.debug(f"Successfully fetched data. Status code: {resp.status_code}")

        # Read excel into DataFrame
        with io.BytesIO(resp.content) as bio:
            df = pd.read_excel(bio)
        logging.debug(f"Successfully loaded {len(df)} rows and {len(df.columns)} columns into DataFrame.")

        print(f"Successfully loaded {len(df)} rows and {len(df.columns)} columns")
        
        # Ensure date column is datetime dtype
        if DATE_COLUMN_NAME in df.columns:
            df[DATE_COLUMN_NAME] = pd.to_datetime(df[DATE_COLUMN_NAME], errors='coerce')
            print(f"Date column '{DATE_COLUMN_NAME}' converted to datetime")
        else:
            print(f"Warning: Date column '{DATE_COLUMN_NAME}' not found in data")
            print(f"Available columns: {list(df.columns)}")

        _df_cache = df
        _last_fetched = now
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Excel file: {e}")
        raise
    except Exception as e:
        print(f"Error processing Excel file: {e}")
        raise

# ---------------------------------------------------------------------------
# Helper: filter DataFrame by date range
# ---------------------------------------------------------------------------

def filter_df(df: pd.DataFrame, start: Optional[date], end: Optional[date]):
    """Filter DataFrame by date range"""
    if DATE_COLUMN_NAME not in df.columns or start is None or end is None:
        return df
    
    try:
        # Ensure the date column is datetime type
        if df[DATE_COLUMN_NAME].dtype == 'object':
            df[DATE_COLUMN_NAME] = pd.to_datetime(df[DATE_COLUMN_NAME], errors='coerce')
        
        # Filter by date range
        mask = (df[DATE_COLUMN_NAME].dt.date >= start) & (df[DATE_COLUMN_NAME].dt.date <= end)
        filtered_df = df.loc[mask]
        
        print(f"Date filtering: {len(df)} -> {len(filtered_df)} records")
        return filtered_df
        
    except Exception as e:
        print(f"Error in date filtering: {e}")
        return df


def get_range(filter_type: str):
    today = date.today()

    if filter_type == "today":
        return today, today
    if filter_type == "yesterday":
        y = today - timedelta(days=1)
        return y, y
    if filter_type == "tomorrow":
        t = today + timedelta(days=1)
        return t, t
    if filter_type == "last7":
        return today - timedelta(days=6), today
    if filter_type == "last30":
        return today - timedelta(days=29), today
    if filter_type == "next7":
        n7 = today + timedelta(days=7)
        return today, n7
    if filter_type == "next30":
        n30 = today + timedelta(days=30)
        return today, n30
    if filter_type == "thisweek":
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
        return start, end
    if filter_type == "thismonth":
        start = today.replace(day=1)
        end = (start + relativedelta(months=1)) - timedelta(days=1)
        return start, end
    if filter_type == "thisyear":
        start = date(today.year, 1, 1)
        end = date(today.year, 12, 31)
        return start, end
    return None, None

# ---------------------------------------------------------------------------
# Helper: Calculate OPD and IPD counts
# ---------------------------------------------------------------------------

def calculate_counts(df: pd.DataFrame):
    """Calculate counts for OPD, IPD, Surgery, and Diagnostics based on actual column names."""
    counts = {
        "opd_count": 0,
        "ipd_count": 0,
        "surgery_suggested": 0,
        "diagnostic_suggested": 0,
        "surgery_not_suggested": 0,
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
    status_col_for_surgery_diagnostic = ' Status'
    if status_col_for_surgery_diagnostic in df.columns:
        print(f"[DEBUG] Using column '{status_col_for_surgery_diagnostic}' as status column for surgery/diagnostic")
        print(f"[DEBUG] First 10 raw values: {df[status_col_for_surgery_diagnostic].head(10).tolist()}")
        
        cleaned_status = df[status_col_for_surgery_diagnostic].astype(str).str.strip().str.lower()
        print(f"[DEBUG] First 10 cleaned values: {cleaned_status.head(10).tolist()}")
        print(f"[DEBUG] Unique values in status column: {cleaned_status.unique().tolist()}")
        
        # Filter for 'Surgery Suggested' and 'Diagnostic Suggested' based on the cleaned status column
        df_filtered = df[cleaned_status.isin(['surgery suggested', 'diagnostic suggested'])]

        counts['surgery_suggested'] = int((df_filtered[status_col_for_surgery_diagnostic].astype(str).str.strip().str.lower() == 'surgery suggested').sum())
        counts['diagnostic_suggested'] = int((df_filtered[status_col_for_surgery_diagnostic].astype(str).str.strip().str.lower() == 'diagnostic suggested').sum())
        counts['surgery_not_suggested'] = int((cleaned_status == 'surgery not suggested').sum())
        
        print(f"[DEBUG] Surgery suggested matches: {counts['surgery_suggested']}")
        print(f"[DEBUG] Diagnostic suggested matches: {counts['diagnostic_suggested']}")
        print(f"[DEBUG] Surgery not suggested matches: {counts['surgery_not_suggested']}")
    else:
        print(f"[DEBUG] Warning: Status column '{status_col_for_surgery_diagnostic}' not found for surgery/diagnostic counts.")

    print(f"[DEBUG] Final counts: {counts}")
    return counts

# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.route("/api/data")
def api_data():
    """Get all data with counts"""
    try:
        df = fetch_excel()
        data = _df_to_records(df)
        counts = calculate_counts(df)
        return jsonify({
            "data": data,
            "counts": counts,
            "success": True
        })
    except Exception as e:
        return jsonify({
            "error": str(e),
            "success": False
        }), 500


@app.route("/api/filter")
def api_filter():
    """Filter data by date range and field"""
    try:
        df = fetch_excel()
        out = df.copy()

        # --- Apply filters ---
        filter_type = request.args.get("type", default="", type=str).lower()
        start_str = request.args.get("start")
        end_str = request.args.get("end")
        field = request.args.get("field")
        query = request.args.get("q")

        start_date = None
        end_date = None
        
        if start_str and end_str:
            try:
                start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
            except ValueError:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD.", "success": False}), 400
        elif filter_type:
            start_date, end_date = get_range(filter_type)
        
        if start_date and end_date:
            out = filter_df(out, start_date, end_date)

        if field and query:
            df_columns_lower = {c.lower(): c for c in out.columns}
            # Find the actual column name ignoring case
            actual_field_name = None
            for col_name in out.columns:
                if col_name.lower() == field.lower():
                    actual_field_name = col_name
                    break

            if actual_field_name:
                # Filter by field and query, case-insensitive
                out = out[out[actual_field_name].astype(str).str.lower() == query.lower()]
            else:
                print(f"[DEBUG] Field '{field}' not found in columns for filtering.")

        # --- Calculate counts on the now filtered DataFrame ---
        filtered_counts = calculate_counts(out)

        return jsonify({
            "success": True,
            "counts": filtered_counts,
            "data": _df_to_records(out)
        })

    except Exception as e:
        print(f"[ERROR] in /api/filter: {e}")
        return jsonify({"error": str(e), "success": False}), 500


@app.route("/api/unique/<field_name>")
def api_unique_values(field_name):
    df = fetch_excel()
    keyword = field_name.lower()
    
    known_map = {
        "bd": "BD",
        "city": "City",
        "hospital": "Hospital",
        "state": "State",
    }
    col_to_find = known_map.get(keyword, keyword)

    preferred = None
    for c in df.columns:
        if col_to_find.lower() in c.lower():
            preferred = c
            break

    if not preferred:
        return jsonify({"error": f"Column for '{field_name}' not found"}), 404

    unique_values = sorted(df[preferred].dropna().unique().astype(str))
    return jsonify(unique_values)


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

        print(f"[DEBUG] Columns from /api/debug/columns: {columns}")
        return jsonify({
            "success": True,
            "columns": columns,
            "sample_data": sample_data
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/debug/unique_status_values')
def debug_unique_status_values():
    """Debug endpoint to see unique values in the 'Status' column."""
    try:
        df = fetch_excel()
        if df is None:
            return jsonify({"error": "Failed to fetch or read the Excel file."}), 500
        
        status_col_name = 'Status'
        if status_col_name in df.columns:
            unique_values = df[status_col_name].astype(str).str.strip().str.lower().unique().tolist()
            print(f"[DEBUG] Unique values from /api/debug/unique_status_values: {unique_values}")
            return jsonify({"success": True, "column": status_col_name, "unique_values": unique_values})
        else:
            print(f"[DEBUG] Column '{status_col_name}' not found for /api/debug/unique_status_values.")
            return jsonify({"success": False, "error": f"Column '{status_col_name}' not found."}), 404
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/counts")
def api_counts():
    """Get just the counts without full data"""
    try:
        df = fetch_excel()
        out = df.copy()

        # Apply same filtering logic as /api/filter
        filter_type = request.args.get("type", default="", type=str).lower()
        start_str = request.args.get("start")
        end_str = request.args.get("end")
        field = request.args.get("field")
        query = request.args.get("q")

        start_date = None
        end_date = None
        
        if start_str and end_str:
            try:
                start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
            except ValueError:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        elif filter_type:
            start_date, end_date = get_range(filter_type)
        
        if start_date and end_date:
            out = filter_df(out, start_date, end_date)

        if field and query:
            df_columns_lower = {c.lower(): c for c in out.columns}
            field_lower = field.lower()
            target_column = None

            # Mapping for predefined dropdowns
            known_map = {
                "bd": "BD",
                "city": "City",
                "hospital": "Hospital",
                "state": "State",
            }

            # Priority 1: Check predefined short-names (bd, city, etc.)
            if field_lower in known_map:
                mapped_name = known_map[field_lower].lower()
                # Find a column that contains the mapped name, e.g., 'BD' or 'BD Name'
                for col_lower, col_original in df_columns_lower.items():
                    if mapped_name in col_lower:
                        target_column = col_original
                        break
            # Priority 2: Check for an exact column name match from "Other" filter
            elif field_lower in df_columns_lower:
                target_column = df_columns_lower[field_lower]

            # Apply exact, case-insensitive match if a column was found
            if target_column:
                # If the query is 'all_bd', we skip filtering for the 'bd' field
                if not (field.lower() == 'bd' and query.lower() == 'all_bd'):
                    out = out[out[target_column].astype(str).str.lower() == query.lower()]
                    print(f"Field filtering on '{target_column}' with '{query}': {len(df)} -> {len(out)} records")

        counts = calculate_counts(out)
        counts["success"] = True
        
        return jsonify(counts)
        
    except Exception as e:
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

# ---------------------------------------------------------------------------
# Front-end route
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve the main dashboard page"""
    resp = make_response(render_template("index.html"))
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

# ---------------------------------------------------------------------------
# Health check endpoint
# ---------------------------------------------------------------------------

@app.route("/health")
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "cache_status": "active" if _df_cache is not None else "empty"
    })

# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

# ---------------------------------------------------------------------------
# Run the app
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Starting Flask application...")
    print(f"Excel URL: {EXCEL_URL}")
    print(f"Date Column: {DATE_COLUMN_NAME}")
    print(f"Cache Duration: {CACHE_SECONDS} seconds")
    
    # Test the Excel connection on startup
    try:
        test_df = fetch_excel()
        print(f"[+] Successfully connected to Excel file")
        print(f"[+] Loaded {len(test_df)} rows and {len(test_df.columns)} columns")
    except Exception as e:
        print(f"[-] Failed to connect to Excel file: {e}")
        print("The app will still start, but data endpoints may fail")
    
    app.run(debug=True, host='0.0.0.0', port=8080, use_reloader=False)