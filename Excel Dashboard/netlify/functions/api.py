import io
import os
from datetime import datetime, timedelta, date
from typing import Optional
import io

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
        
        print(f"Fetching data from: {resolved_url}")
        
        resp = requests.get(resolved_url, headers=headers, timeout=30)
        resp.raise_for_status()

        # Read excel into DataFrame
        with io.BytesIO(resp.content) as bio:
            df = pd.read_excel(bio)

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
    """Calculate OPD and IPD counts based on 'OPD Completed' and 'IPD Completed' status."""
    opd_count = 0
    ipd_count = 0

    # Find the single column that contains the status information.
    # We'll look for a column named 'OPD&IPD' or 'Status' as a fallback.
    status_col = None
    for col in df.columns:
        col_cleaned = col.strip().lower()
        if col_cleaned == 'opd&ipd':
            status_col = col
            break
    if not status_col:
        for col in df.columns:
            if 'status' in col.strip().lower():
                status_col = col
                break

    # If a status column is found, perform the counting.
    if status_col:
        # Clean the status column for reliable matching: convert to string, trim whitespace, and convert to lowercase.
        cleaned_series = df[status_col].astype(str).str.strip().str.lower()
        
        # Count exact matches for 'opd completed'
        opd_count = int((cleaned_series == 'opd completed').sum())
        
        # Count exact matches for 'ipd completed'
        ipd_count = int((cleaned_series == 'ipd completed').sum())

    return {
        "opd_count": opd_count,
        "ipd_count": ipd_count,
        "total_records": len(df),
        "debug_info": {
            "status_column_used": status_col,
            "unique_values": df[status_col].dropna().unique().tolist() if status_col else []
        }
    }
    if ipd_column and ipd_column in df.columns:
        # Get unique values for debugging
        counts["debug_info"]["ipd_unique_values"] = df[ipd_column].dropna().unique().tolist()
        
        # Try multiple patterns for completion
        ipd_series = df[ipd_column].astype(str).str.lower().str.strip()
        ipd_completed = (
            ipd_series.str.contains('completed|complete|yes|true|done|finished|success', na=False) |
            ipd_series.str.match(r'^1$|^1\.0$', na=False) |
            ipd_series.isin(["1", "1.0", "completed", "complete", "done", "finished", "yes", "true", "success"])
        )
        counts["ipd_count"] = int(ipd_completed.sum())

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
        filter_type = request.args.get("type", default="", type=str).lower()
        start_str = request.args.get("start")
        end_str = request.args.get("end")
        field = request.args.get("field")
        query = request.args.get("q")

        df = fetch_excel()
        out = df.copy()

        # Apply date filtering
        start_date = None
        end_date = None
        
        # Priority 1: Custom start/end dates
        if start_str and end_str:
            try:
                start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
            except ValueError:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        # Priority 2: Predefined filter types (only if no custom dates)
        elif filter_type:
            start_date, end_date = get_range(filter_type)
        
        # Apply date filter if we have valid dates
        if start_date and end_date:
            out = filter_df(out, start_date, end_date)

        # Apply field-based filtering (BD / City / Hospital / State)
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

        # Calculate counts for filtered data
        counts = calculate_counts(out)
        
        response_data = {
            "data": _df_to_records(out),
            "counts": counts,
            "filter_info": {
                "total_records": len(out),
                "start_date": start_date.strftime("%Y-%m-%d") if start_date else None,
                "end_date": end_date.strftime("%Y-%m-%d") if end_date else None,
                "field_filter": f"{field}: {query}" if field and query else None
            },
            "success": True
        }
        return jsonify(response_data)
    
    except Exception as e:
        return jsonify({
            "error": str(e),
            "success": False
        }), 500


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


@app.route("/api/counts")
def api_counts():
    """Get just the counts without full data"""
    try:
        filter_type = request.args.get("type", default="", type=str).lower()
        start_str = request.args.get("start")
        end_str = request.args.get("end")
        field = request.args.get("field")
        query = request.args.get("q")

        df = fetch_excel()
        out = df.copy()

        # Apply same filtering logic as /api/filter
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

import serverless_wsgi

def handler(event, context):
    """
    This function is the entry point for the Netlify serverless function.
    It uses serverless-wsgi to wrap the Flask app.
    """
    return serverless_wsgi.handle_request(app, event, context)
