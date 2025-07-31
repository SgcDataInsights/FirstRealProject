# sandy.g.cabanes
# Title: earthquake_etl_standalone.py
# Date: July 14, 2025
# ------------------------------------------------------------
"""
This etl combines extract, transform and load tasks into one without using airflow or docker.
To automate daily runs, we will use Task Scheduler.
Logs are written to the logs folder using this script.
But Task Scheduler logs can also be seen in the Event Viewer.
"""

# earthquake_etl_standalone.py

import os
import pandas as pd
import requests
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys # Added for stdout/stderr redirection

# ----------------------------------
# --- LOAD ENVIRONMENT VARIABLES ---
# ----------------------------------
# This ensures your script can read settings like database credentials from a .env file.
print("Loading environment variables from .env file...")
load_dotenv()
print("Environment variables loaded.")

# ---------------------
# --- CONFIGURATION ---
# ---------------------
# Defines where the data files will be stored.
# DATA_PATH uses a fallback value: it tries to get the path from .env,
# but if not found, it defaults to a 'data' folder within the current script's directory.
# This approach is suitable here as the data folder location is less critical
# than database credentials and provides a convenient default for local file storage.
DATA_PATH = os.path.abspath(os.path.expanduser(
    os.getenv("DATA_PATH", os.path.join(os.path.dirname(__file__), "data"))
))

print(f"Data storage path set to: {DATA_PATH}")
# Ensures the data directory exists before the script tries to save files there.
os.makedirs(DATA_PATH, exist_ok=True)
print(f"Ensured data directory exists: {DATA_PATH}")

# -------------------------------------------------------------------
# Log file configuration: Each run will have its own date-stamped log file
# -------------------------------------------------------------------
LOGS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "logs"))
os.makedirs(LOGS_PATH, exist_ok=True) # Ensure the logs directory exists
# FULL_LOG_PATH and log_pipeline_activity will be removed as we are now creating
# a new timestamped log file for each run, capturing all output.


# -----------------------------
# PostgreSQL connection details.
# -----------------------------
# These are critical and must be defined in the .env file.
# The script will stop and inform us if any of these are missing.
try:
    print("Attempting to load PostgreSQL connection details from environment...")
    POSTGRES_CONN_DETAILS = {
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ["POSTGRES_PORT"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "dbname": os.environ["POSTGRES_DB"],
    }
    print("Main PostgreSQL connection details loaded.")

    # PostgreSQL connection details for administrative tasks (e.g., creating databases).
    # This also strictly requires its value from the .env file.
    POSTGRES_ADMIN_CONN_DETAILS = {
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ["POSTGRES_PORT"],
        "user": os.environ["POSTGRES_USER"], # Assumes the same user has administrative privileges
        "password": os.environ["POSTGRES_PASSWORD"], # Assumes the same password
        "dbname": os.environ["POSTGRES_ADMIN_DB"], # This must now be explicitly set in your .env
    }
    print("PostgreSQL admin connection details loaded.")
except KeyError as e:
    print(f"Error: Missing essential environment variable for PostgreSQL connection: {e}")
    print("Please ensure your .env file contains POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, and POSTGRES_ADMIN_DB.")
    sys.exit(1) # Use sys.exit to ensure proper exit behavior


# -------------------------
# --- SQL TABLE SCHEMAS ---
# -------------------------
# SQL statement to create the 'earthquake' table if it doesn't exist.
# This schema is the same as the previous transform python code project.
CREATE_EARTHQUAKE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.earthquake (
    time VARCHAR(100),
    place VARCHAR(100),
    magnitude NUMERIC(32, 8),
    longitude NUMERIC(32, 8),
    latitude NUMERIC(32, 8),
    depth NUMERIC(32, 8),
    file_name VARCHAR(100)
);
"""

# SQL to add the file_name column if it doesn't exist.
# This ensures that even if the table was created without it previously, it gets added.
ADD_FILENAME_COLUMN_SQL = """
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'earthquake' AND column_name = 'file_name') THEN
        ALTER TABLE public.earthquake ADD COLUMN file_name VARCHAR(100);
    END IF;
END $$;
"""

# SQL statement to create the 'stage_earthquake' table if it doesn't exist.
# This schema is from the transformation logic.
CREATE_STAGE_EARTHQUAKE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.stage_earthquake (
    ts TIMESTAMP,
    dt DATE,
    place VARCHAR(255),
    magnitude NUMERIC(32, 8),
    latitude NUMERIC(32, 8),
    longitude NUMERIC(32, 8)
);
"""


# -------------------------
# --- UTILITY FUNCTIONS ---
# -------------------------
# Functions to establish connections to our PostgreSQL database.


# ----------------------------
# Postgres connection for data
# ----------------------------
def get_postgres_conn():
    """Establishes a connection to the earthquake data PostgreSQL database."""
    print(f"Attempting to connect to PostgreSQL database: {POSTGRES_CONN_DETAILS['dbname']}")
    return psycopg2.connect(**POSTGRES_CONN_DETAILS)


# -----------------------------
# Postgres connection for admin
# -----------------------------
def get_postgres_admin_conn():
    """Establishes an administrative connection to PostgreSQL (e.g., to create databases)."""
    print(f"Attempting to connect to PostgreSQL admin database: {POSTGRES_ADMIN_CONN_DETAILS['dbname']}")
    return psycopg2.connect(**POSTGRES_ADMIN_CONN_DETAILS)



# ################################
# --- CORE ETL LOGIC FUNCTIONS ---
# ################################


def setup_database_and_tables():
    """
    Ensures the target PostgreSQL database exists, and then creates the
    'earthquake' and 'stage_earthquake' tables within it if they don't exist.
    It also ensures the 'file_name' column exists in 'public.earthquake'.
    """
    print("Starting database and table setup activity...")
    admin_conn = None
    main_conn = None
    admin_cur = None
    main_cur = None
    try:
        print("Database and table setup activity initiated.")
        # Step 1: Check/Create the main database (earthquake_data)
        admin_conn = get_postgres_admin_conn()
        admin_conn.autocommit = True
        admin_cur = admin_conn.cursor()
        admin_cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (POSTGRES_CONN_DETAILS["dbname"],))
        if admin_cur.fetchone():
            print(f"Database '{POSTGRES_CONN_DETAILS['dbname']}' already exists. Skipping database creation.")
        else:
            admin_cur.execute(f"CREATE DATABASE {POSTGRES_CONN_DETAILS['dbname']}")
            print(f"Database '{POSTGRES_CONN_DETAILS['dbname']}' created successfully.")
        admin_cur.close()
        admin_conn.close()
        print("Database check/creation completed.")

        # Step 2: Connect to the main database and create tables if they don't exist
        print(f"Connecting to '{POSTGRES_CONN_DETAILS['dbname']}' to set up tables...")
        main_conn = get_postgres_conn()
        main_cur = main_conn.cursor()

        # Create earthquake table
        print("Checking for and creating 'public.earthquake' table...")
        main_cur.execute(CREATE_EARTHQUAKE_TABLE_SQL)
        print("'public.earthquake' table setup complete.")

        # Add file_name column if it doesn't exist
        print("Ensuring 'file_name' column exists in 'public.earthquake' table...")
        main_cur.execute(ADD_FILENAME_COLUMN_SQL)
        print("'file_name' column check/addition complete.")

        # Create stage_earthquake table
        print("Checking for and creating 'public.stage_earthquake' table...")
        main_cur.execute(CREATE_STAGE_EARTHQUAKE_TABLE_SQL)
        print("'public.stage_earthquake' table setup complete.")

        main_conn.commit() # Commit table creations and alterations
        print("All necessary tables and columns ensured.")

    except Exception as e:
        print(f"[ERROR] Failed during database or table setup: {e}")
        if admin_conn:
            admin_conn.rollback()
        if main_conn:
            main_conn.rollback()
        raise # Re-raise the exception to stop the activity if setup fails
    finally:
        if admin_cur:
            admin_cur.close()
        if admin_conn:
            admin_conn.close()
        if main_cur:
            main_cur.close()
        if main_conn:
            main_conn.close()
    print("Database and table setup activity completed.")


def fetch_data_to_local_csv(execution_date_str: str) -> str:
    """
    Fetches earthquake data from USGS for a given date and saves it to a local CSV file.
    Args:
        execution_date_str (str): The date for which to fetch data (format YYYY-MM-DD).
    Returns:
        str: The full path to the saved CSV file, or an empty string if no data was found.
    """
    print(f"Starting data fetch for date: {execution_date_str}...")
    start_time = execution_date_str
    # Calculates the end time as the day after the start time for the USGS query
    end_time = (datetime.strptime(execution_date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_time}&endtime={end_time}"
    print(f"Fetching data from URL: {url}")
    response = requests.get(url)
    response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
    print("Data successfully retrieved from USGS.")

    features = response.json().get("features", [])
    if not features:
        print(f"[INFO] No earthquake data found for {execution_date_str}. No CSV will be generated.")
        return ""

    # Constructs the filename and full path for the CSV
    filename = f"{execution_date_str.replace('-', '')}_earthquakedata.csv"
    full_path = os.path.join(DATA_PATH, filename)
    print(f"Preparing to save data to CSV: {full_path}")

    earthquakes = []
    for feature in features:
        properties = feature["properties"]
        geometry = feature["geometry"]
        earthquakes.append({
            "time": properties.get("time"),
            "place": properties.get("place"),
            "magnitude": properties.get("mag"),
            "longitude": geometry["coordinates"][0],
            "latitude": geometry["coordinates"][1],
            "depth": geometry["coordinates"][2],
            "file_name": filename, # Tracks which file the data came from
        })

    pd.DataFrame(earthquakes).to_csv(full_path, index=False)
    print(f"[INFO] CSV data saved to: {full_path}")
    print("Data fetch activity completed.")
    return full_path


def load_csv_to_postgres(csv_path: str):
    """
    Loads data from a specified CSV file into the 'public.earthquake' table in PostgreSQL.
    Args:
        csv_path (str): The full path to the CSV file to load.
    """
    print(f"Starting data load from CSV to PostgreSQL: {csv_path}...")
    if not csv_path:
        print("[WARN] No CSV path provided. Skipping data load to PostgreSQL.")
        return

    filename_only = os.path.basename(csv_path)
    conn = None
    cur = None
    try:
        conn = get_postgres_conn()
        cur = conn.cursor()
        # Deletes existing data for the same file to prevent duplicates on re-runs
        print(f"Deleting existing data for file '{filename_only}' in public.earthquake table...")
        cur.execute("DELETE FROM public.earthquake WHERE file_name=%s", (filename_only,))
        # Uses psycopg2's copy_expert for efficient bulk loading
        print(f"Loading data from '{csv_path}' into public.earthquake table...")
        with open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert("COPY public.earthquake FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')", f)
        conn.commit() # Commits the transaction
        print(f"[INFO] Data from '{filename_only}' successfully loaded into public.earthquake table.")
    except errors.UndefinedTable:
        print("[ERROR] PostgreSQL table 'public.earthquake' not found. Please ensure it exists with the correct schema.")
        if conn:
            conn.rollback() # Rollback in case of error
        raise
    except Exception as e:
        print(f"[ERROR] Failed to load CSV data to PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    print("Data load to PostgreSQL completed.")


def transform_in_postgres(p_date: str):
    """
    Transforms raw earthquake data from 'public.earthquake' into 'public.stage_earthquake'
    for a specific date.
    Args:
        p_date (str): The date for which to transform data (format YYYY-MM-DD).
    """
    print(f"Starting data transformation in PostgreSQL for date: {p_date}...")
    conn = None
    cur = None
    try:
        conn = get_postgres_conn()
        cur = conn.cursor()
        # Deletes existing transformed data for the specific date to ensure idempotence
        print(f"Deleting existing transformed data for date '{p_date}' in public.stage_earthquake table...")
        cur.execute("DELETE FROM public.stage_earthquake WHERE dt = %s", (p_date,))
        # Inserts transformed data into the staging table
        print(f"Transforming and inserting data for date '{p_date}' into public.stage_earthquake table...")
        cur.execute("""
            INSERT INTO public.stage_earthquake
            SELECT
                to_timestamp(CAST(time AS BIGINT)/1000) AS ts,
                to_timestamp(CAST(time AS BIGINT)/1000)::date AS dt,
                TRIM(SUBSTRING(place FROM POSITION(' of ' IN place))) AS place,
                magnitude,
                latitude,
                longitude
            FROM public.earthquake
            WHERE to_timestamp(CAST(time AS BIGINT)/1000)::date = %s;
        """, (p_date,))
        conn.commit()
        print(f"[INFO] Data transformation successful for date: {p_date}")
    except errors.UndefinedTable:
        print("[ERROR] PostgreSQL table 'public.stage_earthquake' not found. Please ensure it exists with the correct schema.")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        print(f"[ERROR] Failed to transform data in PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    print("Data transformation activity completed.")


# ############################
# --- MAIN EXECUTION BLOCK ---
# ############################


# This block runs when the script is executed directly.
if __name__ == "__main__":
    # Store original stdout and stderr
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    log_file_handle = None # Initialize log file handle

    try:
        # Get current datetime for log file naming and logging
        current_datetime = datetime.now()
        # Use yesterday's date for ETL
        etl_date = current_datetime - timedelta(days=1)
        current_date_for_etl = etl_date.strftime("%Y-%m-%d")

        timestamp_for_log_file = current_datetime.strftime("%Y%m%d_%H%M%S")

        # Define the path for the daily, timestamped log file
        DAILY_LOG_FILE_PATH = os.path.join(LOGS_PATH, f"etl_run_{timestamp_for_log_file}.log")

        # Open the daily log file in write mode ('w') for a fresh log each run
        # All subsequent print statements and errors will go into this file.
        log_file_handle = open(DAILY_LOG_FILE_PATH, "w", encoding="utf-8")
        sys.stdout = log_file_handle
        sys.stderr = log_file_handle

        print(f"\n--- ETL Activity Start: {current_datetime.strftime('%Y-%m-%d %H:%M:%S')} ---")
        print(f"--- Starting earthquake data ETL for {current_date_for_etl} ---")

        # Step 1: Ensure the database and necessary tables exist
        print("\n--- Step 1: Setting up Database and Tables ---")
        setup_database_and_tables()

        # Step 2: Fetch data and save it to a CSV file
        print("\n--- Step 2: Fetching Data to Local CSV ---")
        generated_csv_path = fetch_data_to_local_csv(execution_date_str=current_date_for_etl)

        if generated_csv_path:
            # Step 3: Load the CSV data into the raw PostgreSQL table
            print("\n--- Step 3: Loading CSV Data to PostgreSQL ---")
            load_csv_to_postgres(generated_csv_path)
            # Step 4: Transform the raw data into the staged table
            print("\n--- Step 4: Transforming Data in PostgreSQL ---")
            transform_in_postgres(p_date=current_date_for_etl)
        else:
            print(f"\nNo earthquake data was fetched for {current_date_for_etl}. Skipping load and transform steps.")

        print(f"\n--- ETL activity completed for {current_date_for_etl} ---")
        print(f"--- ETL Activity End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Status: SUCCESS ---")

    except Exception as e:
        # Catches any unhandled exceptions during the ETL activity
        print(f"\n--- ETL activity failed for {current_date_for_etl} due to an error: {e} ---")
        print(f"--- ETL Activity End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Status: FAILED ---")
        # Re-raise the exception after logging if desired, or just log and exit
        # raise # Uncomment to re-raise the exception and potentially show it in Task Scheduler history

    finally:
        # Restore original stdout and stderr
        if log_file_handle:
            log_file_handle.close()
        sys.stdout = original_stdout
        sys.stderr = original_stderr

        # This print will go to the original console/output, if the script is run directly.
        # When run by Task Scheduler, its output is typically captured by Event Viewer or discarded,
        # but the detailed log is now in the file.
        print(f"ETL process finished. Detailed logs for this run are in: {DAILY_LOG_FILE_PATH}")