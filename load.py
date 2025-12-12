from datetime import datetime
import pandas as pd
import sqlite3
import os 
import shutil
import glob

# -----------------------------
# -----------------------------
def db_conn():
    db_path = r"C:\Users\saksh\AppData\Roaming\DBeaverData\workspace6\.metadata\sample-database-sqlite-1\Chinook.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    return cursor, conn

# -----------------------------
# LOAD DATA FUNCTION
# -----------------------------
def load_data(table_name, file_path, file_type=None):
    if not os.path.exists(file_path):
        print(f"File not found → {file_path}")
        return

    cursor, conn = db_conn()

    # Auto-detect file type
    if file_type is None:
        if file_path.endswith('.csv'):
            file_type = 'csv'
        elif file_path.endswith('.xlsx'):
            file_type = 'excel'
        elif file_path.endswith('.json'):
            file_type = 'json'
        elif file_path.endswith('.txt'):
            file_type = 'txt'
        else:
            raise ValueError("Unsupported file type")

    # Read file
    if file_type == 'csv':
        df = pd.read_csv(file_path)
    elif file_type == 'txt':
        df = pd.read_csv(file_path, sep="|")
    elif file_type == 'excel':
        df = pd.read_excel(file_path)
    elif file_type == 'json':
        df = pd.read_json(file_path)

    # Convert datetime columns to string to avoid SQLite issues
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)

    df = df.drop_duplicates()  # Remove duplicates within the file

    # Add system columns
    df["filename"] = os.path.basename(file_path)
    df["user_name"] = "sakshi"
    df["rownumber"] = range(1, len(df) + 1)

    # Prepare SQL with INSERT OR REPLACE (handles UNIQUE/PK duplicates)
    column_names = tuple(df.columns)
    placeholders = ", ".join(["?"] * len(column_names))
    insert_sql = f"""
    INSERT OR REPLACE INTO {table_name} {column_names}
    VALUES ({placeholders})
    """

    # Execute insert
    records = [tuple(x) for x in df.to_numpy()]
    cursor.executemany(insert_sql, records)

    # Update last_updated_at timestamp
    update_sql = f"UPDATE {table_name} SET last_updated_at = CURRENT_TIMESTAMP WHERE filename = ?"
    cursor.execute(update_sql, (os.path.basename(file_path),))

    conn.commit()

    print(f"Loaded → {file_path} → {table_name}")

    # Move file to processed folder
    destination_folder = r"C:\Users\saksh\OneDrive\Desktop\Project\data_folder\process_data"
    os.makedirs(destination_folder, exist_ok=True)
    destination = os.path.join(destination_folder, os.path.basename(file_path))
    shutil.move(file_path, destination)
    print(f"Moved → {file_path} → {destination}")


# -----------------------------
# MAIN FUNCTION
# -----------------------------
if __name__ == "__main__":

    # Recursive file patterns (month-wise folders)
    file_patterns = [
        r"C:\Users\saksh\OneDrive\Desktop\Project\data_folder\unprocess_data\Raw Data\output_by_month\**\crm_customers*.xlsx",
        r"C:\Users\saksh\OneDrive\Desktop\Project\data_folder\unprocess_data\Raw Data\output_by_month\**\marketing_events_*.json",
        r"C:\Users\saksh\OneDrive\Desktop\Project\data_folder\unprocess_data\Raw Data\output_by_month\**\support_tickets_*.txt",
        r"C:\Users\saksh\OneDrive\Desktop\Project\data_folder\unprocess_data\Raw Data\output_by_month\**\ecommerce_orders_*.csv"
    ]

    # Loop through all patterns and files
    for pattern in file_patterns:
        file_list = glob.glob(pattern, recursive=True)

        if not file_list:
            print(f"No files found for pattern: {pattern}")

        for file in file_list:

            # Map file to correct table
            if "crm_customers" in file:
                table = "crm_customer_raw_table"
            elif "marketing_events" in file:
                table = "marketing_event_raw_table"
            elif "support_tickets" in file:
                table = "raw_support_tickets_raw_table"
            elif "ecommerce_orders" in file:
                table = "raw_ecommerce_orders_raw_table"
            else:
                print(f"Unknown file: {file}")
                continue

            try:
                load_data(table, file)
            except Exception as e:
                print(f"Error loading{file}:{e}")


