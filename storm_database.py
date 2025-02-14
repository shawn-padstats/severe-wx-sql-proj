import os
import sqlite3
import csv
from datetime import datetime

def convert_to_iso_date(date_str):
    """
    Convert 'MM/DD/YYYY' to 'YYYY-MM-DD'. If invalid/blank, returns None.
    """
    if not date_str or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


TABLE_DEFINITIONS = {
    "wind": {
        "create_sql": """
            CREATE TABLE IF NOT EXISTS wind (
                DATE TEXT,
                CountyName TEXT,
                [MAGNITUDE (Knots)] REAL,
                [Converted to MPH] REAL,
                BEGIN_LOCATION TEXT,
                BEGIN_TIME TEXT,
                DEATHS_DIRECT INTEGER,
                INJURIES_DIRECT INTEGER,
                DAMAGE_PROPERTY_NUM REAL,
                DAMAGE_CROPS_NUM REAL,
                CZ_TIMEZONE TEXT,
                MAGNITUDE_TYPE TEXT,
                EPISODE_ID TEXT,
                CZ_TYPE TEXT,
                CZ_FIPS TEXT,
                WFO TEXT,
                INJURIES_INDIRECT INTEGER,
                DEATHS_INDIRECT INTEGER,
                SOURCE TEXT,
                FLOOD_CAUSE TEXT,
                TOR_LENGTH REAL,
                TOR_WIDTH REAL,
                BEGIN_RANGE REAL,
                BEGIN_AZIMUTH TEXT,
                END_RANGE REAL,
                END_AZIMUTH TEXT,
                END_LOCATION TEXT,
                END_TIME TEXT,
                BEGIN_LAT REAL,
                BEGIN_LON REAL,
                END_LAT REAL,
                END_LON REAL,
                EVENT_NARRATIVE TEXT,
                EPISODE_NARRATIVE TEXT
            );
        """,
        "num_columns": 34,
        "date_columns": [0],  # Only 'DATE' at index 0 needs converting
        "insert_sql": """
            INSERT INTO wind VALUES (
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?
            )
        """
    },
    "tornado": {
        "create_sql": """
            CREATE TABLE IF NOT EXISTS tornado (
                DATE TEXT,
                CountyName TEXT,
                TOR_F_SCALE TEXT,
                BEGIN_LAT REAL,
                BEGIN_LON REAL,
                END_LAT REAL,
                END_LON REAL,
                BEGIN_TIME TEXT,
                DEATHS_DIRECT INTEGER,
                INJURIES_DIRECT INTEGER,
                DAMAGE_PROPERTY_NUM REAL,
                DAMAGE_CROPS_NUM REAL,
                CZ_TIMEZONE TEXT,
                SOURCE TEXT,
                TOR_LENGTH REAL,
                TOR_WIDTH REAL,
                BEGIN_RANGE INTEGER,
                BEGIN_AZIMUTH TEXT,
                END_RANGE INTEGER,
                END_AZIMUTH TEXT,
                BEGIN_LOCATION TEXT,
                END_LOCATION TEXT,
                END_DATE TEXT,
                END_TIME TEXT,
                EVENT_NARRATIVE TEXT,
                EPISODE_NARRATIVE TEXT,
                ABSOLUTE_ROWNUMBER INTEGER
            );
        """,
        "num_columns": 27,
        "date_columns": [0, 22],  # 'DATE' = index 0, 'END_DATE' = index 22
        "insert_sql": """
            INSERT INTO tornado VALUES (
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?
            )
        """
    },
    "hail": {
        "create_sql": """
            CREATE TABLE IF NOT EXISTS hail (
                DATE TEXT,
                CountyName TEXT,
                [HAIL SIZE (INCHES)] REAL,
                BEGIN_LOCATION TEXT,
                BEGIN_TIME TEXT,
                INJURIES_DIRECT INTEGER,
                DAMAGE_PROPERTY_NUM REAL,
                DAMAGE_CROPS_NUM REAL,
                CZ_TIMEZONE TEXT,
                MAGNITUDE_TYPE TEXT,
                CZ_FIPS TEXT,
                WFO TEXT,
                SOURCE TEXT,
                FLOOD_CAUSE TEXT,
                TOR_LENGTH REAL,
                TOR_WIDTH REAL,
                BEGIN_RANGE REAL,
                BEGIN_AZIMUTH TEXT,
                END_RANGE REAL,
                END_AZIMUTH TEXT,
                END_LOCATION TEXT,
                END_TIME TEXT,
                BEGIN_LAT REAL,
                BEGIN_LON REAL
            );
        """,
        "num_columns": 24,
        "date_columns": [0],  # 'DATE' = index 0
        "insert_sql": """
            INSERT INTO hail VALUES (
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?
            )
        """
    }
}


class StormDatabase:
    def __init__(self, db_path="storms.db", recreate=True):
        """
        Initialize a new StormDatabase connection.
        
        Args:
            db_path (str): Path to the SQLite database file. Defaults to 'storms.db'.
            recreate (bool): If True, deletes existing database to start fresh. Defaults to True.
        """
        if recreate and os.path.exists(db_path):
            os.remove(db_path)

        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name):
        """
        Create a new table in the database based on predefined schema.
        
        Args:
            table_name (str): Name of the table to create ('wind', 'tornado', or 'hail').
            
        Note:
            Table schema is pulled from TABLE_DEFINITIONS dictionary.
        """
        table_def = TABLE_DEFINITIONS[table_name]
        self.cursor.execute(table_def["create_sql"])
        self.conn.commit()

    def load_csv_into_table(self, csv_path, table_name):
        """
        Load data from a CSV file into the specified database table.
        
        Args:
            csv_path (str): Path to the CSV file to load.
            table_name (str): Name of the target table ('wind', 'tornado', or 'hail').
            
        Raises:
            ValueError: If a row's column count doesn't match the expected schema.
            
        Note:
            - Skips the CSV header row
            - Converts date fields from MM/DD/YYYY to YYYY-MM-DD format
            - Performs batch insert for better performance
        """
        table_def = TABLE_DEFINITIONS[table_name]
        num_cols = table_def["num_columns"]
        date_columns = table_def["date_columns"]
        insert_sql = table_def["insert_sql"]

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)  # skip CSV header

            to_insert = []
            for row_num, row in enumerate(reader, start=2):
                # Check column count
                if len(row) != num_cols:
                    raise ValueError(
                        f"{table_name} row {row_num} in {csv_path} has {len(row)} cols, expected {num_cols}. Row = {row}"
                    )

                # Convert date columns
                for dc in date_columns:
                    row[dc] = convert_to_iso_date(row[dc])

                to_insert.append(row)

        self.cursor.executemany(insert_sql, to_insert)
        self.conn.commit()

    def execute_query(self, sql, params=None):
        """
        Execute a SQL query and return results.
        
        Args:
            sql (str): SQL query to execute.
            params (tuple, optional): Parameters to bind to the query. Defaults to None.
            
        Returns:
            list: List of tuples containing query results.
        """
        if params is None:
            params = ()
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def close(self):
        """
        Close the database connection.
        Should be called when finished with the database to free resources.
        """
        self.conn.close()