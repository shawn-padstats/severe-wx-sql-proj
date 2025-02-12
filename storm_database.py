import sqlite3
import csv
import os

class StormDatabase:
    def __init__(self, db_path="storms.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    # -----------------
    # WIND TABLE SETUP
    # -----------------
    def create_table_wind(self):
        create_sql = """
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
        """
        self.cursor.execute(create_sql)
        self.conn.commit()

    # --------------------
    # TORNADO TABLE SETUP
    # --------------------
    def create_table_tornado(self):
        create_sql = """
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
        """
        self.cursor.execute(create_sql)
        self.conn.commit()

    # ------------------
    # HAIL TABLE SETUP
    # ------------------
    def create_table_hail(self):
        """
        Creates a 'hail' table matching the 24 columns in hail_historical_data.csv
        (assuming no trailing comma).
        """
        create_sql = """
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
        """
        self.cursor.execute(create_sql)
        self.conn.commit()

    # -------------------------
    # LOAD CSV -> TABLE METHOD
    # -------------------------
    def load_csv_into_table(self, csv_path, table_name):
        """
        Generic method to bulk-insert rows from a CSV into a given table.
        Make sure the CSV columns match the CREATE TABLE order exactly.
        """
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip header row
            rows = [row for row in reader]
        
        # Decide how many columns we need to insert based on table name
        if table_name == "wind":
            # 33 columns
            insert_sql = f"""
            INSERT INTO {table_name} VALUES (
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?
            )
            """
        elif table_name == "tornado":
            # 27 columns
            insert_sql = f"""
            INSERT INTO {table_name} VALUES (
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?
            )
            """
        elif table_name == "hail":
            # 24 columns
            insert_sql = f"""
            INSERT INTO {table_name} VALUES (
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?
            )
            """
        else:
            raise ValueError(f"Unrecognized table name: {table_name}")

        self.cursor.executemany(insert_sql, rows)
        self.conn.commit()

    # -------------
    # QUERY HELPERS
    # -------------
    def execute_query(self, sql, params=None):
        if params is None:
            params = ()
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()