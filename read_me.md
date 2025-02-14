# Severe Weather Data Explorer (SQL Edition)

This project uses a **SQLite** database to store severe weather data (wind, tornado, hail) and provides a **Streamlit** web interface for querying. We load CSV files into a local database file (`storms.db`), then run queries via specialized classes (`WindSQL`, `TornadoSQL`, `HailSQL`).

---

## Project Structure

1. **main.py**  
   - The primary Streamlit script that orchestrates:
     - Database creation/connection (`StormDatabase` in `storm_database.py`)
     - Loading CSVs into the database
     - Querying via `WindSQL`, `TornadoSQL`, `HailSQL`
     - Displaying results in Streamlit

2. **storm_database.py**  
   - Defines `StormDatabase`, a helper class that creates `storms.db` (a SQLite file) and loads CSV data into tables (`wind`, `tornado`, `hail`).

3. **wind_sql.py**, **tornado_sql.py**, **hail_sql.py**  
   - Each file has a class (`WindSQL`, `TornadoSQL`, `HailSQL`) with **SQL-based** queries. For example:
     - `wind_sql.py` has methods to count wind gusts, get top property damage events, etc.

4. **CSV Files**  
   - `wind_historical_data.csv`  
   - `tor_historical_data.csv`  
   - `hail_historical_data.csv`  
   These are loaded into the database when `main.py` runs.

---

## Installation

1. **Python 3.7+** recommended.
2. Install required libraries (e.g., `streamlit`, `pandas`, `sqlite3` is built into Python):
   ```bash
   pip install streamlit pandas
(sqlite3 comes with the standard Python library, so no extra install needed.)

How to Run
Ensure all files (main.py, storm_database.py, etc.) are in the same folder.
Place the CSV files in the same folder, named exactly:
wind_historical_data.csv
tor_historical_data.csv
hail_historical_data.csv
In a terminal, navigate to that folder and run:
bash
Copy
Edit
streamlit run main.py
This will open a browser window at localhost:8501.
Using the App
Pick a Dataset: Choose Wind, Tornado, or Hail from the radio buttons.
Select a Query: In the dropdown, pick the SQL-based query you want to run (e.g., “Count wind gusts >= X between dates” or “Top-N property damage”).
Set Parameters: For instance, enter the minimum knots or the date range.
Run Query: Press the “Run Query” button to execute the SQL and get results from storms.db.
View Results: Streamlit will display the query output directly (counts, lists of rows, etc.).
Typically, you’ll see something like:
“Found X wind events >= 80 knots,” or
A table of the top N rows by property damage.
Close DB (Optional): There’s a button at the bottom to close the database connection.
Troubleshooting
Missing DB File: If storms.db was not created, confirm the CSV filenames are correct and in the same folder.
Install: Make sure streamlit and pandas are installed in your environment (pip list).
Date Format: The CSVs are typically MM/DD/YYYY; storm_database.py calls convert_to_iso_date to standardize them to YYYY-MM-DD. If you have different date formats, adapt accordingly.
Edits: If you change any table definitions or columns, also update TABLE_DEFINITIONS in storm_database.py.
Enjoy exploring severe weather data with the SQL-based version of this project!

vbnet
Copy
Edit

**How This README Maps to the Original SQL Code**

1. **`storm_database.py`** references `StormDatabase`, which physically creates or recreates `storms.db` and loads data from CSV.  
2. **`wind_sql.py`, `tornado_sql.py`, `hail_sql.py`** each contain methods that run **SQL queries** (like `SELECT * FROM wind ...`) to get counts, break them down by month/year, etc.  
3. **`main.py`** sets up the database, loads the CSVs, then uses the classes to run queries based on user input in Streamlit.  
4. The rest is standard Streamlit usage (radio buttons, text input, etc.) to build the user-facing UI.