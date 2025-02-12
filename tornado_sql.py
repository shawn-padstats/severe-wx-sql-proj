from storm_database import StormDatabase

class TornadoSQL:
    """
    Provides query methods for the 'tornado' table in the storms database.
    Based on tor_data_dictionary.csv with columns such as:
      DATE, County Name, TOR_F_SCALE, BEGIN_LAT, BEGIN_LON, END_LAT, END_LON,
      BEGIN_TIME, DEATHS_DIRECT, INJURIES_DIRECT, DAMAGE_PROPERTY_NUM, ...
    """
    def __init__(self, db):
        self.db = db
        self.table = "tornado"

    # 1) Count EF tornadoes (exact match or >= rating if you prefer).
    def count_ef_tornadoes_exact(self, ef_rating, start_date, end_date):
        """
        Return how many tornadoes have an exact EF scale = ef_rating
        in the specified date range.

        ef_rating might be 'EF0', 'EF1', 'EF2', etc.
        start_date/end_date are 'MM/DD/YYYY' (assuming consistent zero-padding).
        """
        sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE TOR_F_SCALE = ?
          AND DATE >= ?
          AND DATE <= ?
        """
        rows = self.db.execute_query(sql, (ef_rating, start_date, end_date))
        return rows[0][0]
    
    def count_ef_tornadoes_at_least(self, min_ef_rating, start_date, end_date):
        """
        Return how many tornadoes have EF scale >= min_ef_rating
        (e.g., 'EF1' => EF scale >= EF1 includes EF1, EF2, EF3...).
        
        We'll parse the numeric part of EF, compare in SQL.
        For instance, EF0 -> 0, EFU -> handle separately, etc.
        """
        # Convert string e.g. "EF2" -> integer 2
        # If EFU => treat as 0 or unknown?
        if min_ef_rating.upper().startswith("EFU"):
            # If we consider EFU < EF0, we might skip it or handle specially
            numeric_rating = -1  # so everything is above EFU
        else:
            numeric_rating = int(min_ef_rating[2])  # e.g. 'EF2' -> 2

        # We can either store numeric EF rating in the DB or do a dynamic check:
        # e.g. "WHERE SUBSTR(TOR_F_SCALE,3,1) >= numeric_rating" in ASCII is tricky, 
        # but let's do a CASE for EFU. 
        # Alternatively, if the DB has only 'EF0'..'EF5' or 'EFU', we can map them all:
        # EF0->0, EF1->1, EF2->2, EF3->3, EF4->4, EF5->5, EFU->-1
        # Then we’d store that numeric in a computed column or use a big CASE statement.

        # For simplicity here, we’ll do something like:
        sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE (
          CASE
            WHEN TOR_F_SCALE = 'EFU' THEN -1
            WHEN TOR_F_SCALE = 'EF0' THEN 0
            WHEN TOR_F_SCALE = 'EF1' THEN 1
            WHEN TOR_F_SCALE = 'EF2' THEN 2
            WHEN TOR_F_SCALE = 'EF3' THEN 3
            WHEN TOR_F_SCALE = 'EF4' THEN 4
            WHEN TOR_F_SCALE = 'EF5' THEN 5
          END
        ) >= ?
          AND DATE >= ?
          AND DATE <= ?
        """
        rows = self.db.execute_query(sql, (numeric_rating, start_date, end_date))
        return rows[0][0]

    # 2) Monthly/Yearly breakdown
    def monthly_breakdown(self):
        """
        Return a (month, count) breakdown for the entire dataset.
        The DATE is 'MM/DD/YYYY', so month = substr(DATE,1,2).
        """
        sql = f"""
        SELECT substr(DATE,1,2) AS Month, COUNT(*) 
        FROM {self.table}
        GROUP BY Month
        ORDER BY Month
        """
        return self.db.execute_query(sql)

    def yearly_breakdown(self):
        """
        Return a (year, count) breakdown for the entire dataset.
        The DATE is 'MM/DD/YYYY', so year = substr(DATE,7,4).
        """
        sql = f"""
        SELECT substr(DATE,7,4) AS Year, COUNT(*) 
        FROM {self.table}
        GROUP BY Year
        ORDER BY Year
        """
        return self.db.execute_query(sql)

    # 3) Top-N by property damage
    def top_property_damage(self, start_date, end_date, limit=5):
        """
        Return the top N tornadoes by DAMAGE_PROPERTY_NUM in the date range.
        """
        sql = f"""
        SELECT *
        FROM {self.table}
        WHERE DATE >= ?
          AND DATE <= ?
        ORDER BY DAMAGE_PROPERTY_NUM DESC
        LIMIT ?
        """
        rows = self.db.execute_query(sql, (start_date, end_date, limit))
        return rows

    # 4) Top-N by TOR_LENGTH
    def top_tornado_length(self, limit=5):
        """
        Return the top N tornadoes by the 'TOR_LENGTH' field (descending).
        """
        sql = f"""
        SELECT *
        FROM {self.table}
        ORDER BY TOR_LENGTH DESC
        LIMIT ?
        """
        rows = self.db.execute_query(sql, (limit,))
        return rows

    # 5) Percent of tornadoes that started between times
    def percent_of_tornadoes_between_times(self, start_time, end_time):
        """
        e.g. user enters start_time="1200", end_time="1800"
        We'll do string comparisons on BEGIN_TIME.
        """
        total_sql = f"SELECT COUNT(*) FROM {self.table}"
        total_count = self.db.execute_query(total_sql)[0][0]

        if total_count == 0:
            return 0.0

        time_sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE BEGIN_TIME >= ?
          AND BEGIN_TIME <= ?
        """
        in_range = self.db.execute_query(time_sql, (start_time, end_time))[0][0]

        return (in_range / total_count) * 100