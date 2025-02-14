from storm_database import StormDatabase

class TornadoSQL:
    def __init__(self, db):
        """
        Initialize TornadoSQL query wrapper.
        
        Args:
            db (StormDatabase): Database connection object to use for queries.
        """
        self.db = db
        self.table = "tornado"

    def _scale_case_expression(self):
        """
        Generate SQL CASE expression for standardizing tornado scales.
        
        Returns:
            str: SQL CASE expression that converts EF/F scales to numeric values:
                EFU/FU -> -1
                EF0/F0 -> 0
                ...
                EF5/F5 -> 5
                Invalid -> -999
        """
        return """
          CASE
            WHEN UPPER(TOR_F_SCALE) IN ('EFU','FU') THEN -1
            WHEN UPPER(TOR_F_SCALE) IN ('EF0','F0') THEN 0
            WHEN UPPER(TOR_F_SCALE) IN ('EF1','F1') THEN 1
            WHEN UPPER(TOR_F_SCALE) IN ('EF2','F2') THEN 2
            WHEN UPPER(TOR_F_SCALE) IN ('EF3','F3') THEN 3
            WHEN UPPER(TOR_F_SCALE) IN ('EF4','F4') THEN 4
            WHEN UPPER(TOR_F_SCALE) IN ('EF5','F5') THEN 5
            ELSE -999
          END
        """

    # ---------- EXACT EF/F TORNADOES ----------
    def count_ef_tornadoes_exact(self, rating_str, start_date, end_date):
        """
        Count tornadoes matching exact EF/F scale rating in date range.
        
        Args:
            rating_str (str): Target rating ('EF0'-'EF5', 'F0'-'F5', 'EFU', 'FU').
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
            
        Returns:
            int: Number of tornadoes matching exact rating.
        """
        # Convert rating_str e.g. 'EF3' or 'F3' to numeric scale
        # We'll do an equality comparison in SQL
        # But let's handle it purely in the CASE expression in SQL
        scale_case = self._scale_case_expression()

        # figure out the numeric target from the string
        numeric_target = -999
        rating_str = rating_str.strip().upper()
        if rating_str in ("EFU","FU"):
            numeric_target = -1
        elif rating_str.endswith("0") or rating_str.endswith("1") or rating_str.endswith("2") \
             or rating_str.endswith("3") or rating_str.endswith("4") or rating_str.endswith("5"):
            # last char is digit
            try:
                numeric_target = int(rating_str[-1])
            except ValueError:
                numeric_target = -999

        sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE ({scale_case}) = ?
          AND DATE >= ?
          AND DATE <= ?
        """
        rows = self.db.execute_query(sql, (numeric_target, start_date, end_date))
        return rows[0][0] if rows else 0
    
    # ---------- AT LEAST EF/F TORNADOES ----------
    def count_ef_tornadoes_at_least(self, min_rating_str, start_date, end_date):
        """
        Count tornadoes with EF/F scale >= specified rating in date range.
        
        Args:
            min_rating_str (str): Minimum rating ('EF0'-'EF5', 'F0'-'F5', 'EFU', 'FU').
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
            
        Returns:
            int: Number of tornadoes at or above specified rating.
        """
        scale_case = self._scale_case_expression()

        numeric_min = -999
        min_rating_str = min_rating_str.strip().upper()
        if min_rating_str in ("EFU","FU"):
            numeric_min = -1
        elif len(min_rating_str) >= 2 and min_rating_str[-1].isdigit():
            numeric_min = int(min_rating_str[-1])

        sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE ({scale_case}) >= ?
          AND DATE >= ?
          AND DATE <= ?
        """
        rows = self.db.execute_query(sql, (numeric_min, start_date, end_date))
        return rows[0][0] if rows else 0

    # ---------- MONTHLY BREAKDOWN ----------
    def monthly_breakdown(self):
        """
        strftime('%m', DATE) => '01'..'12'
        """
        sql = f"""
        SELECT strftime('%m', DATE) AS Month, COUNT(*)
        FROM {self.table}
        GROUP BY Month
        ORDER BY Month
        """
        return self.db.execute_query(sql)

    # ---------- YEARLY BREAKDOWN ----------
    def yearly_breakdown(self):
        """
        strftime('%Y', DATE) => e.g. '1952','1953','2024'
        """
        sql = f"""
        SELECT strftime('%Y', DATE) AS Year, COUNT(*)
        FROM {self.table}
        GROUP BY Year
        ORDER BY Year
        """
        return self.db.execute_query(sql)

    # ---------- TOP PROPERTY DAMAGE ----------
    def top_property_damage(self, start_date, end_date, limit=5):
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

    # ---------- TOP TORNADO LENGTH ----------
    def top_tornado_length(self, limit=5):
        sql = f"""
        SELECT *
        FROM {self.table}
        ORDER BY TOR_LENGTH DESC
        LIMIT ?
        """
        rows = self.db.execute_query(sql, (limit,))
        return rows

    # ---------- PERCENT OF TORNADOES BETWEEN TIMES ----------
    def percent_of_tornadoes_between_times(self, start_time, end_time):
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