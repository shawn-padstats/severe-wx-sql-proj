from storm_database import StormDatabase

class HailSQL:
    def __init__(self, db):
        """
        Initialize HailSQL query wrapper.
        
        Args:
            db (StormDatabase): Database connection object to use for queries.
        """
        self.db = db
        self.table = "hail"
    
    def count_hail_above_size(self, min_size, start_date, end_date):
        """
        Count hail events with size >= specified inches in date range.
        
        Args:
            min_size (float): Minimum hail size in inches.
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
            
        Returns:
            int: Number of matching hail events.
        """
        sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE [HAIL SIZE (INCHES)] >= ?
          AND DATE >= ?
          AND DATE <= ?
        """
        row = self.db.execute_query(sql, (min_size, start_date, end_date))
        return row[0][0]

    def monthly_breakdown(self):
        """
        Group hail events by month across all years.
        
        Returns:
            list: List of (month, count) tuples, where month is '01'-'12'.
        """
        sql = f"""
        SELECT strftime('%m', DATE) AS Month, COUNT(*)
        FROM {self.table}
        GROUP BY Month
        ORDER BY Month
        """
        return self.db.execute_query(sql)

    def yearly_breakdown(self):
        """
        Group hail events by year across entire dataset.
        
        Returns:
            list: List of (year, count) tuples, where year is like '1950'.
        """
        sql = f"""
        SELECT strftime('%Y', DATE) AS Year, COUNT(*)
        FROM {self.table}
        GROUP BY Year
        ORDER BY Year
        """
        return self.db.execute_query(sql)

    def top_property_damage(self, start_date, end_date, limit=5):
        """
        Get hail events with highest property damage within date range.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
            limit (int): Maximum number of results to return. Defaults to 5.
            
        Returns:
            list: List of hail event records sorted by damage amount.
        """
        sql = f"""
        SELECT *
        FROM {self.table}
        WHERE DATE >= ?
          AND DATE <= ?
        ORDER BY DAMAGE_PROPERTY_NUM DESC
        LIMIT ?
        """
        return self.db.execute_query(sql, (start_date, end_date, limit))

    def percent_of_hail_in_time_range(self, start_time, end_time):
        """
        Calculate percentage of hail events occurring between specified times.
        
        Args:
            start_time (str): Start time in HHMM format (e.g., '1400').
            end_time (str): End time in HHMM format (e.g., '1600').
            
        Returns:
            float: Percentage of events occurring in time range.
        """
        total_sql = f"SELECT COUNT(*) FROM {self.table}"
        total = self.db.execute_query(total_sql)[0][0]
        if total == 0:
            return 0.0

        range_sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE BEGIN_TIME >= ?
          AND BEGIN_TIME <= ?
        """
        in_range = self.db.execute_query(range_sql, (start_time, end_time))[0][0]
        return (in_range / total) * 100