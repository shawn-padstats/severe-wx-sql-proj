class HailSQL:
    def __init__(self, db):
        self.db = db
        self.table = "hail"
    
    def count_hail_above_size(self, min_size, start_date, end_date):
        """
        Return the number of hail events with [HAIL SIZE (INCHES)] >= min_size,
        within the given date range (DATE as 'MM/DD/YY' or 'MM/DD/YYYY').
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
        Group hail events by month (assuming DATE is 'MM/DD/YY' or 'MM/DD/YYYY').
        """
        sql = f"""
        SELECT substr(DATE, 1, 2) AS Month, COUNT(*)
        FROM {self.table}
        GROUP BY Month
        ORDER BY Month
        """
        return self.db.execute_query(sql)

    def yearly_breakdown(self):
        """
        Group hail events by year (assuming DATE is 'MM/DD/YYYY' or 'MM/DD/YY').
        If all are 'MM/DD/YY' with leading zeros, substring( DATE, 7, 2 ) might be used for the year.
        Or if 'MM/DD/YYYY', substring( DATE, 7, 4 ) for the year.
        """
        sql = f"""
        SELECT substr(DATE, 7, 4) AS Year, COUNT(*)
        FROM {self.table}
        GROUP BY Year
        ORDER BY Year
        """
        return self.db.execute_query(sql)

    def top_property_damage(self, start_date, end_date, limit=5):
        """
        Return the top N hail events by property damage in the date range.
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
        Return the % of hail events whose BEGIN_TIME is between start_time and end_time.
        """
        total_sql = f"SELECT COUNT(*) FROM {self.table}"
        total = self.db.execute_query(total_sql)[0][0]
        if total == 0:
            return 0

        range_sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE BEGIN_TIME >= ?
          AND BEGIN_TIME <= ?
        """
        in_range = self.db.execute_query(range_sql, (start_time, end_time))[0][0]
        return (in_range / total) * 100