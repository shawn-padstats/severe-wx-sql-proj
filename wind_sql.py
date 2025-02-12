from storm_database import StormDatabase

class WindSQL:
    def __init__(self, db: StormDatabase):
        self.db = db
        self.table = "wind"
    
    def count_wind_gusts(self, min_knots, start_date, end_date):
        sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE [MAGNITUDE (Knots)] >= ?
          AND DATE >= ?
          AND DATE <= ?
        """
        row = self.db.execute_query(sql, (min_knots, start_date, end_date))
        return row[0][0]
    
    def get_top_property_damage(self, start_date, end_date, limit=5):
        sql = f"""
        SELECT *
        FROM {self.table}
        WHERE DATE >= ?
          AND DATE <= ?
        ORDER BY DAMAGE_PROPERTY_NUM DESC
        LIMIT ?
        """
        return self.db.execute_query(sql, (start_date, end_date, limit))
    
    def get_percentile_rank(self, gust_knots):
        sql_count_less = f"SELECT COUNT(*) FROM {self.table} WHERE [MAGNITUDE (Knots)] < ?"
        count_less = self.db.execute_query(sql_count_less, (gust_knots,))[0][0]

        sql_count_total = f"SELECT COUNT(*) FROM {self.table}"
        count_total = self.db.execute_query(sql_count_total)[0][0]

        if count_total == 0:
            return 0
        return (count_less / count_total) * 100
    
    def monthly_breakdown(self):
        sql = f"""
        SELECT substr(DATE, 1, 2) AS Month, COUNT(*) 
        FROM {self.table}
        GROUP BY Month
        ORDER BY Month
        """
        return self.db.execute_query(sql)
    
    def yearly_breakdown(self):
        sql = f"""
        SELECT substr(DATE, 7, 4) AS Year, COUNT(*) 
        FROM {self.table}
        GROUP BY Year
        ORDER BY Year
        """
        return self.db.execute_query(sql)
    
    def percent_of_events_in_time_range(self, start_time, end_time):
        total_sql = f"SELECT COUNT(*) FROM {self.table}"
        total = self.db.execute_query(total_sql)[0][0]

        if total == 0:
            return 0

        sql = f"""
        SELECT COUNT(*)
        FROM {self.table}
        WHERE BEGIN_TIME >= ?
          AND BEGIN_TIME <= ?
        """
        in_range = self.db.execute_query(sql, (start_time, end_time))[0][0]

        return (in_range / total) * 100