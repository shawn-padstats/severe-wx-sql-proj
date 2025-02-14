from storm_database import StormDatabase
from wind_sql import WindSQL
from tornado_sql import TornadoSQL
from hail_sql import HailSQL
import streamlit as st
#############################################
# 4) MAIN Streamlit app
#############################################

def main():
    st.title("Severe Weather Data Explorer")

    # 1) Create brand-new DB, removing old file.
    db = StormDatabase("storms.db", recreate=True)

    # 2) Create all tables
    db.create_table("wind")
    db.create_table("tornado")
    db.create_table("hail")

    # 3) Load CSV data
    db.load_csv_into_table("wind_historical_data.csv", "wind")
    db.load_csv_into_table("tor_historical_data.csv", "tornado")
    db.load_csv_into_table("hail_historical_data.csv", "hail")

    # 4) Instantiate the SQL classes
    wind = WindSQL(db)
    tornado = TornadoSQL(db)
    hail = HailSQL(db)

    # 5) Provide the user with the dataset choice
    dataset_choice = st.radio("Pick a Dataset:", ["Wind", "Tornado", "Hail"])

    if dataset_choice == "Wind":
        # (Your existing code to handle wind queries)
        query_type = st.selectbox(
            "Choose a Wind Query",
            [
                "Count wind gusts >= X between dates",
                "Top-N property damage (in date range)",
                "Percentile rank of a certain gust",
                "Monthly breakdown (entire dataset)",
                "Yearly breakdown (entire dataset)",
                "Percent of events between times",
            ]
        )
        if query_type == "Count wind gusts >= X between dates":
            min_knots = st.number_input("Minimum knots", min_value=0.0, value=80.0)
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            if st.button("Run Query"):
                result = wind.count_wind_gusts(min_knots, start_date, end_date)
                st.write(f"Found {result} wind events >= {min_knots} knots")

        elif query_type == "Top-N property damage (in date range)":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("Top N results", min_value=1, value=5)
            if st.button("Run Query"):
                rows = wind.get_top_property_damage(start_date, end_date, limit)
                st.write(rows)

        elif query_type == "Percentile rank of a certain gust":
            gust_knots = st.number_input("Wind gust (knots)", min_value=0.0, value=80.0)
            if st.button("Run Query"):
                p = wind.get_percentile_rank(gust_knots)
                st.write(f"{gust_knots} knots is in the {p:.1f}% percentile")

        elif query_type == "Monthly breakdown (entire dataset)":
            if st.button("Run Query"):
                breakdown = wind.monthly_breakdown()
                st.write("Month | Count")
                for m, c in breakdown:
                    st.write(f"{m} | {c}")

        elif query_type == "Yearly breakdown (entire dataset)":
            if st.button("Run Query"):
                breakdown = wind.yearly_breakdown()
                st.write("Year | Count")
                for y, c in breakdown:
                    st.write(f"{y} | {c}")

        elif query_type == "Percent of events between times":
            t_start = st.text_input("Begin time (HHMM)", "0000")
            t_end = st.text_input("End time (HHMM)", "2359")
            if st.button("Run Query"):
                pct = wind.percent_of_events_in_time_range(t_start, t_end)
                st.write(f"{pct:.1f}% of events occurred between {t_start} and {t_end}")

    elif dataset_choice == "Tornado":
        # (Your existing code to handle tornado queries)
        query_type = st.selectbox(
            "Choose a Tornado Query",
            [
                "Count EF tornadoes (exact)",
                "Count EF tornadoes >= rating",
                "Monthly breakdown",
                "Yearly breakdown",
                "Top-N by property damage",
                "Top-N by tornado length",
                "Percent of tornadoes between times",
            ]
        )
        if query_type == "Count EF tornadoes (exact)":
            ef_scale = st.text_input("EF Scale (EF0..EF5, F0..F5, EFU, FU)", "EF1")
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            if st.button("Run Query"):
                result = tornado.count_ef_tornadoes_exact(ef_scale, start_date, end_date)
                st.write(f"Found {result} tornadoes with scale == {ef_scale}")

        elif query_type == "Count EF tornadoes >= rating":
            ef_scale = st.text_input("Minimum EF Scale (EF0..EF5, F0..F5, EFU..FU)", "EF1")
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            if st.button("Run Query"):
                result = tornado.count_ef_tornadoes_at_least(ef_scale, start_date, end_date)
                st.write(f"Found {result} tornadoes with scale >= {ef_scale}")

        elif query_type == "Monthly breakdown":
            if st.button("Run Query"):
                breakdown = tornado.monthly_breakdown()
                st.write("Month | Count")
                for m, c in breakdown:
                    st.write(f"{m} | {c}")

        elif query_type == "Yearly breakdown":
            if st.button("Run Query"):
                breakdown = tornado.yearly_breakdown()
                st.write("Year | Count")
                for y, c in breakdown:
                    st.write(f"{y} | {c}")

        elif query_type == "Top-N by property damage":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("How many results?", min_value=1, value=5)
            if st.button("Run Query"):
                rows = tornado.top_property_damage(start_date, end_date, limit)
                st.write(rows)

        elif query_type == "Top-N by tornado length":
            limit = st.number_input("How many results?", min_value=1, value=5)
            if st.button("Run Query"):
                rows = tornado.top_tornado_length(limit)
                st.write(rows)

        elif query_type == "Percent of tornadoes between times":
            start_time = st.text_input("Start time (HHMM)", "0000")
            end_time = st.text_input("End time (HHMM)", "2359")
            if st.button("Run Query"):
                pct = tornado.percent_of_tornadoes_between_times(start_time, end_time)
                st.write(f"{pct:.1f}% of tornadoes began between {start_time} and {end_time}")

    else:  # "Hail"
        query_type = st.selectbox(
            "Choose a Hail Query",
            [
                "Count hail >= size (inches) between dates",
                "Monthly breakdown",
                "Yearly breakdown",
                "Top-N by property damage",
                "Percent of hail events between times"
            ]
        )
        if query_type == "Count hail >= size (inches) between dates":
            min_hail = st.number_input("Minimum hail size (inches)", min_value=0.0, value=1.0)
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            if st.button("Run Query"):
                result = hail.count_hail_above_size(min_hail, start_date, end_date)
                st.write(f"Found {result} hail events >= {min_hail}\".")

        elif query_type == "Monthly breakdown":
            if st.button("Run Query"):
                breakdown = hail.monthly_breakdown()
                st.write("Month | Count")
                for m, c in breakdown:
                    st.write(f"{m} | {c}")

        elif query_type == "Yearly breakdown":
            if st.button("Run Query"):
                breakdown = hail.yearly_breakdown()
                st.write("Year | Count")
                for y, c in breakdown:
                    st.write(f"{y} | {c}")

        elif query_type == "Top-N by property damage":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("Top N results", min_value=1, value=5)
            if st.button("Run Query"):
                rows = hail.top_property_damage(start_date, end_date, limit)
                st.write(rows)

        elif query_type == "Percent of hail events between times":
            start_time = st.text_input("Begin time (HHMM)", "0000")
            end_time = st.text_input("End time (HHMM)", "2359")
            if st.button("Run Query"):
                pct = hail.percent_of_hail_in_time_range(start_time, end_time)
                st.write(f"{pct:.1f}% of hail events occurred between {start_time} and {end_time}")

    # optionally, a button to close the DB
    if st.button("Close DB"):
        db.close()
        st.write("Database connection closed.")


if __name__ == "__main__":
    main()