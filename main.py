#############################################
# main.py (SQL Edition with UIHelperSQL)
#############################################

import streamlit as st
import pandas as pd

from storm_database import StormDatabase
from wind_sql import WindSQL
from tornado_sql import TornadoSQL
from hail_sql import HailSQL

# This is our new helper that handles showing/downloading DataFrame results in Streamlit
from ui_helper_sql import UIHelperSQL

def main():
    st.title("Severe Weather Data Explorer (SQL Edition)")

    # 1) Create brand-new DB (removes old file if recreate=True)
    db = StormDatabase("storms.db", recreate=True)

    # 2) Create tables
    db.create_table("wind")
    db.create_table("tornado")
    db.create_table("hail")

    # 3) Load CSV data into those tables
    db.load_csv_into_table("wind_historical_data.csv", "wind")
    db.load_csv_into_table("tor_historical_data.csv", "tornado")
    db.load_csv_into_table("hail_historical_data.csv", "hail")

    # 4) Instantiate SQL classes (like your old approach)
    wind = WindSQL(db)
    tornado = TornadoSQL(db)
    hail = HailSQL(db)

    # 5) Let user pick which dataset they'd like to query
    dataset_choice = st.radio("Pick a Dataset:", ["Wind", "Tornado", "Hail"])
    key_prefix = dataset_choice.lower()  # e.g. "wind", "tornado", or "hail"

    ###################################
    # WIND QUERIES
    ###################################
    if dataset_choice == "Wind":
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

        # 1) Count wind gusts >= X
        if query_type == "Count wind gusts >= X between dates":
            min_knots = st.number_input("Minimum knots", min_value=0.0, value=80.0)
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")

            if st.button("Run Query"):
                # Clear old results for 'wind'
                UIHelperSQL.clear_dataset_keys(key_prefix)

                # Execute the SQL query
                result = wind.count_wind_gusts(min_knots, start_date, end_date)
                # Convert the single integer to a 1-row DataFrame
                df_result = pd.DataFrame({"Count": [result]})
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_count_gusts")

                # Let user show/download the last results for this query
                UIHelperSQL.show_and_download_results(f"{key_prefix}_count_gusts", "Wind Gusts Count")

        # 2) Top-N property damage
        elif query_type == "Top-N property damage (in date range)":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("Top N results", min_value=1, value=5)

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                rows = wind.get_top_property_damage(start_date, end_date, limit)
                df_result = pd.DataFrame(rows)
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_top_damage")

                # Let user show/download the last results for this query
                UIHelperSQL.show_and_download_results(f"{key_prefix}_top_damage", "Wind Top Damage")

        # 3) Percentile rank of a certain gust
        elif query_type == "Percentile rank of a certain gust":
            gust_knots = st.number_input("Wind gust (knots)", min_value=0.0, value=80.0)

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                pval = wind.get_percentile_rank(gust_knots)
                df_result = pd.DataFrame({"Percentile": [pval]})
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_percentile")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_percentile", "Wind Gust Percentile")

        # 4) Monthly breakdown (entire dataset)
        elif query_type == "Monthly breakdown (entire dataset)":
            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                breakdown = wind.monthly_breakdown()  # list of (month, count)
                df_result = pd.DataFrame(breakdown, columns=["Month", "Count"])
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_monthly")

                # Let user show/download the last results for this query
                UIHelperSQL.show_and_download_results(f"{key_prefix}_monthly", "Wind Monthly Breakdown")

        # 5) Yearly breakdown (entire dataset)
        elif query_type == "Yearly breakdown (entire dataset)":
            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                breakdown = wind.yearly_breakdown()  # list of (year, count)
                df_result = pd.DataFrame(breakdown, columns=["Year", "Count"])
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_yearly")

                # Let user show/download the last results for this query
                UIHelperSQL.show_and_download_results(f"{key_prefix}_yearly", "Wind Yearly Breakdown")

        # 6) Percent of events between times
        elif query_type == "Percent of events between times":
            t_start = st.text_input("Begin time (HHMM)", "0000")
            t_end = st.text_input("End time (HHMM)", "2359")

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                pct = wind.percent_of_events_in_time_range(t_start, t_end)
                df_result = pd.DataFrame({"Percent (%)": [pct]})
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_pct_time")

                # Let user show/download the last results for this query
                UIHelperSQL.show_and_download_results(f"{key_prefix}_pct_time", "Wind Events % in Time Range")

    ###################################
    # TORNADO QUERIES
    ###################################
    elif dataset_choice == "Tornado":
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

        # 1) Count EF tornadoes (exact)
        if query_type == "Count EF tornadoes (exact)":
            ef_scale = st.text_input("EF Scale (EF0..EF5, F0..F5, EFU, FU)", "EF1")
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                result = tornado.count_ef_tornadoes_exact(ef_scale, start_date, end_date)
                df_result = pd.DataFrame({"Count": [result]})
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_ef_exact")

                # Let user show/download the last results for this query
                UIHelperSQL.show_and_download_results(f"{key_prefix}_ef_exact", "Tornado EF Exact Count")

        # 2) Count EF tornadoes >= rating
        elif query_type == "Count EF tornadoes >= rating":
            ef_scale = st.text_input("Minimum EF Scale (EF0..EF5, F0..F5, EFU..FU)", "EF1")
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                result = tornado.count_ef_tornadoes_at_least(ef_scale, start_date, end_date)
                df_result = pd.DataFrame({"Count": [result]})
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_ef_atleast")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_ef_atleast", "Tornado EF >= Count")

        # 3) Monthly breakdown
        elif query_type == "Monthly breakdown":
            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                breakdown = tornado.monthly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Month", "Count"])
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_monthly")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_monthly", "Tornado Monthly Breakdown")

        # 4) Yearly breakdown
        elif query_type == "Yearly breakdown":
            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                breakdown = tornado.yearly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Year", "Count"])
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_yearly")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_yearly", "Tornado Yearly Breakdown")

        # 5) Top-N by property damage
        elif query_type == "Top-N by property damage":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("How many results?", min_value=1, value=5)

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                rows = tornado.top_property_damage(start_date, end_date, limit)
                df_result = pd.DataFrame(rows)
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_top_damage")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_top_damage", "Tornado Top Damage")

        # 6) Top-N by tornado length
        elif query_type == "Top-N by tornado length":
            limit = st.number_input("How many results?", min_value=1, value=5)

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                rows = tornado.top_tornado_length(limit)
                df_result = pd.DataFrame(rows)
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_top_length")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_top_length", "Tornado Top Length")

        # 7) Percent of tornadoes between times
        elif query_type == "Percent of tornadoes between times":
            start_time = st.text_input("Start time (HHMM)", "0000")
            end_time = st.text_input("End time (HHMM)", "2359")

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                pct = tornado.percent_of_tornadoes_between_times(start_time, end_time)
                df_result = pd.DataFrame({"Percent (%)": [pct]})
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_pct_time")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_pct_time", "Tornado % Between Times")

    ###################################
    # HAIL QUERIES
    ###################################
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

        # 1) Count hail >= size (inches) between dates
        if query_type == "Count hail >= size (inches) between dates":
            min_hail = st.number_input("Minimum hail size (inches)", min_value=0.0, value=1.0)
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                result = hail.count_hail_above_size(min_hail, start_date, end_date)
                df_result = pd.DataFrame({"Count": [result]})
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_count_size")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_count_size", "Hail Count >= Size")

        # 2) Monthly breakdown
        elif query_type == "Monthly breakdown":
            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                breakdown = hail.monthly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Month", "Count"])
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_monthly")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_monthly", "Hail Monthly Breakdown")

        # 3) Yearly breakdown
        elif query_type == "Yearly breakdown":
            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                breakdown = hail.yearly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Year", "Count"])
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_yearly")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_yearly", "Hail Yearly Breakdown")

        # 4) Top-N by property damage
        elif query_type == "Top-N by property damage":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("Top N results", min_value=1, value=5)

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                rows = hail.top_property_damage(start_date, end_date, limit)
                df_result = pd.DataFrame(rows)
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_top_damage")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_top_damage", "Hail Top Damage")

        # 5) Percent of hail events between times
        elif query_type == "Percent of hail events between times":
            start_time = st.text_input("Begin time (HHMM)", "0000")
            end_time = st.text_input("End time (HHMM)", "2359")

            if st.button("Run Query"):
                UIHelperSQL.clear_dataset_keys(key_prefix)
                pct = hail.percent_of_hail_in_time_range(start_time, end_time)
                df_result = pd.DataFrame({"Percent (%)": [pct]})
                UIHelperSQL.set_query_results(df_result, f"{key_prefix}_pct_time")

                UIHelperSQL.show_and_download_results(f"{key_prefix}_pct_time", "Hail % Between Times")

    # Optionally, a button to close the DB
    if st.button("Close DB"):
        db.close()
        st.write("Database connection closed.")


if __name__ == "__main__":
    main()