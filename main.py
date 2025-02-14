#########################################
# main.py
#########################################

"""
This script was originally designed around an SQL-based approach (e.g., StormDatabase + WindSQL, etc.).
We have replaced that SQL logic with pandas-based dataframes, but the user-facing queries remain the same.
The structure of the queries (e.g., count, top-N, breakdown) is identical to our old SQL code.
"""

import streamlit as st
import pandas as pd

# Replaces old 'StormDatabase' with 'StormDataFrames'
from storm_dataframes import StormDataFrames

# Replaces old 'WindSQL', 'TornadoSQL', 'HailSQL' with 'WindDF', 'TornadoDF', 'HailDF'
from wind_df import WindDF
from tornado_df import TornadoDF
from hail_df import HailDF

from ui_helper import UIHelper

def main():
    st.title("Severe Weather Data Explorer (DataFrame Edition)")

    # 1) In-memory data store (instead of StormDatabase for SQLite)
    db = StormDataFrames(recreate=True)

    # 2) Create the "tables" (which in pandas just ensures DataFrames exist)
    db.create_table("wind")
    db.create_table("tornado")
    db.create_table("hail")

    # 3) Load CSV data (similar to old 'db.load_csv_into_table(...)' for SQL, but now into DataFrames)
    db.load_csv_into_table("wind_historical_data.csv", "wind")
    db.load_csv_into_table("tor_historical_data.csv", "tornado")
    db.load_csv_into_table("hail_historical_data.csv", "hail")

    # 4) Instantiate classes that replace old 'WindSQL', 'TornadoSQL', 'HailSQL'
    wind = WindDF(db)
    tornado = TornadoDF(db)
    hail = HailDF(db)

    # 5) Let user pick a dataset (same UI concept as old approach)
    dataset_choice = st.radio("Pick a Dataset:", ["Wind", "Tornado", "Hail"])
    key_prefix = dataset_choice.lower()

    #
    # --------------------------------------------------------------------------
    # WIND (similar queries to old SQL-based code, but now in pandas)
    # --------------------------------------------------------------------------
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

        # (1) Count wind gusts >= X
        if query_type == "Count wind gusts >= X between dates":
            min_knots = st.number_input("Minimum knots", min_value=0.0, value=80.0)
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")

            if st.button("Run Query"):
                # Clear old results for wind
                UIHelper.clear_dataset_keys(key_prefix)

                # This is analogous to the old 'wind.count_wind_gusts(...)' in SQL
                count_val = wind.count_wind_gusts(min_knots, start_date, end_date)
                df_result = pd.DataFrame({"Count": [count_val]})
                UIHelper.set_query_results(df_result, f"{key_prefix}_count_gusts")
                
            UIHelper.show_and_download_results(f"{key_prefix}_count_gusts", "Wind Gusts Count")

        # (2) Top-N property damage
        elif query_type == "Top-N property damage (in date range)":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("Top N results", min_value=1, value=5)

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                rows = wind.get_top_property_damage(start_date, end_date, limit)
                df_result = pd.DataFrame(rows)
                UIHelper.set_query_results(df_result, f"{key_prefix}_top_damage")
                
            UIHelper.show_and_download_results(f"{key_prefix}_top_damage", "Wind Top Damage")

        # (3) Percentile rank of a certain gust
        elif query_type == "Percentile rank of a certain gust":
            gust_knots = st.number_input("Wind gust (knots)", min_value=0.0, value=80.0)

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                pval = wind.get_percentile_rank(gust_knots)
                df_result = pd.DataFrame({"Percentile": [pval]})
                UIHelper.set_query_results(df_result, f"{key_prefix}_percentile")
                
            UIHelper.show_and_download_results(f"{key_prefix}_percentile", "Wind Gust Percentile")

        # (4) Monthly breakdown
        elif query_type == "Monthly breakdown (entire dataset)":
            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                breakdown = wind.monthly_breakdown()  # list of (month, count)
                df_result = pd.DataFrame(breakdown, columns=["Month", "Count"])
                UIHelper.set_query_results(df_result, f"{key_prefix}_monthly")
                
            UIHelper.show_and_download_results(f"{key_prefix}_monthly", "Wind Monthly Breakdown")

        # (5) Yearly breakdown
        elif query_type == "Yearly breakdown (entire dataset)":
            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                breakdown = wind.yearly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Year", "Count"])
                UIHelper.set_query_results(df_result, f"{key_prefix}_yearly")
                
            UIHelper.show_and_download_results(f"{key_prefix}_yearly", "Wind Yearly Breakdown")

        # (6) Percent of events between times
        elif query_type == "Percent of events between times":
            t_start = st.text_input("Begin time (HHMM)", "0000")
            t_end = st.text_input("End time (HHMM)", "2359")

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                pct = wind.percent_of_events_in_time_range(t_start, t_end)
                df_result = pd.DataFrame({"Percent (%)": [pct]})
                UIHelper.set_query_results(df_result, f"{key_prefix}_pct_time")
                
            UIHelper.show_and_download_results(f"{key_prefix}_pct_time", "Wind Events % in Time Range")

    #
    # --------------------------------------------------------------------------
    # TORNADO (similarly parallels the old TornadoSQL code)
    # --------------------------------------------------------------------------
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

        # (1) Count EF tornadoes (exact)
        if query_type == "Count EF tornadoes (exact)":
            ef_scale = st.text_input("EF Scale (EF0..EF5, F0..F5, EFU, FU)", "EF1")
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                val = tornado.count_ef_tornadoes_exact(ef_scale, start_date, end_date)
                df_result = pd.DataFrame({"Count": [val]})
                UIHelper.set_query_results(df_result, f"{key_prefix}_ef_exact")
                
            UIHelper.show_and_download_results(f"{key_prefix}_ef_exact", "Tornado EF Exact Count")

        # (2) Count EF tornadoes >= rating
        elif query_type == "Count EF tornadoes >= rating":
            ef_scale = st.text_input("Minimum EF Scale (EF0..EF5, F0..F5, EFU..FU)", "EF1")
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                val = tornado.count_ef_tornadoes_at_least(ef_scale, start_date, end_date)
                df_result = pd.DataFrame({"Count": [val]})
                UIHelper.set_query_results(df_result, f"{key_prefix}_ef_atleast")
                
            UIHelper.show_and_download_results(f"{key_prefix}_ef_atleast", "Tornado EF >= Count")

        # (3) Monthly breakdown
        elif query_type == "Monthly breakdown":
            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                breakdown = tornado.monthly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Month", "Count"])
                UIHelper.set_query_results(df_result, f"{key_prefix}_monthly")
                
            UIHelper.show_and_download_results(f"{key_prefix}_monthly", "Tornado Monthly Breakdown")

        # (4) Yearly breakdown
        elif query_type == "Yearly breakdown":
            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                breakdown = tornado.yearly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Year", "Count"])
                UIHelper.set_query_results(df_result, f"{key_prefix}_yearly")
                
            UIHelper.show_and_download_results(f"{key_prefix}_yearly", "Tornado Yearly Breakdown")

        # (5) Top-N by property damage
        elif query_type == "Top-N by property damage":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("How many results?", min_value=1, value=5)

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                rows = tornado.top_property_damage(start_date, end_date, limit)
                df_result = pd.DataFrame(rows)
                UIHelper.set_query_results(df_result, f"{key_prefix}_top_damage")
                
            UIHelper.show_and_download_results(f"{key_prefix}_top_damage", "Tornado Top Damage")

        # (6) Top-N by tornado length
        elif query_type == "Top-N by tornado length":
            limit = st.number_input("How many results?", min_value=1, value=5)

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                rows = tornado.top_tornado_length(limit)
                df_result = pd.DataFrame(rows)
                UIHelper.set_query_results(df_result, f"{key_prefix}_top_length")
                
            UIHelper.show_and_download_results(f"{key_prefix}_top_length", "Tornado Top Length")

        # (7) Percent of tornadoes between times
        elif query_type == "Percent of tornadoes between times":
            start_time = st.text_input("Start time (HHMM)", "0000")
            end_time = st.text_input("End time (HHMM)", "2359")

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                pct = tornado.percent_of_tornadoes_between_times(start_time, end_time)
                df_result = pd.DataFrame({"Percent (%)": [pct]})
                UIHelper.set_query_results(df_result, f"{key_prefix}_pct_time")
                
            UIHelper.show_and_download_results(f"{key_prefix}_pct_time", "Tornado % Between Times")

    #
    # --------------------------------------------------------------------------
    # HAIL (similarly parallels old HailSQL code)
    # --------------------------------------------------------------------------
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

        # (1) Count hail >= size
        if query_type == "Count hail >= size (inches) between dates":
            min_hail = st.number_input("Minimum hail size (inches)", min_value=0.0, value=1.0)
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                count_val = hail.count_hail_above_size(min_hail, start_date, end_date)
                df_result = pd.DataFrame({"Count": [count_val]})
                UIHelper.set_query_results(df_result, f"{key_prefix}_count_size")

            UIHelper.show_and_download_results(f"{key_prefix}_count_size", "Hail Count >= Size")

        # (2) Monthly breakdown
        elif query_type == "Monthly breakdown":
            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                breakdown = hail.monthly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Month", "Count"])
                UIHelper.set_query_results(df_result, f"{key_prefix}_monthly")
                
            UIHelper.show_and_download_results(f"{key_prefix}_monthly", "Hail Monthly Breakdown")

        # (3) Yearly breakdown
        elif query_type == "Yearly breakdown":
            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                breakdown = hail.yearly_breakdown()
                df_result = pd.DataFrame(breakdown, columns=["Year", "Count"])
                UIHelper.set_query_results(df_result, f"{key_prefix}_yearly")
                
            UIHelper.show_and_download_results(f"{key_prefix}_yearly", "Hail Yearly Breakdown")

        # (4) Top-N by property damage
        elif query_type == "Top-N by property damage":
            start_date = st.text_input("Start Date (YYYY-MM-DD)", "1950-01-01")
            end_date = st.text_input("End Date (YYYY-MM-DD)", "2025-12-31")
            limit = st.number_input("Top N results", min_value=1, value=5)

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                rows = hail.top_property_damage(start_date, end_date, limit)
                df_result = pd.DataFrame(rows)
                UIHelper.set_query_results(df_result, f"{key_prefix}_top_damage")
                
            UIHelper.show_and_download_results(f"{key_prefix}_top_damage", "Hail Top Damage")

        # (5) Percent of hail events between times
        elif query_type == "Percent of hail events between times":
            start_time = st.text_input("Begin time (HHMM)", "0000")
            end_time = st.text_input("End time (HHMM)", "2359")

            if st.button("Run Query"):
                UIHelper.clear_dataset_keys(key_prefix)

                pct = hail.percent_of_hail_in_time_range(start_time, end_time)
                df_result = pd.DataFrame({"Percent (%)": [pct]})
                UIHelper.set_query_results(df_result, f"{key_prefix}_pct_time")
                
            UIHelper.show_and_download_results(f"{key_prefix}_pct_time", "Hail % Between Times")

    # Optionally, a button to close "DB" (in memory)
    if st.button("Close DB"):
        db.close()
        st.write("DataFrames cleared.")

if __name__ == "__main__":
    main()