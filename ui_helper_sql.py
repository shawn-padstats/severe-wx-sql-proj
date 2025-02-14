# ui_helper_sql.py

import streamlit as st
import pandas as pd

class UIHelperSQL:
    """
    A helper for the SQL-based approach to store or display query results.
    It uses st.session_state to hold the result DataFrame, then provides
    checkboxes to show/download.
    """

    @staticmethod
    def clear_dataset_keys(prefix: str):
        """
        Remove all st.session_state entries that start with the given prefix.
        This is optional, but if you want to ensure each new query for a dataset
        wipes out older results, call this before setting new results.
        """
        for key in list(st.session_state.keys()):
            if key.startswith(prefix):
                del st.session_state[key]

    @staticmethod
    def set_query_results(df: pd.DataFrame, key: str):
        """
        Store the DataFrame in st.session_state with a unique key.
        """
        st.session_state[key] = df

    @staticmethod
    def show_and_download_results(key: str, label: str):
        """
        If there's a DataFrame stored at st.session_state[key], display
        checkboxes to show the table and to download it as CSV.
        """
        if key not in st.session_state:
            return

        df = st.session_state[key]
        if df is None or df.empty:
            st.write(f"No rows found for {label}.")
            return

        # 1) Checkbox to show table
        show_table = st.checkbox(f"Show {label} in a table?", key=f"show_{key}")
        if show_table:
            st.dataframe(df)

        # 2) Checkbox to download CSV
        download_csv = st.checkbox(f"Download {label} as CSV?", key=f"download_{key}")
        if download_csv:
            csv_data = df.to_csv(index=False)
            st.download_button(
                label=f"Download {label} CSV",
                data=csv_data,
                file_name=f"{label}.csv",
                mime="text/csv",
                key=f"btn_{key}"
            )