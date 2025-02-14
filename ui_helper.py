# ui_helper.py

"""
This UI helper also parallels the old approach where we might show
query results from the SQL database. Now we do it for pandas DataFrames.
"""

import streamlit as st
import pandas as pd

class UIHelper:
    """
    A helper to store query results in session state and show them with checkboxes.
    Works similarly to the old approach that displayed SQL query results, but
    it's updated for pandas DataFrames in Streamlit.
    """

    @staticmethod
    def clear_dataset_keys(prefix: str):
        """
        Remove all st.session_state entries that start with the given prefix.
        This is how we 'clear old results' for the current dataset whenever
        the user runs a new query.
        """
        for k in list(st.session_state.keys()):
            if k.startswith(prefix):
                del st.session_state[k]

    @staticmethod
    def set_query_results(df: pd.DataFrame, key: str):
        """
        Store the DataFrame in st.session_state so we can later display/download it
        even after Streamlit reruns.
        """
        st.session_state[key] = df

    @staticmethod
    def show_and_download_results(key: str, label: str):
        """
        If st.session_state[key] exists and is a DataFrame, display checkboxes:
          1) Show in a table
          2) Download as CSV

        This is analogous to how we once displayed SQL results, but now for DataFrames.
        """
        if key not in st.session_state:
            return  # No data yet for this query

        df = st.session_state[key]
        if df is None or df.empty:
            st.write(f"No rows found for {label}.")
            return

        # Checkbox to show table
        show_table = st.checkbox(
            f"Show {label} results in a table?",
            key=f"show_{key}"  # unique widget key
        )
        if show_table:
            st.dataframe(df)

        # Checkbox to download
        download_csv = st.checkbox(
            f"Download {label} as CSV?",
            key=f"download_{key}"
        )
        if download_csv:
            csv_data = df.to_csv(index=False)
            st.download_button(
                label=f"Download {label} CSV",
                data=csv_data,
                file_name=f"{label}.csv",
                mime="text/csv",
                key=f"btn_download_{key}"
            )