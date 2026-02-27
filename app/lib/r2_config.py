"""
R2 credentials from Streamlit secrets. Use only in Streamlit context.
"""
import streamlit as st


def get_r2_config():
    return {
        "key": st.secrets["R2_ACCESS_KEY"],
        "secret": st.secrets["R2_SECRET_KEY"],
        "endpoint": st.secrets["R2_ENDPOINT"],
        "bucket": st.secrets["R2_BUCKET"],
    }
