import duckdb
import streamlit as st
import pandas as pd

st.title("HG Insights - Telecom Churn Pipeline")
con = duckdb.connect('../data/staging/staging.db')
df = con.execute("SELECT churn, COUNT(*) as count FROM clean_churn GROUP BY churn").df()
st.bar_chart(df.set_index('churn'))