import streamlit as st
import pandas as pd
from agent import run_query   # your NLP query function

st.title("📊 Business Intelligence Agent")
st.write("Ask questions about Monday.com data")

query = st.text_input("Enter your question:")

if st.button("Run Query"):
    if query:
        result = run_query(query)
        st.write("### Result")
        st.write(result)