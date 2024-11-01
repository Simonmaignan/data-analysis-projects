"""
# Welcome to Streamlit!

Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:

If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).

In the meantime, below is an example of what you can do with just a few lines of code:
"""

from collections import namedtuple
import math
import pandas as pd
import plotly.express as px
import streamlit as st

st.title("ducklit :duck:")
st.write("Query your files with DuckDB")
st.divider()
st.subheader("New subheader")
st.subheader("New subheader 1")


with st.echo(code_location="below"):
    total_points = st.slider("Number of points in spiral", 1, 5000, 2000)
    num_turns = st.slider("Number of turns in spiral", 1, 100, 9)

    Point = namedtuple("Point", "x y")
    data = []

    points_per_turn = total_points / num_turns

    for curr_point_num in range(total_points):
        curr_turn, i = divmod(curr_point_num, points_per_turn)
        angle = (curr_turn + 1) * 2 * math.pi * i / points_per_turn
        radius = curr_point_num / total_points
        x = radius * math.cos(angle)
        y = radius * math.sin(angle)
        data.append(Point(x, y))

df_machine_data_1 = pd.read_parquet("dataset/machine_data_1.parquet")
st.dataframe(df_machine_data_1.head())
df_machine_data_2 = pd.read_parquet("dataset/machine_data_2.parquet")
st.dataframe(df_machine_data_2.head())
df_machine_num_treatments = df_machine_data_1.groupby("machine_id").agg(
    {"num_treatments": "sum"}
)
st.dataframe(df_machine_num_treatments.head())

bar_chart = px.bar(
    df_machine_num_treatments,
    # x="machine_id",
    # y="num_treatments",
)
st.plotly_chart(bar_chart, use_container_width=True)
st.bar_chart(df_machine_num_treatments, use_container_width=True)

option = st.selectbox(
    "Select a machine",
    df_machine_data_1["machine_id"].unique(),
    key="option",
)
st.write(option)
