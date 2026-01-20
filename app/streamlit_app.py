# import streamlit as st
# import sqlite3
# import pandas as pd

# DB_PATH = "data/cleaned/f1.db"

# @st.cache_data
# def load_data():
#     conn = sqlite3.connect(DB_PATH)
#     df = pd.read_sql("SELECT * FROM season_results", conn)
#     conn.close()
#     return df

# st.set_page_config(page_title="Lift & Coast", layout="wide")
# st.title("🏁 Lift & Coast: F1 Season Overview")

# df = load_data()

# st.write("Dataset preview:")
# st.dataframe(df.head())


# driver = st.selectbox(
#     "Select a driver",
#     sorted(df["driver"].unique())
# )

# driver_df = df[df["driver"] == driver].sort_values("round")

# st.subheader(f"{driver} — Points by Race")
# st.line_chart(driver_df.set_index("round")["points"])




















import sqlite3
import pandas as pd

conn = sqlite3.connect("data/cleaned/f1.db")

df = pd.read_sql(
    "SELECT season, COUNT(*) AS rows FROM season_results GROUP BY season",
    conn
)

conn.close()
df
