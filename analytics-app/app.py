import os

import pandas as pd
import psycopg2
import streamlit as st

DB_URL = os.getenv("BOOKOPS_DB_URL", "postgresql://airflow:airflow@localhost:5432/bookops")

st.set_page_config(page_title="BookShop Dashboard", layout="wide")
st.title("BookShop Pipeline Dashboard")


@st.cache_data(ttl=30)
def query(sql):
    conn = psycopg2.connect(DB_URL)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


tab1, tab2, tab3, tab4 = st.tabs(["Books Catalog", "Raw Sales", "Daily Sales", "Genre Report"])

with tab1:
    st.header("Books Catalog")
    try:
        df = query("SELECT COUNT(*) AS total FROM books")
        st.metric("Total Books", int(df["total"].iloc[0]))

        df = query("SELECT isbn, title, author, genre, price FROM books ORDER BY genre, title")
        st.dataframe(df, use_container_width=True)

        df = query("SELECT genre, COUNT(*) AS count FROM books GROUP BY genre ORDER BY count DESC")
        st.bar_chart(df.set_index("genre"))
    except Exception as e:
        st.warning(f"Run DAG 01 first. ({e})")

with tab2:
    st.header("Raw Sales")
    try:
        totals = query("SELECT COUNT(*) AS sales, SUM(total) AS revenue FROM raw_sales")

        col1, col2 = st.columns(2)
        col1.metric("Total Transactions", int(totals["sales"].iloc[0]))
        col2.metric("Total Revenue", f"${totals['revenue'].iloc[0]:,.2f}")

        st.subheader("Sales by Date")
        df = query("""
            SELECT sale_date, SUM(quantity) AS units, SUM(total) AS revenue
            FROM raw_sales GROUP BY sale_date ORDER BY sale_date
        """)
        st.line_chart(df.set_index("sale_date")[["units", "revenue"]])

        st.subheader("All Records")
        df = query("SELECT sale_id, isbn, sale_date, quantity, total FROM raw_sales ORDER BY sale_date DESC")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning(f"Run DAG 02 first. ({e})")

with tab3:
    st.header("Daily Sales")
    try:
        totals = query("SELECT COUNT(*) AS sales, SUM(total) AS revenue FROM daily_sales")
        quarantine_count = query("SELECT COUNT(*) AS bad FROM sales_quarantine")

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Transactions", int(totals["sales"].iloc[0]))
        col2.metric("Total Revenue", f"${totals['revenue'].iloc[0]:,.2f}")
        col3.metric("Quarantined Records", int(quarantine_count["bad"].iloc[0]))

        st.subheader("Sales by Date")
        df = query("""
            SELECT sale_date, SUM(quantity) AS units, SUM(total) AS revenue
            FROM daily_sales GROUP BY sale_date ORDER BY sale_date
        """)
        st.line_chart(df.set_index("sale_date")[["units", "revenue"]])

        st.subheader("Quarantine Log")
        df = query("SELECT raw, reason, quarantined_at FROM sales_quarantine ORDER BY quarantined_at DESC LIMIT 50")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning(f"Run DAGs 02 and 03 first. ({e})")

with tab4:
    st.header("Genre Report")
    try:
        df = query("""
            SELECT report_date, genre, books_sold, revenue
            FROM daily_report ORDER BY report_date DESC, revenue DESC
        """)
        st.dataframe(df, use_container_width=True)

        latest = query("""
            SELECT genre, SUM(revenue) AS revenue
            FROM daily_report
            WHERE report_date = (SELECT MAX(report_date) FROM daily_report)
            GROUP BY genre ORDER BY revenue DESC
        """)
        st.subheader("Revenue by Genre (Latest Day)")
        st.bar_chart(latest.set_index("genre"))
    except Exception as e:
        st.warning(f"Run DAG 04 first. ({e})")
