from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import numpy as np
import os


def send_failure_notification(context):
    """
    Placeholder for sending failure notifications.
    In a real-world scenario, you would integrate with an alerting system (e.g., PagerDuty, Slack, email).
    For email alerts, ensure your Airflow SMTP connection is configured.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    logical_date = context["logical_date"].isoformat()
    # You can retrieve more context information like exception, log links, etc.
    print(f"DAG: {dag_id}, Task: {task_id} failed on {logical_date}")
    # Example for sending email (requires SMTP configuration in Airflow):
    # from airflow.utils.email import send_email
    # subject = f"Airflow Alert: DAG {dag_id} - Task {task_id} Failed"
    # html_content = f"Task {task_id} in DAG {dag_id} failed on {logical_date}.
    #                <br> View logs: {context['task_instance'].log_url}"
    # send_email("your_email@example.com", subject, html_content)


TICKERS = ["AAPL", "MSFT", "TSLA"]  # add more if needed

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
os.makedirs(DATA_DIR, exist_ok=True)

# -----------------------------
# ETL Functions
# -----------------------------

# 1. Extract
def extract_stock_data(**kwargs):
    all_data = {}
    for ticker in TICKERS:
        df = yf.download(ticker, period="5d", interval="1d")
        print(f"Columns for {ticker}: {df.columns}")
        all_data[ticker] = df
        file_path = os.path.join(DATA_DIR, f"raw_{ticker}_data.csv")
        df.to_csv(file_path)
        print(f"[Extract] {ticker} data saved to {file_path}")

    return list(all_data.keys())  # push tickers to XCom

# 2. Transform
def transform_stock_data(**kwargs):
    ti = kwargs["ti"]
    tickers = ti.xcom_pull(task_ids="extract_stock_data")

    for ticker in tickers:
        file_path = os.path.join(DATA_DIR, f"raw_{ticker}_data.csv")
        df = pd.read_csv(file_path, index_col=0, parse_dates=True)

        # Ensure 'Adj Close' column exists before transformations
        if "Adj Close" not in df.columns:
            # Attempt to use 'Close' if 'Adj Close' is not available
            if "Close" in df.columns:
                df["Adj Close"] = df["Close"]
            else:
                raise KeyError(f"'Adj Close' or 'Close' column not found for {ticker}")

        # Basic Transformations
        df["Daily Return"] = df["Adj Close"].pct_change()
        df["MA_5"] = df["Adj Close"].rolling(window=5).mean()
        df["MA_20"] = df["Adj Close"].rolling(window=20).mean()

        # Advanced Transformations: RSI
        delta = df["Adj Close"].diff(1)
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df["RSI"] = 100 - (100 / (1 + rs))

        # Advanced Transformations: MACD
        exp1 = df["Adj Close"].ewm(span=12, adjust=False).mean()
        exp2 = df["Adj Close"].ewm(span=26, adjust=False).mean()
        df["MACD"] = exp1 - exp2
        df["Signal Line"] = df["MACD"].ewm(span=9, adjust=False).mean()

        output_path = os.path.join(DATA_DIR, f"transformed_{ticker}_data.csv")
        df.to_csv(output_path)
        print(f"[Transform] {ticker} transformed data saved to {output_path}")

# -----------------------------
# Default DAG args
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 5),  # should always be in the past
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_failure_notification,
}

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="stock_etl_dag",
    default_args=default_args,
    description="Stock Market ETL DAG (Extract from Yahoo Finance → Transform → Save CSV)",
    schedule_interval="0 22 * * *",  # daily at 10 PM UTC
    catchup=False,
    tags=["stock", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_stock_data",
        python_callable=extract_stock_data,
    )

    transform_task = PythonOperator(
        task_id="transform_stock_data",
        python_callable=transform_stock_data,
    )

    extract_task >> transform_task
