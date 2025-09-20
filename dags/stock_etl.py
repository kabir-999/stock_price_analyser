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
        try:
            file_path = os.path.join(DATA_DIR, f"raw_{ticker}_data.csv")
            print(f"[Transform] Processing {ticker} from {file_path}")
            
            # Read CSV and ensure proper data types
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            
            # Convert all numeric columns to float
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
            
            # Ensure 'Adj Close' column exists before transformations
            if "Adj Close" not in df.columns:
                if "Close" in df.columns:
                    df["Adj Close"] = df["Close"]
                    print(f"[Transform] Using 'Close' as 'Adj Close' for {ticker}")
                else:
                    raise KeyError(f"'Adj Close' or 'Close' column not found for {ticker}")
            
            # Ensure we have enough data points
            if len(df) < 5:
                print(f"[Transform] Warning: Not enough data points for {ticker}, skipping...")
                continue

            print(f"[Transform] Calculating indicators for {ticker}")
            
            # Basic Transformations
            df["Daily_Return"] = df["Adj Close"].pct_change()
            df["MA_5"] = df["Adj Close"].rolling(window=min(5, len(df))).mean()
            df["MA_20"] = df["Adj Close"].rolling(window=min(20, len(df))).mean()

            # Advanced Transformations: RSI
            delta = df["Adj Close"].diff(1)
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            
            # Use a smaller window if not enough data
            window = min(14, len(df) - 1)
            if window < 2:  # Need at least 2 points for RSI
                print(f"[Transform] Not enough data points for RSI calculation for {ticker}")
                df["RSI"] = None
            else:
                avg_gain = gain.rolling(window=window).mean()
                avg_loss = loss.rolling(window=window).mean()
                rs = avg_gain / avg_loss.replace(0, float('inf'))  # Avoid division by zero
                df["RSI"] = 100 - (100 / (1 + rs))

            # Advanced Transformations: MACD
            exp1 = df["Adj Close"].ewm(span=min(12, len(df)), adjust=False).mean()
            exp2 = df["Adj Close"].ewm(span=min(26, len(df)), adjust=False).mean()
            df["MACD"] = exp1 - exp2
            df["Signal_Line"] = df["MACD"].ewm(span=min(9, len(df)), adjust=False).mean()

            # Save the transformed data
            output_path = os.path.join(DATA_DIR, f"transformed_{ticker}_data.csv")
            df.to_csv(output_path, float_format='%.4f')
            print(f"[Transform] Successfully processed {ticker}. Saved to {output_path}")
            print(f"[Transform] Data shape: {df.shape}, Columns: {', '.join(df.columns)}")
            
        except Exception as e:
            print(f"[Transform] Error processing {ticker}: {str(e)}")
            raise

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

    def check_trading_signals(**kwargs):
        """
        Check trading signals based on technical indicators and send alerts.
        """
        ti = kwargs["ti"]
        tickers = ti.xcom_pull(task_ids="extract_stock_data")
        
        alerts = []
        
        for ticker in tickers:
            try:
                file_path = os.path.join(DATA_DIR, f"transformed_{ticker}_data.csv")
                df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                
                # Get the latest data point
                latest = df.iloc[-1]
                
                # RSI Conditions
                if 'RSI' in df.columns and pd.notna(latest['RSI']):
                    rsi = float(latest['RSI'])
                    if rsi > 70:
                        alerts.append(f"🚨 {ticker}: RSI {rsi:.2f} - Overbought condition!")
                    elif rsi < 30:
                        alerts.append(f"📉 {ticker}: RSI {rsi:.2f} - Oversold condition!")
                
                # MACD Crossover
                if 'MACD' in df.columns and 'Signal_Line' in df.columns:
                    if len(df) >= 2:
                        prev_macd = df['MACD'].iloc[-2]
                        curr_macd = df['MACD'].iloc[-1]
                        prev_signal = df['Signal_Line'].iloc[-2]
                        curr_signal = df['Signal_Line'].iloc[-1]
                        
                        # Bullish crossover
                        if prev_macd < prev_signal and curr_macd > curr_signal:
                            alerts.append(f"📈 {ticker}: Bullish MACD Crossover detected!")
                        # Bearish crossover
                        elif prev_macd > prev_signal and curr_macd < curr_signal:
                            alerts.append(f"📉 {ticker}: Bearish MACD Crossover detected!")
                
                # Price crossing moving averages
                if 'Close' in df.columns and 'MA_5' in df.columns and 'MA_20' in df.columns:
                    if len(df) >= 2:
                        prev_close = df['Close'].iloc[-2]
                        curr_close = df['Close'].iloc[-1]
                        prev_ma5 = df['MA_5'].iloc[-2]
                        curr_ma5 = df['MA_5'].iloc[-1]
                        
                        # Price crossing above MA5
                        if prev_close < prev_ma5 and curr_close > curr_ma5:
                            alerts.append(f"🟢 {ticker}: Price crossed above 5-day MA")
                        # Price crossing below MA5
                        elif prev_close > prev_ma5 and curr_close < curr_ma5:
                            alerts.append(f"🔴 {ticker}: Price crossed below 5-day MA")
                
            except Exception as e:
                print(f"Error processing alerts for {ticker}: {str(e)}")
        
        # Print alerts to logs
        if alerts:
            print("\n" + "="*50)
            print("📊 TRADING ALERTS 📊")
            print("="*50)
            for alert in alerts:
                print(alert)
                
            # In a production environment, you would add code here to send emails/slack/etc.
            # Example for email (uncomment and configure in Airflow):
            # from airflow.utils.email import send_email
            # send_email(
            #     to="your_email@example.com",
            #     subject=f"Trading Alerts - {datetime.now().strftime('%Y-%m-%d')}",
            #     html_content="<br>\n".join(alerts)
            # )
            print("="*50 + "\n")
    
    # Add the alert task to the DAG
    alert_task = PythonOperator(
        task_id="check_trading_signals",
        python_callable=check_trading_signals,
    )

    extract_task >> transform_task >> alert_task
