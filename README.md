# Stock Market ETL Pipeline

A robust data pipeline that extracts, transforms, and analyzes stock market data using Apache Airflow. This project fetches real-time stock data, processes it to generate technical indicators, and provides trading alerts based on market conditions.

##Features

### 1. Data Extraction
- **Multi-Stock Support**: Fetches data for multiple tickers (AAPL, MSFT, TSLA by default)
- **Automatic Updates**: Scheduled to run daily for the latest market data
- **Data Persistence**: Stores raw historical data in CSV format for analysis

### 2. Technical Indicators
- **Moving Averages**:
  - 5-day Moving Average (MA_5)
  - 20-day Moving Average (MA_20)
- **Momentum Indicators**:
  - Relative Strength Index (RSI)
  - Moving Average Convergence Divergence (MACD)
  - Signal Line (9-day EMA of MACD)
- **Price Action**:
  - Daily Returns
  - Price vs Moving Average Crossovers

### 3. Trading Alerts
Automated detection of key market conditions:

#### RSI-Based Alerts
- Overbought: RSI > 70
- Oversold: RSI < 30

#### MACD Signals
- Bullish: MACD crosses above Signal Line
- Bearish: MACD crosses below Signal Line

#### Moving Average Crossovers
- Buy Signal: Price crosses above 5-day MA
- Sell Signal: Price crosses below 5-day MA

### 4. Data Output
- **Raw Data**: Original market data from Yahoo Finance
- **Processed Data**: Enhanced with technical indicators
- **Structured Format**: CSV files with consistent schema for easy analysis

### 5. Error Handling & Logging
- Comprehensive error handling for data fetching and processing
- Detailed logging of all operations and alerts
- Automatic retry mechanism for failed tasks

## 🔍 Sample Data Structure
Processed data includes the following columns:
- **Basic Data**: Date, Open, High, Low, Close, Volume
- **Calculated Fields**:
  - Daily_Return: Percentage change in closing price
  - MA_5/MA_20: Moving averages
  - RSI: Relative Strength Index
  - MACD/Signal_Line: Trend-following momentum indicators

## 🛠 Technical Implementation
- **Orchestration**: Apache Airflow
- **Data Processing**: Pandas, NumPy
- **Data Source**: Yahoo Finance API (via yfinance)
- **Scheduling**: Daily execution with configurable intervals

*The pipeline automates data extraction, processing, and alerting on a daily schedule.*

1. **Extract**: Fetch latest stock data
2. **Transform**: Calculate technical indicators
3. **Analyze**: Generate trading signals
4. **Alert**: Notify about significant market conditions

