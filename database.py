import sqlite3
import pandas as pd
import numpy as np
from functools import lru_cache
from datetime import datetime, timedelta
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from typing import List, Dict, Optional, Tuple

# 配置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class StockDatabase:
    """優化的股票數據庫類"""
    
    def __init__(self, db_path: str = "stocks_optimized.db"):
        self.db_path = db_path
        self._init_db()
        self.session = self._create_session()
        self.lock = threading.Lock()
        
    def _create_session(self):
        """創建帶重試機制的requests session"""
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=20,
            pool_maxsize=20
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
        
    def _init_db(self):
        """初始化數據庫結構"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # 創建主表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stocks (
                    Date TEXT, 
                    Ticker TEXT, 
                    Open REAL, 
                    High REAL, 
                    Low REAL, 
                    Close REAL, 
                    Adj_Close REAL, 
                    Volume INTEGER,
                    MA10 REAL,
                    EMA12 REAL,
                    EMA26 REAL,
                    MACD REAL,
                    MACD_Signal REAL,
                    MACD_Hist REAL,
                    PRIMARY KEY (Date, Ticker)
                )
            ''')
            # 創建元數據表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    ticker TEXT PRIMARY KEY,
                    last_updated TEXT,
                    data_source TEXT
                )
            ''')
            # 創建索引
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_ticker_date 
                ON stocks (Ticker, Date)
            ''')
            conn.commit()
    
    @lru_cache(maxsize=1000)
    def get_ticker_last_updated(self, ticker: str) -> Optional[datetime]:
        """獲取股票最後更新時間（帶緩存）"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_updated FROM metadata WHERE ticker = ?",
                (ticker,)
            )
            result = cursor.fetchone()
            return datetime.strptime(result[0], '%Y-%m-%d') if result else None
    
    def update_ticker_data(
        self, 
        ticker: str, 
        data: pd.DataFrame,
        data_source: str = 'yfinance'
    ) -> bool:
        """更新單個股票數據（增量更新）"""
        if data.empty:
            return False
            
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 計算技術指標
            data['MA10'] = data['Close'].rolling(10).mean()
            data['EMA12'] = data['Close'].ewm(span=12, adjust=False).mean()
            data['EMA26'] = data['Close'].ewm(span=26, adjust=False).mean()
            data['MACD'] = data['EMA12'] - data['EMA26']
            data['MACD_Signal'] = data['MACD'].ewm(span=9, adjust=False).mean()
            data['MACD_Hist'] = data['MACD'] - data['MACD_Signal']
            
            # 轉換為適合插入的格式
            records = [
                (
                    date.strftime('%Y-%m-%d'), ticker,
                    row['Open'], row['High'], row['Low'], 
                    row['Close'], row['Adj Close'], row['Volume'],
                    row['MA10'], row['EMA12'], row['EMA26'],
                    row['MACD'], row['MACD_Signal'], row['MACD_Hist']
                )
                for date, row in data.iterrows()
            ]
            
            # 批量插入
            cursor.executemany('''
                INSERT OR REPLACE INTO stocks 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', records)
            
            # 更新元數據
            last_date = data.index[-1].strftime('%Y-%m-%d')
            cursor.execute('''
                INSERT OR REPLACE INTO metadata 
                VALUES (?, ?, ?)
            ''', (ticker, last_date, data_source))
            
            conn.commit()
            return True
    
    def fetch_stock_data(
        self, 
        tickers: List[str], 
        start_date: str, 
        end_date: str
    ) -> Dict[str, pd.DataFrame]:
        """從數據庫獲取股票數據"""
        result = {}
        with sqlite3.connect(self.db_path) as conn:
            for ticker in tickers:
                query = '''
                    SELECT * FROM stocks 
                    WHERE Ticker = ? AND Date BETWEEN ? AND ?
                    ORDER BY Date
                '''
                df = pd.read_sql_query(
                    query, conn, 
                    params=(ticker, start_date, end_date),
                    parse_dates=['Date'],
                    index_col='Date'
                )
                if not df.empty:
                    result[ticker] = df
        return result
