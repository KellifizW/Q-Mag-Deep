import yfinance as yf
from alpha_vantage.timeseries import TimeSeries
from typing import Dict, Optional, Tuple
import pandas as pd
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)

class DataFetcher:
    """多源股票數據獲取器"""
    
    def __init__(self, alpha_vantage_key: Optional[str] = None):
        self.alpha_vantage_key = alpha_vantage_key
        self.source_priority = ['yfinance', 'alpha_vantage']
        self.source_stats = {source: 0 for source in self.source_priority}
        
    def fetch_data(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str,
        retries: int = 3,
        delay: int = 5
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """從多個數據源獲取股票數據"""
        for attempt in range(retries):
            for source in self.source_priority:
                try:
                    if source == 'yfinance':
                        data = self._fetch_yfinance(ticker, start_date, end_date)
                    elif source == 'alpha_vantage' and self.alpha_vantage_key:
                        data = self._fetch_alpha_vantage(ticker)
                    
                    if data is not None and not data.empty:
                        self.source_stats[source] += 1
                        return data, source
                        
                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt + 1} failed with {source}: {str(e)}"
                    )
                    time.sleep(delay * (attempt + 1))
        
        logger.error(f"All attempts failed for {ticker}")
        return None, "failed"
    
    def _fetch_yfinance(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """從yfinance獲取數據"""
        try:
            data = yf.download(
                ticker, 
                start=start_date, 
                end=end_date,
                progress=False,
                threads=True
            )
            return data if not data.empty else None
        except Exception as e:
            logger.error(f"yfinance failed for {ticker}: {str(e)}")
            return None
    
    def _fetch_alpha_vantage(self, ticker: str) -> Optional[pd.DataFrame]:
        """從Alpha Vantage獲取數據"""
        if not self.alpha_vantage_key:
            return None
            
        try:
            ts = TimeSeries(key=self.alpha_vantage_key, output_format='pandas')
            data, _ = ts.get_daily_adjusted(
                symbol=ticker, 
                outputsize='full'
            )
            data = data.rename(columns={
                '1. open': 'Open',
                '2. high': 'High',
                '3. low': 'Low',
                '4. close': 'Close',
                '5. adjusted close': 'Adj Close',
                '6. volume': 'Volume'
            })
            return data if not data.empty else None
        except Exception as e:
            logger.error(f"Alpha Vantage failed for {ticker}: {str(e)}")
            return None
    
    def get_source_stats(self) -> Dict[str, int]:
        """獲取各數據源使用統計"""
        return self.source_stats
