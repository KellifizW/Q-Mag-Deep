import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

class StockScreener:
    """優化的股票篩選器"""
    
    def __init__(self, db):
        self.db = db
    
    def screen_stocks(
        self,
        tickers: List[str],
        prior_days: int = 20,
        consol_days: int = 10,
        min_rise_22: float = 10.0,
        min_rise_67: float = 40.0,
        max_range: float = 10.0,
        min_adr: float = 2.0,
        max_workers: int = 4
    ) -> pd.DataFrame:
        """多線程篩選股票"""
        # 計算所需數據日期範圍
        end_date = pd.Timestamp.now().strftime('%Y-%m-%d')
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=180)).strftime('%Y-%m-%d')
        
        # 批量獲取數據
        stock_data = self.db.fetch_stock_data(tickers, start_date, end_date)  # 修正為 fetch_stock_data
        
        # 多線程分析
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for ticker, data in stock_data.items():
                futures.append(executor.submit(
                    self._analyze_stock,
                    ticker,
                    data,
                    prior_days,
                    consol_days,
                    min_rise_22,
                    min_rise_67,
                    max_range,
                    min_adr
                ))
            
            results = []
            for future in futures:
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Analysis failed: {str(e)}")
        
        return pd.concat(results) if results else pd.DataFrame()
    
    def _analyze_stock(
        self,
        ticker: str,
        data: pd.DataFrame,
        prior_days: int,
        consol_days: int,
        min_rise_22: float,
        min_rise_67: float,
        max_range: float,
        min_adr: float
    ) -> Optional[pd.DataFrame]:
        """分析單個股票"""
        try:
            if len(data) < (prior_days + consol_days + 30):
                logger.warning(f"Insufficient data for {ticker}")
                return None
            
            close = data['Close']
            volume = data['Volume']
            high = data['High']
            low = data['Low']
            
            # 計算指標
            rise_22 = (close / close.shift(22) - 1) * 100
            rise_67 = (close / close.shift(67) - 1) * 100
            recent_high = close.rolling(consol_days).max()
            recent_low = close.rolling(consol_days).min()
            consolidation_range = (recent_high / recent_low - 1) * 100
            vol_decline = volume.rolling(consol_days).mean() < volume.shift(consol_days).rolling(prior_days).mean()
            daily_range = (high - low) / close.shift(1)
            adr = daily_range.rolling(prior_days).mean() * 100
            breakout = (close > recent_high.shift(1)) & (close.shift(1) <= recent_high.shift(1))
            breakout_volume = volume > volume.rolling(10).mean() * 1.5
            
            # 應用篩選條件
            mask = (
                (rise_22 >= min_rise_22) & 
                (rise_67 >= min_rise_67) & 
                (consolidation_range <= max_range) & 
                (adr >= min_adr)
            )
            
            if not mask.any():
                return None
                
            # 構建結果DataFrame
            matched_dates = data.index[mask]
            result = pd.DataFrame({
                'Ticker': ticker,
                'Date': matched_dates,
                'Price': close[mask],
                'Prior_Rise_22_%': rise_22[mask],
                'Prior_Rise_67_%': rise_67[mask],
                'Consolidation_Range_%': consolidation_range[mask],
                'ADR_%': adr[mask],
                'Breakout': breakout[mask],
                'Breakout_Volume': breakout_volume[mask],
                'Volume': volume[mask]
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing {ticker}: {str(e)}")
            return None
