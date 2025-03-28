import streamlit as st
import pandas as pd
from database_optimized import StockDatabase
from data_fetcher import DataFetcher
from screening_optimized import StockScreener
from visualize import plot_top_5_stocks, plot_breakout_stocks
import time
import logging
from typing import List, Dict, Optional

# 配置
DB_PATH = "stocks_optimized.db"
ALPHA_VANTAGE_KEY = st.secrets.get("ALPHA_VANTAGE_KEY", None)

# 初始化組件
@st.cache_resource
def init_components():
    db = StockDatabase(DB_PATH)
    fetcher = DataFetcher(ALPHA_VANTAGE_KEY)
    screener = StockScreener(db)
    return db, fetcher, screener

db, fetcher, screener = init_components()

# 頁面佈局
st.title("優化版 Qullamaggie Breakout Screener")

# 數據更新面板
with st.expander("數據更新設置"):
    col1, col2 = st.columns(2)
    with col1:
        update_all = st.button("更新所有股票數據")
    with col2:
        update_selected = st.button("更新選中股票數據")
    
    selected_tickers = st.multiselect(
        "選擇要更新的股票",
        options=st.session_state.get('tickers', []),
        key='update_tickers'
    )

# 篩選參數
with st.sidebar.form("screening_params"):
    st.header("篩選參數")
    index_option = st.selectbox(
        "股票池", 
        ["NASDAQ 100", "S&P 500", "自定義"],
        key='index_option'
    )
    prior_days = st.slider(
        "前段上升天數", 10, 30, 20,
        key='prior_days'
    )
    consol_days = st.slider(
        "盤整天數", 5, 15, 10,
        key='consol_days'
    )
    min_rise_22 = st.slider(
        "22 日內最小漲幅 (%)", 0, 50, 10,
        key='min_rise_22'
    )
    min_rise_67 = st.slider(
        "67 日內最小漲幅 (%)", 0, 100, 40,
        key='min_rise_67'
    )
    max_range = st.slider(
        "最大盤整範圍 (%)", 3, 15, 10,
        key='max_range'
    )
    min_adr = st.slider(
        "最小 ADR (%)", 0, 10, 2,
        key='min_adr'
    )
    submit = st.form_submit_button("運行篩選")

# 數據更新邏輯
def update_tickers(tickers: List[str]):
    """更新指定股票的數據"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(tickers):
        status_text.text(f"正在更新 {ticker} ({i+1}/{len(tickers)})...")
        progress_bar.progress((i + 1) / len(tickers))
        
        # 獲取最後更新日期
        last_updated = db.get_ticker_last_updated(ticker)
        start_date = (
            (last_updated + timedelta(days=1)).strftime('%Y-%m-%d') 
            if last_updated else None
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 從API獲取數據
        data, source = fetcher.fetch_data(ticker, start_date, end_date)
        if data is not None:
            db.update_ticker_data(ticker, data, source)
            time.sleep(0.5)  # 避免觸發API限制
    
    status_text.success("更新完成！")
    progress_bar.empty()

if update_all and 'tickers' in st.session_state:
    update_tickers(st.session_state['tickers'])

if update_selected and selected_tickers:
    update_tickers(selected_tickers)

# 篩選邏輯
if submit:
    with st.spinner("篩選中..."):
        # 根據選擇的股票池獲取股票列表
        if st.session_state.index_option == "NASDAQ 100":
            tickers = get_nasdaq_100()
        elif st.session_state.index_option == "S&P 500":
            tickers = get_sp500()
        else:
            tickers = st.session_state.get('custom_tickers', [])
        
        st.session_state['tickers'] = tickers
        
        # 運行篩選
        results = screener.screen_stocks(
            tickers,
            prior_days=st.session_state.prior_days,
            consol_days=st.session_state.consol_days,
            min_rise_22=st.session_state.min_rise_22,
            min_rise_67=st.session_state.min_rise_67,
            max_range=st.session_state.max_range,
            min_adr=st.session_state.min_adr
        )
        
        if not results.empty:
            st.session_state['results'] = results
            st.success(f"找到 {len(results)} 個符合條件的記錄")
        else:
            st.warning("未找到符合條件的股票")

# 顯示結果
if 'results' in st.session_state:
    results = st.session_state['results']
    
    # 顯示最新結果
    latest_results = results[results['Date'] == results['Date'].max()]
    st.dataframe(latest_results)
    
    # 繪製圖表
    top_tickers = (
        latest_results
        .sort_values('Prior_Rise_22_%', ascending=False)
        ['Ticker']
        .unique()[:5]
    )
    
    if top_tickers.size > 0:
        st.subheader("Top 5 股票走勢")
        plot_top_5_stocks(top_tickers)
    
    # 顯示突破股票
    breakout_stocks = latest_results[
        latest_results['Breakout'] & 
        latest_results['Breakout_Volume']
    ]
    
    if not breakout_stocks.empty:
        st.subheader("突破股票")
        plot_breakout_stocks(
            breakout_stocks['Ticker'].unique(),
            st.session_state.consol_days
        )

# 顯示數據源統計
st.sidebar.markdown("---")
st.sidebar.subheader("數據源統計")
source_stats = fetcher.get_source_stats()
for source, count in source_stats.items():
    st.sidebar.text(f"{source}: {count} 次")
