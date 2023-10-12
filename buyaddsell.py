import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np

import Database.db_config as db_config
from custom_indicators import *
from tradingview_ta import TA_Handler, Interval, Exchange


def cal_indicators(data, index_data, volume_period=50, price_period = 15, TF='daily'):
    pd.options.mode.chained_assignment = None

    data["AVG_VOL"] = data.Volume.rolling(window = volume_period, min_periods = 1).mean()
    data["HI_CLOSE"] = data.Close.rolling(window = price_period, min_periods = 1).max() * 0.95
    data["EMA21"] = ta.ema(data["Close"],21)
    data["RSI"] = ta.rsi(data["Close"],14)
    data["ADX"] = ta.adx(data["High"],data["Low"],data["Close"], 14)["ADX_14"]
    PSAR = ta.psar(data["High"],data["Low"],data["Close"])
    data["PSAR"] = PSAR["PSARl_0.02_0.2"].fillna(PSAR["PSARs_0.02_0.2"])
    data["DC_UP"] = ta.donchian(data["High"],data["Low"])["DCU_20_20"]
    data["ATR"] = ta.atr(data["High"],data["Low"],data["Close"])
    data = data.dropna(subset="ATR")
    data["VStop"] = calculate_vstop(data)
    # data["RS2"] = calculate_relative_strength(data, index_data)
    # data['SUPERTREND'] = ta.supertrend(data['High'], data['Low'], data['Close'], length=14, multiplier=3)[['SUPERT_14_3.0']]
    # data['STOCHRSI'] = ta.stochrsi(data['Close'])['STOCHRSIk_14_14_3_3']

    return data


def find_buy_zone():
    conn = db_config.create_connection(db_config.DB_FILE)
    all_stocks = pd.read_sql('select id,symbol from stock', conn)
    index_data = yf.download("^NSEI")
    # print(index_data)
    results = []
    results_good_rr = []

    for st_key, st_val in all_stocks.iterrows():
        try:
            data = pd.read_sql(f"""select date(date) as Date,open as Open,high as High,low as Low,close as Close,volume as Volume 
                            from stock_price where stock_id = {st_val['id']} and date(date) >'2023-01-01';""", conn)
            data['Date'] = pd.to_datetime(data['Date'])
            data['RS'] = calculate_relative_strength(data, index_data)
            data = cal_indicators(data, index_data)[-2:]
            # print(data[[ 'Close', 'EMA21', 'RSI', 'ADX', 'Volume', 'AVG_VOL', 'RS', 'HI_CLOSE']])
            today_condition = ((data[-1:]['RSI'].values[0] > 55) & (data[-1:]['EMA21'].values[0] < data[-1:]['Close'].values[0]) 
                & (data[-1:]['ADX'].values[0] > 25) & ( data[-1:]['Volume'].values[0] > 1.5 * data[-1:]['AVG_VOL'].values[0]) & 
                ( data[-1:]['Close'].values[0] > data[-1:]['HI_CLOSE'].values[0]) & (data[-1:]['RS'].values[0] > 0 ))
            
            yesterday_condition = ((data[-2:]['RSI'].values[0] < 55) | (data[-2:]['EMA21'].values[0] > data[-2:]['Close'].values[0]) 
                | (data[-2:]['ADX'].values[0] < 25) | (data[-2:]['RS'].values[0] < 0 ))
            
            if (today_condition):
                symbol = TA_Handler(
                    symbol=st_val['symbol'],
                    screener="india",
                    exchange="NSE",
                    interval=Interval.INTERVAL_1_DAY,
                )
                summary = symbol.get_analysis().summary
                indicators = symbol.get_analysis().indicators       
                if ((data[-1:]['Close'].values[0] - indicators['Pivot.M.Fibonacci.S1'])/data[-1:]['Close'].values[0]) * 100 < 9 :
                    results_good_rr.append({'symbol': st_val['symbol'], 'summary': summary})

            if (today_condition & yesterday_condition):
                results.append(f"Buy generated for {st_val['symbol']}")
        except:
            print(f"Failed for stock {st_val['symbol']}")
    
    print (results)

    print (results_good_rr)
    conn.commit()
    conn.close()


def find_sell_zone(watchlist, sell_strategy="VStop"):
    conn = db_config.create_connection(db_config.DB_FILE)
    all_stocks = pd.read_sql('select id,symbol from stock', conn)
    index_data = yf.download("^NSEI")

    for stock in watchlist:
        id = all_stocks[ all_stocks.symbol == stock ]['id'].values[0]
        data = pd.read_sql(f"""select date(date) as Date,open as Open,high as High,low as Low,close as Close,volume as Volume 
                           from stock_price where stock_id = {id} and date(date) >'2023-01-01';""", conn)
        data['Date'] = pd.to_datetime(data['Date'])
        data['RS'] = calculate_relative_strength(data, index_data)
        data = cal_indicators(data, index_data)
        
        for key, value in data[-1:].iterrows():
            if ((value[sell_strategy] > value['Close']) & ((value['RSI'] < 55) | (value['EMA21'] > value['Close']) |
                            (value['ADX'] < 25) | (value['RS'] < 0 ))):
                print (f"Sell generated for {stock}")

    conn.commit()
    conn.close()

def find_add_zone(watchlist):
    conn = db_config.create_connection(db_config.DB_FILE)
    all_stocks = pd.read_sql('select id,symbol from stock', conn)

    for stock in watchlist:
        id = all_stocks[ all_stocks.symbol == stock ]['id'].values[0]
        data = pd.read_sql(f"""select date(date) as Date,open as Open,high as High,low as Low,close as Close,volume as Volume 
                           from stock_price where stock_id = {id} and date(date) >'2023-01-01';""", conn)
        data['Date'] = pd.to_datetime(data['Date'])
        data["AVG_VOL"] = data.Volume.rolling(window = 50, min_periods = 1).mean()
        data["DC_UP"] = ta.donchian(data["High"],data["Low"])["DCU_20_20"]

        for key, value in data[-1:].iterrows():
            threshold = 0.5
            if (value['Volume'] > 2 * value['AVG_VOL']):
                threshold = 2
            if ((abs(((value['DC_UP'] - value['Close'])/value['Close'])) * 100 < threshold) ):
                print(f"Add Signal generated for {stock}")

    conn.commit()
    conn.close()    

def main():
    conn = db_config.create_connection(db_config.DB_FILE)
    journal = pd.read_sql('select symbol from stock where id in (select distinct stock_id from journal where id not in ( select id from journal where sell_price > 0));', conn)

    watchlist = [symbol for symbol in journal['symbol']]
    
    print(watchlist)
    find_sell_zone(watchlist)
    find_add_zone(watchlist)
    find_buy_zone()


if __name__ == "__main__":
    main()
