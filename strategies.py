import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np

from custom_indicators import *

def cal_add_buy_sell(data, data_point=0):
    data['Buy'] = data.Buy.diff()
    data['Sell'] = data.Sell.diff()
    if data_point > 0:
        df_pos = data.iloc[-data_point:][(data.iloc[-data_point:]['Buy'] == 1) | (data.iloc[-data_point:]['Add'] == 1) | (data.iloc[-data_point:]['Sell'] == 1)].copy()
    else:
        df_pos = data[(data['Buy'] == 1) | (data['Add'] == 1) | (data['Sell'] == 1)]
    Buy = 0
    Sell = 0
    Add = 0
    df_final = pd.DataFrame()
    
    for idx in df_pos.index:
        if (df_pos['Buy'][idx] == 1) & (Buy == 0):
            Buy += 1
            Sell = 0
            df_final = pd.concat([df_final,df_pos[df_pos.index == idx]])
            df_final['Position'][idx] = 'Buy'        
        elif (df_pos['Add'][idx] == 1) & (Buy == 1) & (Add < 4):
            df_final = pd.concat([df_final,df_pos[df_pos.index == idx]])
            df_final['Position'][idx] = 'Add'
            Add += 1
        elif (df_pos['Sell'][idx] == 1) & (Buy == 1) & (Sell ==0):
            Buy = 0
            Add = 0
            Sell += 1
            df_final = pd.concat([df_final,df_pos[df_pos.index == idx]])
            df_final['Position'][idx] = 'Sell'
    
    return df_final

def cal_indicators(name='ITC', volume_period=50, price_period = 15, TF='daily'):
    data_daily = yf.download(f"{name}.NS")
    index_data = yf.download("^NSEI")

    if TF == 'daily':
        data = data_daily
    elif TF == 'weekly':
        data = data_daily.resample('W').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
    elif TF == 'monthly':
        data = data_daily.resample('M').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
    
    data["AVG_VOL"] = data.Volume.rolling(window = volume_period, min_periods = 1).mean()
    data["HI_CLOSE"] = data.Close.rolling(window = price_period, min_periods = 1).max() * 0.95
    data["G_PRICE"] = data['High'] - (data['High'] - data['Low']) * 0.25
    data["EMA21"] = ta.ema(data["Close"],21)
    data["RSI"] = ta.rsi(data["Close"],14)
    data["ADX"] = ta.adx(data["High"],data["Low"],data["Close"], 14)["ADX_14"]
    PSAR = ta.psar(data["High"],data["Low"],data["Close"])
    data["PSAR"] = PSAR["PSARl_0.02_0.2"].fillna(PSAR["PSARs_0.02_0.2"])
    data["DC_UP"] = ta.donchian(data["High"],data["Low"])["DCU_20_20"]
    data["ATR"] = ta.atr(data["High"],data["Low"],data["Close"])
    data = data.dropna(subset="ATR")
    data["VStop"] = calculate_vstop(data)
    data["RS"] = calculate_relative_strength(data, index_data)
    data['SUPERTREND'] = ta.supertrend(data['High'], data['Low'], data['Close'], length=14, multiplier=3)[['SUPERT_14_3.0']]
    data['STOCHRSI'] = ta.stochrsi(data['Close'])['STOCHRSIk_14_14_3_3']

    
    data['Buy'] = 0
    data['Add'] = 0
    data['Sell'] = 0
    data['Position'] = 'NA'

    return data


def my_strategy(name='ITC', sell_strategy='VStop', data_point=0, volume_period=50, price_period = 15, TF='daily' ):
    pd.options.mode.chained_assignment = None
    data = cal_indicators(name=name, volume_period=50, price_period = 15, TF='daily')

    data['Buy'] = np.where((data['RSI'] > 55) & (data['EMA21'] < data['Close']) & (data['ADX'] > 25) & 
                           ( data['Volume'] > 1.5 * data['AVG_VOL']) & ( data['Close'] > data['HI_CLOSE']) , 1, 0)
    data['Add'] = np.where(((data['DC_UP'] - data['Close'])/data['Close']).abs() * 100 < 0.5, 1, 0)
    data['Sell'] = np.where((data[sell_strategy] > data['Close']) & ((data['RSI'] < 55) | (data['EMA21'] > data['Close']) | 
                            (data['ADX'] < 25)), 1, 0)
    
    # print(tabulate(df_final[['Close','Position']], headers = 'keys', tablefmt = 'psql'))
    return cal_add_buy_sell(data, data_point)

def supertrend_stocRSI(name='ITC', data_point=0):
    pd.options.mode.chained_assignment = None
    data = cal_indicators(name=name, volume_period=50, price_period = 15, TF='daily')

    Buy = 0
    Sell = 0
    Add = 0
    SL = 0
    for key, value in data.iterrows():
        if Buy == 0:
            if ((value['STOCHRSI'] < 20) & (value['Close'] > value['SUPERTREND']) & (value['RS'] > 0 )
                & (value['Close'] > value['G_PRICE'])):
                # print(key, data[ data.index == key ])
                data.at[ key, 'Buy'] = 1
                SL = value['Close'] - 2 * value['ATR']
                Buy = 1
                Sell = 0
                Add = 0
                # print("In buy", key, Add)
        elif ((Buy == 1) & (Sell == 0)):
            threshold = 0.5
            if (value['Volume'] > 2 * value['AVG_VOL']):
                threshold = 2
            if ((abs(((value['DC_UP'] - value['Close'])/value['Close'])) * 100 < threshold) & (Add < 4)):
                data.at[ key, 'Add'] = 1
                Add += 1
                SL = value['Close'] - 2 * value['ATR']            
                # print("In Add", key, Add, data.at[ key, 'Add'])      

            if (((value['STOCHRSI'] > 80) & (value['Close'] < value['SUPERTREND']) & (value['RS'] < 0 )) | (value['Close'] < SL)):
                data.at[ key, 'Sell'] = 1
                Sell = 1
                Buy = 0    
                Add = 0
                # print("In Sell", key, Add)              
    # print (data[ data.index.strftime('%Y-%m-%d') == '2022-11-29'][['Close', 'ATR', 'DC_UP', 'SUPERTREND', 'STOCHRSI', 'Buy', 'Add', 'Sell']])
    
    return cal_add_buy_sell(data, data_point)

def my_strategy_withSL(name='ITC', sell_strategy='VStop', data_point=0, volume_period=50, price_period = 15, TF='daily'):
    data = cal_indicators(name=name, volume_period=50, price_period = 15, TF='daily')

    Buy = 0
    Buy_Price = 0
    Atr = 0
    Sell = 0
    Add = 0

    for key, value in data.iterrows():
        if Buy == 0:
            if ((value['RSI'] > 55) & (value['EMA21'] < value['Close']) & (value['ADX'] > 25) & 
                           ( value['Volume'] > 1.5 * value['AVG_VOL']) & ( value['Close'] > value['HI_CLOSE']) & 
                           (value['RS'] > 0 ) & (value['Close'] > value['G_PRICE'])):
                # print(key, data[ data.index == key ])
                data.at[ key, 'Buy'] = 1
                Atr = value['ATR']
                Buy_Price = value['Close']
                Buy = 1
                Sell = 0
                Add = 0
        elif ((Buy == 1) & (Sell == 0)):
            threshold = 0.5
            if (value['Volume'] > 2 * value['AVG_VOL']):
                threshold = 2
            if ((abs(((value['DC_UP'] - value['Close'])/value['Close'])) * 100 < threshold) & (Add < 4)):
                data.at[ key, 'Add'] = 1
                Add += 1
                SL = value['Close'] - 2 * value['ATR']                    
            if ((((value[sell_strategy] > value['Close']) & ((value['RSI'] < 55) | (value['EMA21'] > value['Close']) | 
                            (value['ADX'] < 25) | (value['RS'] < 0 )))) | (value['Close'] < Buy_Price - 2 * Atr)):
                data.at[ key, 'Sell'] = 1
                Sell = 1
                Buy = 0
                Add = 0 
    return cal_add_buy_sell(data, data_point)                
            
def main():
    data = supertrend_stocRSI(name="ZENSARTECH")
    print (data[['Close', 'ATR', 'DC_UP', 'SUPERTREND', 'STOCHRSI', 'Position']])

if __name__ == '__main__':
    main()



