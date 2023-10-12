import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime

import Database.db_config as db_config
from nsepython import *
from custom_indicators import *

def calc_indicators(data, volume_period=50, price_period = 15):
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
    data["SUPERTREND"] = ta.supertrend(data['High'], data['Low'], data['Close'], length=14, multiplier=3)[['SUPERT_14_3.0']]
    data["STOCHRSI"] = ta.stochrsi(data['Close'])['STOCHRSIk_14_14_3_3']

    return data


def populate_stock():
    nse_stocks = nse_eq_symbols()
    conn = db_config.create_connection(db_config.DB_FILE)

    df = pd.read_sql('select symbol from stock', conn)
    existing_stock = [symbol for symbol in df['symbol']]

    for stock in nse_stocks:
        if stock not in existing_stock:
            try:
                info = yf.Ticker(f"{stock}.NS").info
                longName = ""
                sector = ""
                industry = ""
                if 'longName' in info:
                    longName = info['longName']
                if 'sectorDisp' in info:
                    sector = info['sectorDisp']
                if 'industryDisp' in info:
                    industry = info['industryDisp']
                query = f'INSERT INTO stock (symbol, exchange, company, sector, industry) VALUES \
                    ("{stock}", "NS", "{longName}", "{sector}", "{industry}");'
                db_config.execute_query(conn, query)
                conn.commit()
                print(f"Processed {stock}")
            except:
                print(f"Not able to insert for {stock}")

    conn.close()

def populate_historical_price():
    conn = db_config.create_connection(db_config.DB_FILE)

    df = pd.read_sql('select id,symbol,exchange from stock', conn)
    existing_record = pd.read_sql('select stock_id, date(max(date)) as last_date from stock_price group by stock_id;', conn)
    print(f"existing_record: {existing_record}")
    existing_ids = [id for id in existing_record['stock_id']]

    for index,row in df.iterrows():
        print(f"Processing for {row['symbol']}")

        try:
            if row['id'] not in existing_ids:

                data = yf.download(f"{row['symbol']}.{row['exchange']}")

                for index,row_d in data.iterrows():
                    query = f"""
                        INSERT INTO stock_price (stock_id, date, open, high, low, close, adjusted_close, volume) VALUES
                        ({row['id']}, '{index}', {row_d['Open']}, {row_d['High']}, {row_d['Low']}, {row_d['Close']},
                        {row_d['Adj Close']}, {row_d['Volume']} );
                    """
                    # print(query)
                    db_config.execute_query(conn, query)
        except:
            print(f"Cannot process for {row['symbol']}")


    conn.commit()
    conn.close()

def get_candle_pattern():
    conn = db_config.create_connection(db_config.DB_FILE)
    db_config.execute_query(conn, "DELETE FROM pattern_value;")
    db_config.execute_query(conn, "DELETE FROM indicator_value;")
    stocks_df = pd.read_sql('select id, symbol from stock;', conn)
    stock_id_lst = [id for id in stocks_df['id']]
    # stock_id_lst = [1,2,3,4,5]
    stocks_df.set_index('symbol', inplace=True)
    index_data = yf.download('^NSEI')

    db_pattern = pd.DataFrame()
    db_indicators = pd.DataFrame()
    for id in stock_id_lst:
        try:
            df = pd.read_sql(f"select date(date) as Date,open as Open,high as High,low as Low,close as Close,volume as Volume from stock_price where stock_id = {id} and date(date) >'2023-01-01';", conn)
            db_row = df.ta.cdl_pattern(name="all").tail(1)
            db_row['id'] = id
            db_row.set_index('id', inplace=True)
            # db_row.to_sql(name='pattern_value', con=conn, if_exists='append', index_label='id')
            db_pattern = db_pattern._append(db_row)

            df['Date'] = pd.to_datetime(df['Date'])

            df_indicator = calc_indicators(df).tail(2)
            df_indicator.set_index('Date', inplace=True)

            df_indicator["RS"] = calculate_relative_strength(df, index_data).tail(2)
            df_indicator['id'] = id
            df_indicator.reset_index('Date', inplace=True)
            df_indicator.set_index('id', inplace=True)
            db_indicators = db_indicators._append(df_indicator)
            # df_indicator.to_sql(name='indicator_value', con=conn, if_exists='append', index_label='id')

        except:
            print(f"Not able to calculate row for id: {id}")

    db_pattern.to_sql(name='pattern_value', con=conn, if_exists='append', index_label='id')
    db_indicators.to_sql(name='indicator_value', con=conn, if_exists='append', index_label='id')

    conn.commit()
    conn.close()


def add_daily_data():
    print("Getting bhavcopy data")
    bhavcopy = get_bhavcopy(datetime.datetime.now().strftime('%d-%m-%Y'))
    bhavcopy[' DATE1'] = pd.to_datetime(bhavcopy[' DATE1'])
    print("got bhavcopy data from NSE")

    conn = db_config.create_connection(db_config.DB_FILE)
    stocks_df = pd.read_sql('select id, symbol as SYMBOL from stock;', conn)
    print("Connection to DB established")
    merged_df =  pd.merge(bhavcopy ,stocks_df, how = 'inner', on = ['SYMBOL'])

    for key,value in merged_df.iterrows():

        query = f"""
            INSERT INTO stock_price (stock_id, date, open, high, low, close, adjusted_close, volume) VALUES
            ({value['id']}, '{value[' DATE1']}', {value[' OPEN_PRICE']}, {value[' HIGH_PRICE']},
            {value[' LOW_PRICE']}, {value[' CLOSE_PRICE']}, {value[' CLOSE_PRICE']}, {value[' TTL_TRD_QNTY']} );
        """
        db_config.execute_query(conn, query)

    conn.commit()
    conn.close()

def main():
    # print("Populating Stock Table")
    # populate_stock()
    # print("Done Populating Stock Table, Starting with Populating historical Stock Prices")
    # populate_historical_price()
    # print("Done Populating Historical Stock prices")
    print("Starting the process")
    add_daily_data()
    get_candle_pattern()


if __name__ == '__main__':
    main()