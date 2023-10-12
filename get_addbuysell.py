import pandas as pd
from tradingview_ta import TA_Handler, Interval, Exchange

import Database.db_config as db_config

def is_consolidating(df, percentage=3):

    threshold = 1 - (percentage/ 100)
    recent_candlesticks = df[:15]
    max_close = recent_candlesticks['close'].max()
    min_close = recent_candlesticks['close'].min()

    if min_close > (max_close * threshold):
        return True

    return False

def is_breaking_out(df, percentage=3):

    last_close = df[:1]['close'].values[0]
    last_high = df[:1]['high'].values[0]
    last_low = df[:1]['low'].values[0]
    last_volume = df[:1]['volume'].values[0]
    golden_price = last_high - (last_high - last_low) * 0.2

    if is_consolidating(df[1:], percentage):
        recent_closes = df[1:16]
        recent_volume = df[1:51]
        #
        if ((last_close > recent_closes['close'].max()) & (last_volume > 1.5 * recent_volume['volume'].mean())
            & (last_close > golden_price)) :
            return True

    return False

def range_breakout():
    conn = db_config.create_connection(db_config.DB_FILE)
    stocks_df = pd.read_sql('select id, symbol from stock;', conn)
    stock_id_lst = [id for id in stocks_df['id']]
    stocks_df.set_index('symbol', inplace=True)
    range_bo = []
    for id in stock_id_lst:

        df = pd.read_sql(f'select date(date),close,high,low,volume from stock_price where stock_id = {id} order by date(date) desc limit 60;', conn)

        try:
            if is_breaking_out(df, percentage=3):
                range_bo.append(stocks_df[ stocks_df.id == id].index[0])
        except:
            print (f"range_breakout Failed for {id}")

        # db_row.to_sql(name='range_breakout', con=conn, if_exists='append', index_label='id')

    conn.commit()
    conn.close()
    return range_bo

def generate_add_sell():
    conn = db_config.create_connection(db_config.DB_FILE)
    add_query = """
        SELECT * FROM stock WHERE id in (SELECT id from indicator_value WHERE 
        (Date = (SELECT max(Date) FROM indicator_value))
        AND  ((((DC_UP - close)/close) * 100 < 0.5) OR ((((DC_UP - close)/close) * 100 < 2) AND (Volume > 2 * AVG_VOL))));
    """
    vstop_sell_query = "SELECT * FROM stock WHERE id in (SELECT id FROM indicator_value WHERE (Date = (SELECT max(Date) FROM indicator_value)) AND (VStop > Close) AND (RSI < 55 OR EMA21 > Close OR ADX < 25 OR RS < 0));"
    df_add = pd.read_sql(add_query, conn)
    df_sell_vstop = pd.read_sql(vstop_sell_query, conn)

    conn.commit()
    conn.close()

    return df_add, df_sell_vstop


def portfolio_level_recmmendation():

    df_add, df_sell = generate_add_sell()

    conn = db_config.create_connection(db_config.DB_FILE)
    journal = pd.read_sql('select symbol from stock where id in (select distinct stock_id from journal where id not in ( select id from journal where sell_price > 0));', conn)
    
    watchlist = [symbol for symbol in journal['symbol']]
    recommendation = {}
    add_watchlist = df_add['symbol'].tolist()
    sell_watchlist = df_sell['symbol'].tolist()
    for stock in watchlist:
        if stock in add_watchlist:
            recommendation[stock] = "Add"
        elif stock in sell_watchlist:
            recommendation[stock] = "Sell"
        else:
            recommendation[stock] = "Hold"
    conn.commit()
    conn.close()

    return recommendation

def get_buy_recommendation():
    conn = db_config.create_connection(db_config.DB_FILE)    
    buy_query_mystrategy = """
        SELECT * FROM stock WHERE id in (SELECT id FROM indicator_value WHERE (Date = (SELECT max(Date) FROM indicator_value)) 
        AND (RSI > 55 AND ADX > 25 AND RS > 0 AND Volume > 1.5 * AVG_VOL AND Close > EMA21 AND Close > HI_CLOSE) AND id in
        (SELECT id FROM indicator_value WHERE (date(Date) = (SELECT Date(max(Date),"-1 day") FROM indicator_value)) AND 
        (RSI < 55 OR ADX < 25 OR RS < 0 OR Close < EMA21)));
    """
    buy_query_suptrend = """
        SELECT * FROM stock WHERE id in (SELECT id FROM indicator_value WHERE (Date = (SELECT max(Date) FROM indicator_value)) 
        AND (STOCHRSI > 20 AND SUPERTREND < Close AND RS > 0 AND Volume > 1.5 * AVG_VOL AND Close > EMA21 AND Close > HI_CLOSE)
        AND id in ( SELECT id FROM indicator_value WHERE (date(Date) = (SELECT Date(max(Date),"-1 day") FROM indicator_value)) 
        AND (STOCHRSI < 20)));
    """

    df_buy_mystrategy = pd.read_sql(buy_query_mystrategy, conn)
    df_buy_supertrend = pd.read_sql(buy_query_suptrend, conn)
    buy_my_strategy = []
    buy_supertrend = []
    range_bo = []

    for key,value in df_buy_mystrategy.iterrows():
        try:
            symbol = TA_Handler(
                symbol=value['symbol'],
                screener="india",
                exchange="NSE",
                interval=Interval.INTERVAL_1_DAY,
            )
            summary = symbol.get_analysis().summary
            indicators = symbol.get_analysis().indicators
            golden_price = indicators['high'] - (indicators['high'] - indicators['low']) * 0.2
            if (indicators['close'] > golden_price) & (indicators['close'] > 10):
                buy_my_strategy.append({'symbol': value['symbol'], 'recommendation':summary['RECOMMENDATION'], 'BUY': summary['BUY'],
                                        'SELL':summary['SELL'], 'NEUTRAL':summary['NEUTRAL'], 'S1': round(indicators['Pivot.M.Fibonacci.S1'],2),
                                        'S2': round(indicators['Pivot.M.Fibonacci.S2'],2),'S3': round(indicators['Pivot.M.Fibonacci.S3'],2),
                                        'R1': round(indicators['Pivot.M.Fibonacci.R1'],2), 'R2': round(indicators['Pivot.M.Fibonacci.R2'],2),
                                        'R3': round(indicators['Pivot.M.Fibonacci.R3'],2),'close': round(indicators['close'],2),
                                        'change': round(indicators['change'],2), 'Pivot': round(indicators['Pivot.M.Fibonacci.Middle'],2)}) 
        except:
            print(f"Symbol Not found {value['symbol']}")   

    for key,value in df_buy_supertrend.iterrows():
        try:
            symbol = TA_Handler(
                symbol=value['symbol'],
                screener="india",
                exchange="NSE",
                interval=Interval.INTERVAL_1_DAY,
            )
            summary = symbol.get_analysis().summary
            indicators = symbol.get_analysis().indicators       
            golden_price = indicators['high'] - (indicators['high'] - indicators['low']) * 0.2
            if (indicators['close'] > golden_price) & (indicators['close'] > 10):            
                buy_supertrend.append({'symbol': value['symbol'], 'recommendation':summary['RECOMMENDATION'], 'BUY': summary['BUY'],
                                            'SELL':summary['SELL'], 'NEUTRAL':summary['NEUTRAL'], 'S1': round(indicators['Pivot.M.Fibonacci.S1'],2),
                                            'S2': round(indicators['Pivot.M.Fibonacci.S2'],2),'S3': round(indicators['Pivot.M.Fibonacci.S3'],2),
                                            'R1': round(indicators['Pivot.M.Fibonacci.R1'],2), 'R2': round(indicators['Pivot.M.Fibonacci.R2'],2),
                                            'R3': round(indicators['Pivot.M.Fibonacci.R3'],2),'close': round(indicators['close'],2),
                                            'change': round(indicators['change'],2), 'Pivot': round(indicators['Pivot.M.Fibonacci.Middle'],2)})
        except:
            print(f"Failed for {value['symbol']}")

    for bo in range_breakout():
        try:
            symbol = TA_Handler(
                symbol=bo,
                screener="india",
                exchange="NSE",
                interval=Interval.INTERVAL_1_DAY,
            )
            summary = symbol.get_analysis().summary
            indicators = symbol.get_analysis().indicators       
            range_bo.append({'symbol': bo, 'recommendation':summary['RECOMMENDATION'], 'BUY': summary['BUY'],
                                        'SELL':summary['SELL'], 'NEUTRAL':summary['NEUTRAL'], 'S1': round(indicators['Pivot.M.Fibonacci.S1'],2),
                                        'S2': round(indicators['Pivot.M.Fibonacci.S2'],2),'S3': round(indicators['Pivot.M.Fibonacci.S3'],2),
                                        'R1': round(indicators['Pivot.M.Fibonacci.R1'],2), 'R2': round(indicators['Pivot.M.Fibonacci.R2'],2),
                                        'R3': round(indicators['Pivot.M.Fibonacci.R3'],2),'close': round(indicators['close'],2),
                                        'change': round(indicators['change'],2), 'Pivot': round(indicators['Pivot.M.Fibonacci.Middle'],2)})
        except:
            print(f"Failed for {bo}")

    conn.commit()
    conn.close()    

    return buy_my_strategy, buy_supertrend, range_bo



def old():
    conn = db_config.create_connection(db_config.DB_FILE)
    add_query = """
        SELECT * FROM stock WHERE id in (SELECT id from indicator_value WHERE 
        (Date = (SELECT max(Date) FROM indicator_value))
        AND  ((((DC_UP - close)/close) * 100 < 0.5) OR ((((DC_UP - close)/close) * 100 < 2) AND (Volume > 2 * AVG_VOL))));
    """
    
    vstop_sell_query = "SELECT * FROM stock WHERE id in (SELECT id FROM indicator_value WHERE (Date = (SELECT max(Date) FROM indicator_value)) AND (VStop > Close) AND (RSI < 55 OR EMA21 > Close OR ADX < 25 OR RS < 0));"
    buy_query_mystrategy = """
        SELECT * FROM stock WHERE id in (SELECT id FROM indicator_value WHERE (Date = (SELECT max(Date) FROM indicator_value)) 
        AND (RSI > 55 AND ADX > 25 AND RS > 0 AND Volume > 1.5 * AVG_VOL AND Close > EMA21 AND Close > HI_CLOSE) AND id in
        (SELECT id FROM indicator_value WHERE (date(Date) = (SELECT Date(max(Date),"-1 day") FROM indicator_value)) AND 
        (RSI < 55 OR ADX < 25 OR RS < 0 OR Close < EMA21)));
    """
    buy_query_suptrend = """
        SELECT * FROM stock WHERE id in (SELECT id FROM indicator_value WHERE (Date = (SELECT max(Date) FROM indicator_value)) 
        AND (STOCHRSI > 20 AND SUPERTREND < Close AND RS > 0 AND Volume > 1.5 * AVG_VOL AND Close > EMA21 AND Close > HI_CLOSE)
        AND id in ( SELECT id FROM indicator_value WHERE (date(Date) = (SELECT Date(max(Date),"-1 day") FROM indicator_value)) 
        AND (STOCHRSI < 20)));
    """
    df_add = pd.read_sql(add_query, conn)
    df_sell_vstop = pd.read_sql(vstop_sell_query, conn)

    df_buy_mystrategy = pd.read_sql(buy_query_mystrategy, conn)
    df_buy_supertrend = pd.read_sql(buy_query_suptrend, conn)

    # print(df_add)
    # print(df_sell_vstop)
    # print(df_sell_psar)
    print("Buy using My Strategy")
    for key,value in df_buy_mystrategy.iterrows():
        try:
            symbol = TA_Handler(
                symbol=value['symbol'],
                screener="india",
                exchange="NSE",
                interval=Interval.INTERVAL_1_DAY,
            )
            summary = symbol.get_analysis().summary
            indicators = symbol.get_analysis().indicators       
            print(f"Buy generated for {value['symbol']}, recommendation: {summary}, ")
            print(f"Fibonacci Support S1: {indicators['Pivot.M.Fibonacci.S1']}, Fibonacci Resistance R1:{indicators['Pivot.M.Fibonacci.R1']}")
        except:
            print(f"Symbol Not found {value['symbol']}")

    print("Buy using Supertrend Strategy")
    for key,value in df_buy_supertrend.iterrows():
        symbol = TA_Handler(
            symbol=value['symbol'],
            screener="india",
            exchange="NSE",
            interval=Interval.INTERVAL_1_DAY,
        )
        summary = symbol.get_analysis().summary
        indicators = symbol.get_analysis().indicators       
        print(f"Buy generated for {value['symbol']}, recommendation: {summary}, ")
        print(f"Fibonacci Support S1: {indicators['Pivot.M.Fibonacci.S1']}, Fibonacci Resistance R1:{indicators['Pivot.M.Fibonacci.R1']}")

    print(portfolio_level_recmmendation(df_add,df_sell_vstop))

    conn.commit()
    conn.close()

def main():
    buy_my_strategy, buy_supertrend, range_bo = get_buy_recommendation()
    print(buy_my_strategy)
    print(buy_supertrend)
    print(range_bo)
    print(portfolio_level_recmmendation())

if __name__ == "__main__":
    main()