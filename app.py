from flask import Flask, render_template, request

import pandas as pd

from pattern import patterns, patterns_value
import Database.db_config as db_config
import journal as journal
import get_addbuysell as get_addbuysell
from backtesting import *

# data["EMA21"] = ta.ema(data["Close"],21)
buy_my_strategy, buy_supertrend, range_bo = get_addbuysell.get_buy_recommendation()

app = Flask(__name__)

@app.route("/")
def index():
    pattern = request.args.get('pattern', None)

    conn = db_config.create_connection(db_config.DB_FILE)
    stocks_df = pd.read_sql('select id, symbol, company, sector, industry from stock;', conn)
    

    stocks_df.set_index('symbol', inplace=True)

    stocks = {}
    for key,value in stocks_df.iterrows():
        # print(value['company'])
        stocks[key] = {'company':value['company']}

    if pattern:
        query = f'select id,"{patterns_value[pattern]}" from pattern_value;'
        candle_pattern = pd.read_sql(query, conn)
        for key, value in candle_pattern.iterrows():
            symbol = stocks_df[stocks_df.id == value['id']].index[0]
            last = value[patterns_value[pattern]]
            if last > 0:
                stocks[symbol][pattern] = 'bullish'
            elif last < 0:
                stocks[symbol][pattern] = 'bearish'
            else:
                stocks[symbol][pattern] = None

    return render_template('index.html', patterns=patterns, stocks=stocks, current_pattern=pattern, tab=1)


@app.route("/journal")
def journal_app():
    jrn, overall = journal.create_journal()
    notional_jrn = []
    for key,value in jrn.iterrows():
        notional_jrn.append({"sector": value['industry'], "symbol": value['symbol'], "qty": value['qty'], "avg_buy": value['avg_buy'], 
                             "invested": round(value['invested'],2), "current_return": value['current_return'], "notional": round(value['notional'],2), 
                             "cmp": value['cmp'], "prev_close": value['prev_close'], "recommendation": value['recommendation'] })
    return render_template('journal.html', overall=overall, jrn=notional_jrn, tab=2)

@app.route("/buyrecommend")
def buyrecommend():
    global buy_my_strategy, buy_supertrend, range_bo

    return render_template('buy.html', buy_my_strategy=buy_my_strategy, buy_supertrend=buy_supertrend, range_bo=range_bo, tab=3)

@app.route("/backtesting")
def backtesting():
    stock = request.args.get('stocks', None)
    strategy = request.args.get('strategy', None)
    capital = 300000
    if request.args.get('capital', None):
        capital = int(request.args.get('capital', None))
    tr = []
    stock_dict = {}
    summary=[]

    conn = db_config.create_connection(db_config.DB_FILE)
    all_stocks_symbols = pd.read_sql('select id, symbol, company, sector, industry from stock;', conn)
    symbols = [symbol for symbol in all_stocks_symbols['symbol'] ]
    
    if stock:
        stock_dict = {'symbol':stock, 'company': all_stocks_symbols[ all_stocks_symbols.symbol == stock ]['company'].values[0] }
        trades = backtest_script(name=stock, TF="daily", volume_period=50, price_period = 15, capital=capital, strategy=strategy)
        summary = trading_summary(trades)
        print(summary)
        for key,values in trades.iterrows():
            tr.append({'Entry Date': str(values['Entry Date'])[:-9], 'Entry Price': values['Entry Price'],
                       'Quantity': values['Quantity'], 'Exit Date': str(values['Exit Date'])[:-9], 
                       'Exit Price': values['Exit Price'], 'PNL': values['PNL'],
                       '% PNL': values['% PNL'], 'Holding Period': str(values['Holding Period'])[:-9] })


    return render_template('backtest.html', symbols=symbols, stock=stock_dict, trades=tr, summary = summary, tab=4)