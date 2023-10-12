import pandas as pd
import Database.db_config as db_config
from datetime import datetime
from get_addbuysell import *

dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d')


def add_journal():
    pass

def edit_journal():
    pass

def calc_summary():
    pass

def calc_pivot():
    pass

def create_journal():
    pd.options.mode.chained_assignment = None
    conn = db_config.create_connection(db_config.DB_FILE)
    journal = pd.read_sql('select * from journal', conn)

    journal['cmp'] = 0.0
    journal['prev_close'] = 0.0
    journal['notional'] = 0.0
    journal['realized'] = 0.0
    current_invested = 0.0
    for key,value in journal.iterrows():
        journal.loc[journal.id == value['id'], 'invested'] = journal.loc[journal.id == value['id']]['buy_price'].values[0] * journal.loc[journal.id == value['id']]['qty'].values[0]
        price = pd.read_sql(f"select close from stock_price where stock_id = {value['stock_id']} order by date desc limit 2", conn)
        try:
            journal.loc[journal.id == value['id'], 'cmp'] = price['close'].values[0]
            journal.loc[journal.id == value['id'], 'prev_close'] = price['close'].values[1]
        except:
            pass
        if value['sell_date'] is None:
            current_invested += journal.loc[journal.id == value['id'], 'invested'].values[0] 

            journal.loc[journal.id == value['id'], 'notional'] = (price['close'].values[0] - journal.loc[journal.id == value['id']]['buy_price'].values[0]) * journal.loc[journal.id == value['id']]['qty'].values[0]
            journal.loc[journal.id == value['id'], 'today_notional'] = (price['close'].values[0] - price['close'].values[1]) * journal.loc[journal.id == value['id']]['qty'].values[0]
        else:

            journal.loc[journal.id == value['id'], 'realized'] = (journal.loc[journal.id == value['id']]['sell_price'].values[0] - journal.loc[journal.id == value['id']]['buy_price'].values[0]) * journal.loc[journal.id == value['id']]['qty'].values[0]

    notional = round(journal['notional'].sum(), 2)
    realized = round(journal['realized'].sum(), 2)
    today_notional = round(journal['today_notional'].sum(), 2)

    total = {"Overall Notional": notional, "Realized": realized, "Total": round(notional + realized, 2), "Current Investement": round(current_invested, 2), "Todays Notional": today_notional}

    sum_journal = journal[journal.notional != 0][['stock_id', 'qty', 'invested', 'notional']].groupby('stock_id').sum()

    sum_journal['avg_buy'] = round(sum_journal['invested']/sum_journal['qty'], 2)
    sum_journal['current_return'] = round(sum_journal['notional']/sum_journal['invested'] * 100, 2)
    stocks = pd.read_sql('select * from stock', conn)
    sum_journal['symbol'] = 'NA'
    sum_journal['industry'] = 'NA'
    sum_journal['cmp'] = 0.0
    sum_journal['prev_close'] = 0.0
    sum_journal['recommendation'] = 'NA'
    sum_journal.reset_index(inplace=True)
    
    for key,value in sum_journal.iterrows():
        sum_journal.loc[sum_journal.stock_id == value['stock_id'], 'symbol'] = stocks[stocks.id == value['stock_id']]['symbol'].values[0]
        sum_journal.loc[sum_journal.stock_id == value['stock_id'], 'industry'] = stocks[stocks.id == value['stock_id']]['industry'].values[0]
        sum_journal.loc[sum_journal.stock_id == value['stock_id'], 'cmp'] = journal[journal.stock_id == value['stock_id']]['cmp'].values[0]
        sum_journal.loc[sum_journal.stock_id == value['stock_id'], 'prev_close'] = journal[journal.stock_id == value['stock_id']]['prev_close'].values[0]

    recommendation = portfolio_level_recmmendation()
    for key in recommendation:
        sum_journal.loc[sum_journal.symbol == key, 'recommendation'] = recommendation[key]
        
    conn.commit()
    conn.close()

    return sum_journal,total

def get_journal():
    conn = db_config.create_connection(db_config.DB_FILE)
    journal = pd.read_sql('select * from journal', conn)
    conn.commit()
    conn.close()

    return journal

def first_time():
    conn = db_config.create_connection(db_config.DB_FILE)
    stock_df = pd.read_sql('select id,symbol from stock', conn)
    db_config.execute_query(conn, 'delete from journal')

    journal = pd.read_csv("journal.csv")
    journal['buy_date'] = pd.to_datetime(journal['buy_date'])
    journal['sale_date'] = pd.to_datetime(journal['sale_date'])
    
    for key,value in journal.iterrows():
        id = stock_df[ stock_df.symbol == value['stock']]['id'].values[0]

        if value['sale_price'] == 0:
            query = f"""INSERT INTO journal (broker, stock_id, buy_date, buy_price, qty) VALUES 
                ('{value['broker']}', {id}, '{value['buy_date']}', {value['buy_price']}, {value['qty']});"""
        else:
            query = f"""INSERT INTO journal (broker, stock_id, buy_date, buy_price, sell_date, sell_price, qty) VALUES 
                ('{value['broker']}', {id}, '{value['buy_date']}', {value['buy_price']}, '{value['sale_date']}', {value['sale_price']}, 
                {value['qty']});"""
            
        # print(query)
        db_config.execute_query(conn, query)

    conn.commit()
    conn.close()

def main():
    first_time()
    create_journal()
    

if __name__ == "__main__":
    main()