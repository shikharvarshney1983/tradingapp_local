import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from tabulate import tabulate

from custom_indicators import *
from strategies import *


def backtest_script(name='ITC', sell_strategy='VStop', data_point=0, volume_period=50, price_period=15, capital=300000, 
                    charges=20, TF='daily', strategy="MyStrategy"):
    
    if strategy == "SupertrentStochRSI":
        data = supertrend_stocRSI(name=name, data_point=data_point)
    elif strategy == "MyStrategyWithSL":
        data = my_strategy_withSL(name=name, sell_strategy=sell_strategy, data_point=data_point, volume_period= volume_period,
                       price_period=price_period)
    else:
        data = my_strategy(name=name, sell_strategy=sell_strategy, data_point=data_point, volume_period= volume_period,
                       price_period=price_period)
    # required_df = data[data.index <= data[data['Position'] == 'Sell'].index[-1]]
    required_df = data
    per_trade_cap = capital / 100
    temp_df = {}
    rows = []
    print(data)
    for index, row in required_df.iterrows():
        
        if row.Position == 'Buy':
            temp_df['Name'] = name
            temp_df['Entry Date'] = index
            temp_df['Entry Price'] = row.Close
            temp_df['Quantity'] = per_trade_cap // row.Close
        elif row.Position == 'Add':
            add_qty = per_trade_cap // row.Close 
            total_allocation = temp_df['Entry Price'] * temp_df['Quantity'] + add_qty * row.Close
            temp_df['Entry Price'] = round (total_allocation / (add_qty + temp_df['Quantity']), 2)
            temp_df['Quantity'] = temp_df['Quantity'] + add_qty
        elif row.Position == 'Sell':
            temp_df['Exit Date'] = index
            temp_df['Exit Price'] = row.Close
            temp_df['PNL'] = round ( (temp_df['Exit Price'] - temp_df['Entry Price']) * temp_df['Quantity'] - charges, 2)
            temp_df['% PNL'] = round ((temp_df['PNL'] / (temp_df['Quantity'] * temp_df['Entry Price'])) * 100, 2)
            temp_df['Holding Period'] = temp_df['Exit Date'] - temp_df['Entry Date']
            rows.append(temp_df)  
            #print(temp_df)
            temp_df = {}
    return pd.DataFrame(rows)

def trading_summary(trades):
    if len(trades) > 0:
        total_traded_scripts = len(trades['Name'].unique())
        total_trade = len(trades.index)
        pnl = round(trades.PNL.sum(), 2)
        winners = len(trades[trades.PNL > 0])
        loosers = len(trades[trades.PNL < 0])
        win_ratio = str(round((winners/total_trade) * 100, 2)) + "%"
        total_profit = round(trades[trades.PNL > 0].PNL.sum(), 2)
        total_loss = round(trades[trades.PNL < 0].PNL.sum(), 2)
        average_loss_per_trade = round(total_loss/loosers, 2)
        average_profit_per_trade = round(total_profit/winners, 2)
        average_pnl_per_trade = round(pnl/total_trade, 2)
        risk_reward = f'1:{round(average_profit_per_trade/average_pnl_per_trade, 2)}'
    else:
        total_traded_scripts = total_trade = pnl = winners = loosers = total_profit = total_loss =  0
        win_ratio = average_loss_per_trade = average_profit_per_trade = risk_reward = 0
    parameters = ['Total Trade Scripts', 'Total Trade', 'PNL', 'Winners', 'Loosers', ' Win Ratio', 'Total Profit','Total Loss',
                    'Average Profit per Trade', 'Average Loss per Trade', 'Average PNL per Trade', 'Risk Reward']
    data_points = [total_traded_scripts, total_trade, pnl, winners, loosers, win_ratio, total_profit, total_loss, average_profit_per_trade,
                   average_loss_per_trade, risk_reward]
    
    data = list(zip(parameters, data_points))
    # print (tabulate(data, ['Parameters', 'Values'], tablefmt='psql'))    
    return data

def main():
    symbol = "SDBL"
    # print("My Strategy Output")
    # trades = backtest_script(name=symbol, TF="daily", volume_period=50, price_period = 15)
    # str1_dp = trading_summary(trades)
    # print(trades)
    print("Super Trend StochRSI Output")
    trades = backtest_script(name=symbol, strategy="SupertrentStochRSI")
    str2_dp = trading_summary(trades)
    # print(trades)
    print("My Strategy with SL")
    trades = backtest_script(name=symbol, strategy="MyStrategyWithSL")

    str3_dp = trading_summary(trades)
    data = []

    for i in range(len(str2_dp)):
        data.append((str2_dp[i][0], str2_dp[i][1], str3_dp[i][1]))

    print (tabulate(data, ['Parameters', 'StochRSI', 'MyStrategyWithSL'], tablefmt='psql')) 



if __name__ == '__main__':
    main()