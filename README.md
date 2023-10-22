# tradingapp_local
Trading App on your local

You can only use it with Python 3.10 , and you can use requirements.txt to get the libraries

For 1st time run -
In pouplate_db.py Uncomment below function
    # print("Populating Stock Table")
    # populate_stock()
    # print("Done Populating Stock Table, Starting with Populating historical Stock Prices")
    # populate_historical_price()
    # print("Done Populating Historical Stock prices")
and Comment 
    add_daily_data() function

from next day, it needs to be run everyday with on add_daily_data() and get_candle_pattern(), other needs to be commented out, 
right now it only gets current days data, so the functions has to be run everyday with other functions commented out

journal.csv can be modified and then need to run journal.py to be able to load the journal

the backtest function right now uses only the strategy which I thought works, but you can add your own strategy and use that instead
