create_table_sqls = [""" CREATE TABLE IF NOT EXISTS stock (
               id INTEGER PRIMARY KEY,
               symbol TEXT NOT NULL UNIQUE,
               exchange TEXT NOT NULL, 
               company TEXT NOT NULL,
               sector TEXT,
               industry TEXT,
               searchable TEXT
    ); """,
    """CREATE TABLE IF NOT EXISTS stock_price (
               id INTEGER PRIMARY KEY,
               stock_id INTEGER,
               date NOT NULL,
               open NOT NULL,
               high NOT NULL,
               low NOT NULL,
               close NOT NULL,
               adjusted_close NOT NULL,
               volume NOT NULL,
               FOREIGN KEY (stock_id) REFERENCES stock (id),
               UNIQUE(stock_id,date)
    );""",
    """CREATE TABLE IF NOT EXISTS journal (
               id INTEGER PRIMARY KEY,
               broker NOT NULL,               
               stock_id INTEGER,
               buy_date NOT NULL,
               buy_price NOT NULL,
               sell_date,
               sell_price,
               qty NOT NULL,               
               FOREIGN KEY (stock_id) REFERENCES stock (id)
    );"""]