import mysql.connector

db_param = {
    "host": "localhost",
    "user": "root",
    "password": "Sunn1@34"
}

def create_connection(db_param):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Sunn1@34"
)
        return conn
    except :
        print("Cannot connect")

    return conn


def execute_query(conn, sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except:
        print(f"Cannot execute {sql}")


def main():
    conn = create_connection(db_param)
