import db_config
import table

def main():

    # create a database connection
    conn = db_config.create_connection(db_config.DB_FILE)

    # # create tables
    if conn is not None:
        # create tables
        for tableSQL in table.create_table_sqls:
            db_config.execute_query(conn, tableSQL)
        
        conn.commit()
        conn.close()
    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()