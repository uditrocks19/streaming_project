import sqlite3

class SQLiteConnector:
    def __init__(self, db_name='stock_prices.db'):
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()
        self.create_table()

    def create_table(self):
        # self.cursor.execute('DROP TABLE IF EXISTS PRICES')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS PRICES (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL NOT NULL
            )
        ''')
        self.connection.commit()

    def insert_tick(self, symbol: str, price: float):
        self.cursor.execute('''
            INSERT INTO PRICES (symbol, price)
            VALUES (?, ?)
        ''', (symbol, price))
        self.connection.commit()
    
    def res(self):
        self.cursor.execute('SELECT * FROM PRICES')
        return self.cursor.fetchall()
    
    def close(self):
        self.connection.close()