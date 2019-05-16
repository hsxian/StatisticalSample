class DataAccess():
    def __init__(self):
        import psycopg2
        self.conn = psycopg2.connect(
            database="testdb", user="postgres", password="123", host="127.0.0.1", port="5432")

        cur = self.conn.cursor()
        cur.execute('DROP TABLE SPORTS_RECORD')
        # cur.execute('''CREATE TABLE IF NOT EXISTS SPORTS_RECORD
        #     (ID INT PRIMARY KEY     NOT NULL,
        #     STARTTIME          TIMESTAMP    NOT NULL,
        #     ENDTIME            TIMESTAMP     NOT NULL,           
        #     SITE               TEXT,
        #     EQUIPMENT          TEXT,
        #     ITEM               TEXT,
        #     COACH              TEXT
        #     );''')

        self.conn.commit()


    def mock_data(self):
        pass
        # self.db.

dac = DataAccess()
