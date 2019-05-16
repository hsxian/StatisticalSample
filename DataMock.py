class DataMock():
    def __init__(self):
        import psycopg2

        self.SPORTS_RECORD = 'SPORTS_RECORD'

        self.conn = psycopg2.connect(
            database="testdb", user="postgres", password="123", host="127.0.0.1", port="5432")

        self.cur = self.conn.cursor()

        self.cur.execute('''CREATE TABLE IF NOT EXISTS {}
            (ID INT PRIMARY KEY     NOT NULL,
            STARTTIME          TIMESTAMP    NOT NULL,
            ENDTIME            TIMESTAMP     NOT NULL,           
            SITE               TEXT,
            EQUIPMENT          TEXT,
            ITEM               TEXT,
            COACH              TEXT
            );'''.format(self.SPORTS_RECORD)

        self.conn.commit()

    def drop_all_table(self):
        self.cur.execute('DROP TABLE {}'.format(self.SPORTS_RECORD))
