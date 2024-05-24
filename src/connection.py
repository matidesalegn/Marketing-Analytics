import psycopg2
from psycopg2 import sql, extras

class PostgresConnection:
    def __init__(self, dbname, user, password, host='localhost', port='5432'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.conn.cursor()
            print("Connected to PostgreSQL database!")
        except Exception as e:
            print(f"Error: {e}")

    def create_telegram_posts_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS telegram_posts (
            id SERIAL PRIMARY KEY,
            keyword TEXT,
            post_link TEXT,
            views INTEGER,
            date DATE,
            post_hour TIME,
            time_of_day TEXT
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            print("Table 'telegram_posts' created successfully!")
        except Exception as e:
            print(f"Error creating table: {e}")
            self.conn.rollback()

    def create_google_play_reviews_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS google_play_reviews (
            bank TEXT,
            appId TEXT,
            reviewId TEXT,
            userName TEXT,
            userImage TEXT,
            thumbsUpCount INTEGER,
            reviewCreatedVersion TEXT,
            at TIMESTAMP,
            replyContent TEXT,
            repliedAt TIMESTAMP,
            appVersion TEXT,
            score INTEGER,
            content TEXT,
            keywords TEXT,
            LDA_Category TEXT,
            Sentiment TEXT,
            Insight TEXT
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            print("Table 'google_play_reviews' created successfully!")
        except Exception as e:
            print(f"Error creating table: {e}")
            self.conn.rollback()

    def create_google_play_downloads_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS google_play_downloads (
            bank TEXT,
            appId TEXT,
            date DATE,
            downloads TEXT
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            print("Table 'google_play_downloads' created successfully!")
        except Exception as e:
            print(f"Error creating table: {e}")
            self.conn.rollback()

    def insert_data(self, data, table_name, columns):
        if self.conn is not None:
            try:
                insert_query = sql.SQL("""
                INSERT INTO {} ({}) VALUES %s
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                extras.execute_values(self.cursor, insert_query, data)
                self.conn.commit()
                print(f"Data inserted successfully into {table_name}!")
            except Exception as e:
                print(f"Error inserting data: {e}")
                self.conn.rollback()

    def close_connection(self):
        if self.conn is not None:
            self.cursor.close()
            self.conn.close()
            print("Connection closed.")