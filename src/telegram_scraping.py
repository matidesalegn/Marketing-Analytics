import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2

def get_time_of_day(hour):
    if 5 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 17:
        return 'afternoon'
    elif 17 <= hour < 21:
        return 'evening'
    else:
        return 'night'

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

    def insert_data(self, df, table_name):
        if self.conn is not None:
            try:
                for i, row in df.iterrows():
                    self.cursor.execute(f"""
                        INSERT INTO {table_name} (date, post_link, view, post_hour, bank, time_of_day)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, tuple(row))
                self.conn.commit()
                print("Data inserted successfully!")
            except Exception as e:
                print(f"Error: {e}")
                self.conn.rollback()

    def close_connection(self):
        if self.conn is not None:
            self.cursor.close()
            self.conn.close()
            print("Connection closed.")

# URL of the Telegram post
url = 'https://t.me/tikvahethiopia/86164'

# Fetch the page content
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Extract data
footer_element = soup.find('div', {'class': 'tgme_widget_message_footer'})
if footer_element:
    date_element = footer_element.find('a', {'class': 'tgme_widget_message_date'})
    if date_element:
        date = date_element.find('time')['datetime'].split('T')[0]

    post_link_element = footer_element.find('div', {'class': 'tgme_widget_message_link'})
    if post_link_element:
        post_link = post_link_element.find('a')['href']

    view_element = footer_element.find('span', {'class': 'tgme_widget_message_views'})
    if view_element:
        view = view_element.text.replace(' views', '')

    time_element = date_element.find('time')
    post_hour = time_element.text.strip().split()[-1].split(':')[0]

    bank = 'CBE'  # Manually set this based on the context or extract if available
    time_of_day = get_time_of_day(int(post_hour))

    # Create DataFrame
    data = {
        'Date': [date],
        'Post link': [post_link],
        'View': [view],
        'Post Hour': [post_hour],
        'Bank': [bank],
        'Time of day': [time_of_day]
    }
    df = pd.DataFrame(data)

    # Insert data into PostgreSQL
    db = PostgresConnection(dbname='bank', user='postgres', password='Mati@1993')
    db.connect()
    db.insert_data(df, 'telegram_posts')
    db.close_connection()
else:
    print("Unable to extract data from the page.")