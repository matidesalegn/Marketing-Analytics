import pandas as pd
import nest_asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetHistoryRequest
import os
import logging

# Apply asyncio patch to allow nested event loops in Jupyter
nest_asyncio.apply()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TelegramScraper:
    def __init__(self, api_id, api_hash, phone, channel_username, keywords):
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.channel_username = channel_username
        self.keywords = keywords

    async def start(self):
        client = TelegramClient(StringSession(), self.api_id, self.api_hash)
        await client.start(phone=self.phone)
        return client

    async def get_channel_entity(self, client):
        try:
            channel = await client.get_entity(self.channel_username)
            return channel
        except Exception as e:
            logger.error(f"Error: {e}")
            await client.disconnect()
            return None

    def contains_keyword(self, message):
        if message and 'message' in message:
            message_text = message['message']
            for keyword in self.keywords:
                if keyword.lower() in message_text.lower():
                    return True
        return False

    async def get_all_messages(self, client, channel):
        all_messages = []
        offset_id = 0
        limit = 100
        total_messages = 0
        total_count_limit = 1000  # or some other limit you want to set

        while True:
            history = await client(GetHistoryRequest(
                peer=channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages:
                break
            messages = history.messages
            for message in messages:
                if self.contains_keyword(message.to_dict()):
                    all_messages.append(message.to_dict())
            offset_id = messages[-1].id
            total_messages = len(all_messages)
            if total_count_limit and total_messages >= total_count_limit:
                break

        return all_messages

    def extract_attributes(self, row):
        """Extract relevant attributes from a message."""
        # Extract message text
        message = row.get('message', '')

        # Find all keywords in the message
        keyword_found = [kw for kw in self.keywords if kw in message]
        keyword_found_str = ', '.join(keyword_found) if keyword_found else None

        # Extract post ID
        post_id = row.get('id', '')

        # Construct post link
        post_link = f"https://t.me/{self.channel_username}/{post_id}"

        # Extract views
        views = row.get('views', '')

        # Extract date and time
        date = pd.to_datetime(row['date']).date() if 'date' in row else ''
        post_hour = pd.to_datetime(row['date']).time() if 'date' in row else ''

        return pd.Series([keyword_found_str, post_link, views, date, post_hour])

    def save_messages_to_csv(self, messages):
        # Convert to DataFrame
        df = pd.DataFrame(messages)

        # Apply attribute extraction
        df[['keyword', 'post_link', 'views', 'date', 'post_hour']] = df.apply(self.extract_attributes, axis=1)

        # Select only the relevant columns
        df_filtered = df[['keyword', 'post_link', 'views', 'date', 'post_hour']]

        # Ensure the data directory exists
        data_dir = '../data'
        os.makedirs(data_dir, exist_ok=True)  # Create data directory if it doesn't exist

        # Save to CSV
        csv_file_path = os.path.join(data_dir, 'telegram_channel_data.csv')
        df_filtered.to_csv(csv_file_path, index=False)
        logger.info(f"Data extraction completed successfully. CSV file saved at: {csv_file_path}")


async def main():
    api_id = '28036061'
    api_hash = '2400c765921b76f7cd50658053f0639b'
    phone = '+251974619952'
    channel_username = 'tikvahethiopia'
    keywords = [
        '#bank', '#CBE', '#BOA', '#GBE', '#CBO', '#ABAY', '#ABSC', '#ABYS', '#DASH', '#ENAT', '#BUNA',
        '#Commercial Bank of Ethiopia', '#bankofabyssinia', '#GlobalBankEthiopia', '#BoAEth', '#Coopbank', '#berhanbank'
    ]

    scraper = TelegramScraper(api_id, api_hash, phone, channel_username, keywords)
    client = await scraper.start()
    channel = await scraper.get_channel_entity(client)
    if channel:
        messages = await scraper.get_all_messages(client, channel)
        scraper.save_messages_to_csv(messages)
        await client.disconnect()


# Run the main function
import asyncio

asyncio.run(main())