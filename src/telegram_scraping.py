from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
import pandas as pd

api_id = '28036061'
api_hash = '2400c765921b76f7cd50658053f0639b'
phone = '+251974619952'

client = TelegramClient('session_name', api_id, api_hash)
client.start(phone=phone)

channel_username = 'tikvahethiopia'
channel = client.get_entity(channel_username)

def get_all_messages(client, channel):
    all_messages = []
    offset_id = 0
    limit = 100
    total_messages = 0
    total_count_limit = 1000  # or some other limit you want to set

    while True:
        history = client(GetHistoryRequest(
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
            all_messages.append(message.to_dict())
        offset_id = messages[-1].id
        total_messages = len(all_messages)
        if total_count_limit and total_messages >= total_count_limit:
            break

    return all_messages

messages = get_all_messages(client, channel)

# Convert to DataFrame
df = pd.DataFrame(messages)
df.to_csv('telegram_channel_data.csv', index=False)

print("Data extraction completed successfully")
