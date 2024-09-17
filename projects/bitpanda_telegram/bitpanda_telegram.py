import json
from typing import Dict

from pathlib import Path
import pandas as pd
import pygsheets
from telethon import TelegramClient

TELEGRAM_SECRET_FILE = Path.home() / ".telegram" / "secrets.json"
with open(TELEGRAM_SECRET_FILE, encoding="utf8") as secrets_file:
    secrets: Dict[str, str] = json.load(secrets_file)

GOOGLE_CLOUD_SECRET_FILE = (
    Path.home() / ".google_apis" / "bitpanda-435910-1991f9531b28.json"
)
gc = pygsheets.authorize(service_file=GOOGLE_CLOUD_SECRET_FILE)

# Connect to Telegram
client = TelegramClient(
    secrets["session_name"], secrets["api_id"], secrets["api_hash"]
)

TELEGRAM_CHANNEL = "bitpanda_de"


async def main():
    # Connect to the client
    await client.start(phone=secrets["phone_number"])

    # Get the channel entity
    channel = await client.get_entity(TELEGRAM_CHANNEL)

    # Retrieve messages
    async for message in client.iter_messages(
        channel, search="btc", limit=10
    ):  # Change the limit as needed
        print(f"{message.date}: {message.sender_id} {message.text}")
        print(message)


# Run the client
with client:
    client.loop.run_until_complete(main())

# Create empty dataframe
df = pd.DataFrame()

# Create a column
df["name"] = ["John", "Steve", "Sarah"]

# open the google spreadsheet (where 'PY to Gsheet Test' is the name of my sheet)
sh = gc.open("Telegram_Bitpand_de")

# select the first sheet
wks = sh[0]

# update the first sheet with df, starting at cell B2.
wks.set_dataframe(df, (1, 1))
