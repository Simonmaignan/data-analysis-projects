"""Script to retrieve all the telegram messages from a given channel containing a given str"""

import datetime as dt
import json
from typing import Any, Dict, List, Optional

from pathlib import Path
import pandas as pd
import pygsheets
import pygsheets.client
from telethon import types, TelegramClient

# API to Telegram
TELEGRAM_SECRET_FILE: Path = Path.home() / ".telegram" / "secrets.json"
with open(TELEGRAM_SECRET_FILE, encoding="utf8") as secrets_file:
    secrets: Dict[str, str] = json.load(secrets_file)

# Connect to Telegram
client = TelegramClient(
    secrets["session_name"], secrets["api_id"], secrets["api_hash"]
)

TELEGRAM_CHANNEL = "bitpanda_de"

# API to Google Sheet
GOOGLE_CLOUD_SECRET_FILE: Path = (
    Path.home() / ".google_apis" / "bitpanda-435910-1991f9531b28.json"
)
g_sheets_api: pygsheets.client.Client = pygsheets.authorize(
    service_file=GOOGLE_CLOUD_SECRET_FILE
)

# Open the google spreadsheet (where 'PY to Gsheet Test' is the name of my sheet)
GOOGLE_SHEET_NAME = "Telegram_Bitpanda_de"
g_spreadsheet: pygsheets.Spreadsheet = g_sheets_api.open(GOOGLE_SHEET_NAME)
# And select the first work sheet
g_worksheet: pygsheets.Worksheet = g_spreadsheet[0]

# Retrieve existing data from google work sheet as pandas data frame
existing_messages_df: pd.DataFrame = g_worksheet.get_as_df(
    value_render=pygsheets.ValueRenderOption.UNFORMATTED_VALUE
)
print(existing_messages_df.head())
print(existing_messages_df.info())

# If google sheet data is not valid
highest_existing_id: int = 0
if (
    "id" not in existing_messages_df.columns
    or "date" not in existing_messages_df.columns
    or "message" not in existing_messages_df.columns
):
    # Reset it
    existing_messages_df = pd.DataFrame()
else:
    # Cast to specific types
    existing_messages_df["id"].astype(int)
    existing_messages_df["date"] = pd.to_datetime(existing_messages_df["date"])
    # Sort by id
    existing_messages_df.sort_values(by="id", inplace=True)
    # Telegram Message id are increasing in time and a min_id can be given on the telethon iter_message function
    # Thus we retrieve the highest existing id from the Google worksheet to use it as min_id for the iter_message
    highest_existing_id = existing_messages_df["id"].max()

print(existing_messages_df.head())
print(existing_messages_df.info())
print(highest_existing_id)

# exit()

new_messages: List[Dict[str, Any]] = []


async def read_telegram_messages(
    min_id: int = 0, nb_days_in_past: Optional[int] = 31
):
    """Async function to read the messages of interest from the telegram channel"""
    # Connect to the client
    await client.start(phone=secrets["phone_number"])

    # Get the channel entity
    channel: types.Channel = await client.get_entity(TELEGRAM_CHANNEL)

    # If no min id was passed, limit the messages to the ones nb_days_in_past from today
    min_datetime: Optional[dt.datetime] = None
    if min_id == 0:
        min_datetime = dt.datetime.now(dt.UTC) - dt.timedelta(
            days=nb_days_in_past
        )

    # Retrieve messages containing 'btc'
    message: types.Message
    async for message in client.iter_messages(
        channel, search="btc", min_id=min_id
    ):
        # Skip old message if a min datetime was set
        if min_datetime is not None and min_datetime > message.date:
            continue
        new_message: Dict[str, Any] = {
            "id": message.id,
            "date": message.date,
            "message": message.message,
        }
        print(f"{message.date}: {message.from_id.user_id} {message.message}")
        new_messages.append(new_message)


# Run the client
with client:
    client.loop.run_until_complete(
        read_telegram_messages(min_id=highest_existing_id)
    )

# Create data frame from new messages
new_messages_df = pd.DataFrame(new_messages)
new_messages_df.sort_values(by="id", inplace=True)
print(new_messages_df.head())
print(new_messages_df.info())

final_messages = pd.concat(
    [existing_messages_df, new_messages_df], ignore_index=True
)


# Clear the worksheet from previous data
g_worksheet.clear()
# Update the first sheet with df_new_messages, starting at cell A1.
g_worksheet.set_dataframe(final_messages, (1, 1))
