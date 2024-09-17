import json
from pathlib import Path
from typing import Dict

from telethon import TelegramClient

TELEGRAM_SECRET_FILE = Path.home() / ".telegram" / "secrets.json"
with open(TELEGRAM_SECRET_FILE, encoding="utf8") as secrets_file:
    secrets: Dict[str, str] = json.load(secrets_file)

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
