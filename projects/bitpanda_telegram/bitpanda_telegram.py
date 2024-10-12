"""Script to retrieve all the telegram messages from a given channel containing a given str"""

import datetime as dt
import json
from typing import Any, Dict, List, Optional, Union

from pathlib import Path
import pandas as pd
import pygsheets
import pygsheets.client
from telethon import types, TelegramClient


class TelegramAPI:
    """Telegram API for the bitpanda project"""

    def __init__(
        self, channel: str, keyword_to_search: Optional[str] = "btc"
    ) -> None:
        # Load secrets
        secret_file: Path = Path.home() / ".secrets.json"
        with open(secret_file, encoding="utf8") as secrets_file:
            self.__secrets: Dict[str, str] = json.load(secrets_file)[
                "telegram"
            ]
        # Connect to Telegram and create a client
        self.__client = TelegramClient(
            self.__secrets["session_name"],
            self.__secrets["api_id"],
            self.__secrets["api_hash"],
        )
        self.__channel: str = channel
        self.__keyword_to_search: str = keyword_to_search

    async def _read_telegram_messages(
        self, min_id: Optional[int] = 0, nb_days_in_past: Optional[int] = 31
    ) -> List[Dict[str, Union[dt.datetime, int, str]]]:
        """Async function to read the messages of interest from the telegram channel

        Args:
            min_id: the min_id of the messages to read
            nb_days_in_past: the number of days in the past
        """
        # Connect to the client
        await self.__client.start(phone=self.__secrets["phone_number"])

        # Get the channel entity
        channel: types.Channel = await self.__client.get_entity(self.__channel)

        # If no min id was passed, limit the messages to the ones nb_days_in_past from today
        min_datetime: Optional[dt.datetime] = None
        if min_id == 0:
            min_datetime = dt.datetime.now(dt.UTC) - dt.timedelta(
                days=nb_days_in_past
            )

        new_messages: List[Dict[str, Union[dt.datetime, int, str]]] = []
        # Retrieve messages containing 'btc'
        message: types.Message
        async for message in self.__client.iter_messages(
            channel, search=self.__keyword_to_search, min_id=min_id
        ):
            # Skip old message if a min datetime was set
            if min_datetime is not None and min_datetime > message.date:
                # Since the messages are returned in descending timestamp order (newest firs)
                # We can break the loop as soon as the first message too old is read
                # TODO: switch to logging instead of print
                print(f"Message too old ({message.date} < {min_datetime})")
                print(
                    "Breaking the iteration loop since all the following messages will be older."
                )
                break
            new_message: Dict[str, Any] = {
                "id": message.id,
                "date": message.date,
                "message": message.message,
            }
            print(
                f"{message.date}: {message.from_id.user_id} {message.message}"
            )
            new_messages.append(new_message)
        return new_messages

    def get_new_messages_df(self, min_id: Optional[int] = 0) -> pd.DataFrame:
        """Get the new messages from telegram in a Pandas DataFrame format

        Args:
            min_id: the min_id of the messages to retrieve
        """
        # Run the client
        with self.__client:
            # Create data frame from new messages
            return pd.DataFrame(
                self.__client.loop.run_until_complete(
                    self._read_telegram_messages(min_id=min_id)
                )
            )


class GoogleSheetAPI:
    """Google Sheet API for the bitpanda project"""

    def __init__(
        self,
        document_name: str,
        sheet_name: Optional[str] = "",
        secret_file_name: Optional[str] = "bitpanda-435910-1991f9531b28.json",
    ) -> None:
        secret_file_path: Path = (
            Path.home() / ".google_apis" / secret_file_name
        )
        g_sheets_api: pygsheets.client.Client = pygsheets.authorize(
            service_file=secret_file_path
        )

        # Open the google spreadsheet with given name
        spreadsheet: pygsheets.Spreadsheet = g_sheets_api.open(document_name)
        # And select the first work sheet
        # By default select the first one
        self.__worksheet: pygsheets.Worksheet = spreadsheet[0]
        # Retrieve the one by its name if given
        if sheet_name:
            self.__worksheet = spreadsheet.worksheet_by_title(sheet_name)

        self.__existing_messages_df = pd.DataFrame()
        self.__highest_existing_id: int = 0

    @property
    def highest_existing_id(self) -> int:
        """Property access to the highest id in the existing messages"""
        return self.__highest_existing_id

    def retrieve_existing_messages(self) -> pd.DataFrame:
        """Retrieve the existing messages from the worksheet in a Pandas DataFrame format"""
        # Retrieve existing data from google work sheet as pandas data frame
        self.__existing_messages_df: pd.DataFrame = (
            self.__worksheet.get_as_df()
        )
        print(self.__existing_messages_df.head())
        print(self.__existing_messages_df.info())
        self._validate_existing_messages()

    def _validate_existing_messages(self) -> None:
        """Validate the existing messages
        Check if the data frame contains exactly 3 columns
        and that they named as desired
        """
        # If google sheet data is not valid format
        if (
            "id" not in self.__existing_messages_df.columns
            or "date" not in self.__existing_messages_df.columns
            or "message" not in self.__existing_messages_df.columns
            or len(self.__existing_messages_df.columns) != 3
        ):
            # Reset it
            print(
                "Existing messages data frame is not is the right format. Resetting it."
            )
            self.__existing_messages_df = pd.DataFrame()
        else:
            # Cast to specific types
            self.__existing_messages_df["id"].astype(int)
            # TODO: improve the way the date column is saved and retrieved the google sheet (as string by default)
            self.__existing_messages_df["date"] = pd.to_datetime(
                self.__existing_messages_df["date"]
            )
            # Sort by id
            self.__existing_messages_df.sort_values(by="id", inplace=True)
            # Telegram Message id are increasing in time and
            # a min_id can be given on the telethon iter_message function
            # Thus we retrieve the highest existing id from the Google worksheet
            # to use it as min_id for the iter_message
            self.__highest_existing_id = self.__existing_messages_df[
                "id"
            ].max()

        print(self.__existing_messages_df.head())
        print(self.__existing_messages_df.info())
        print(self.__highest_existing_id)

    def update_worksheet(self, new_messages_df: pd.DataFrame) -> None:
        """Update the Google worksheet with a new set of messages

        Args:
            new_messages_df: a Panda data frame containing the new messages
        """
        # TODO: validate the new messages format before updating the worksheet
        if new_messages_df.empty:
            print(
                "New messages DataFrame is empty; Skipping Google Worksheet update"
            )
            return
        new_messages_df.sort_values(by="id", inplace=True)
        print(new_messages_df.head())
        print(new_messages_df.info())

        # Concat the existing and new messages data frame
        # This will fail if the new messages format do not match the existing format
        final_messages = pd.concat(
            [self.__existing_messages_df, new_messages_df], ignore_index=True
        )

        # Clear the worksheet from previous data
        self.__worksheet.clear()
        # Update the first sheet with df_new_messages, starting at cell A1.
        self.__worksheet.set_dataframe(final_messages, (1, 1))


def main() -> None:
    """Main function in init the Telegram and Google Sheet APIs and run the main functions"""
    telegram_api = TelegramAPI(channel="bitpanda_de")
    google_sheet_api = GoogleSheetAPI(document_name="Telegram_Bitpanda_de")

    google_sheet_api.retrieve_existing_messages()
    google_sheet_api.update_worksheet(
        telegram_api.get_new_messages_df(google_sheet_api.highest_existing_id)
    )


if __name__ == "__main__":
    main()
