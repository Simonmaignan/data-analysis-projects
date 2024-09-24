"""Unit test file for the software_ab_migration.py script"""

from pathlib import Path
from typing import List
import unittest
from unittest.mock import call, MagicMock, patch

import software_ab_migration


class TestFilePath(unittest.TestCase):
    """Test class to test the software_ab_migration.files_path function"""

    def setUp(self) -> None:
        self.files_list: List[str] = ["file1.txt", "file_2.py"]
        self.file_path = Path(__file__).parent
        return super().setUp()

    def test_file_path(self):
        """Test the return values of the software_ab_migration.files_path function"""
        # Call
        files_paths: List[Path] = list(
            software_ab_migration.files_path(*self.files_list)
        )

        # Assert
        for i, file in enumerate(self.files_list):
            self.assertEqual(files_paths[i], self.file_path / Path(file))


class TestGatherData(unittest.TestCase):
    """Test class to test the software_ab_migration.gather_data function"""

    def setUp(self) -> None:
        self.xlsx_migration_mapping_path = "migration_mapping.xlsx"
        self.csv_to_migrate_path = "data_to_migrate.csv"
        self.csv_validation_data_path = "validation_data.csv"
        return super().setUp()

    @patch.object(software_ab_migration, "pd")
    def test_gather_data(self, mock_pd: MagicMock):
        """Test the software_ab_migration.gather_data function"""
        # Setup
        mock_excel_file = "excel_file"
        mock_pd.ExcelFile = MagicMock(return_value=mock_excel_file)
        mock_migration_mapping_df = "migration_mapping"
        mock_pd.read_excel = MagicMock(return_value=mock_migration_mapping_df)
        mock_data_to_migrate_df = "data to migrate"
        mock_validation_data_df = "validation data"
        mock_pd.read_csv = MagicMock(
            side_effect=[mock_data_to_migrate_df, mock_validation_data_df]
        )

        # Call
        migration_mapping_df, data_to_migrate_df, validation_data_df = (
            software_ab_migration.gather_data(
                xlsx_migration_mapping_path=self.xlsx_migration_mapping_path,
                csv_to_migrate_path=self.csv_to_migrate_path,
                csv_validation_data_path=self.csv_validation_data_path,
            )
        )

        # Assert
        mock_pd.ExcelFile.assert_called_with(self.xlsx_migration_mapping_path)
        mock_pd.read_excel.assert_called_with(mock_excel_file, sheet_name=0)
        self.assertEqual(migration_mapping_df, mock_migration_mapping_df)
        mock_pd.read_csv.assert_has_calls(
            [
                call(self.csv_to_migrate_path),
                call(self.csv_validation_data_path),
            ]
        )
        self.assertEqual(data_to_migrate_df, mock_data_to_migrate_df)
        self.assertEqual(validation_data_df, mock_validation_data_df)


# TODO: extend unit tests

if __name__ == "__main__":
    # begin the unittest.main()
    unittest.main()
