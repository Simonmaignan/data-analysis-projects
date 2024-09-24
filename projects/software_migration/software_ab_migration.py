"""Script to migrate data from SoftwareA to SoftwareB"""

from pathlib import Path
from typing import Tuple

import pandas as pd


def files_path(
    migration_mapping_xlsx_file: str,
    csv_file_to_migrate: str,
    output_csv_file: str,
) -> Tuple[Path]:
    cur_file_path: Path = Path(__file__).parent

    return (
        cur_file_path / Path(migration_mapping_xlsx_file),
        cur_file_path / Path(csv_file_to_migrate),
        cur_file_path / Path(output_csv_file),
    )


def gather_data(
    migration_mapping_xlsx_file: str, csv_file_to_migrate: str
) -> Tuple[pd.DataFrame]:
    """Gather the data from given files and return the corresponding Pandas DataFrame"""
    migration_mapping_xlsx = pd.ExcelFile(migration_mapping_xlsx_file)
    migration_mapping_df = pd.read_excel(migration_mapping_xlsx, sheet_name=0)
    migration_mapping_df.head()
    data_to_migrate_df = pd.read_csv(csv_file_to_migrate)

    return migration_mapping_df, data_to_migrate_df


def clean_data(data_to_migrate_df: pd.DataFrame) -> pd.DataFrame:
    """Clean data to migrate"""
    clean_data_to_migrate_df = data_to_migrate_df.copy()
    clean_data_to_migrate_df["Duration"] = pd.to_timedelta(
        clean_data_to_migrate_df["Duration"]
    )
    return clean_data_to_migrate_df


def migrate_data(
    data_to_migrate_df: pd.DataFrame, migration_mapping_df: pd.DataFrame
) -> pd.DataFrame:
    migrated_data_df = data_to_migrate_df.copy()

    for _, row in migration_mapping_df.iterrows():
        field = row["Field"]
        if field == "CustomFields":
            migrated_data_df[field] = migrated_data_df[field].str.replace(
                row["SoftwareA"], row["SoftwareB"]
            )
        else:
            migrated_data_df[field] = migrated_data_df[field].replace(
                row["SoftwareA"], row["SoftwareB"]
            )

    return migrated_data_df


def group_data(data_to_group_df: pd.DataFrame) -> pd.DataFrame:
    grouped_data_df = data_to_group_df.groupby(
        ["id", "Channel", "Language", "CustomFields"], as_index=False
    ).sum()
    total_points_df = grouped_data_df.groupby("id").agg(
        TotalPointsGained=("PointsGained", "sum")
    )

    return grouped_data_df.merge(total_points_df, how="left", on="id")


def save_data(data_to_save_df: pd.DataFrame, output_csv_file: str) -> None:
    data_to_save_df.to_csv(output_csv_file, index=False)


def main() -> None:
    migration_mapping_xlsx_path, csv_to_migrate_path, output_csv_path = (
        files_path(
            migration_mapping_xlsx_file="dataset/Software_Migration_Mapping.xlsx",
            csv_file_to_migrate="dataset/data_to_migrate.csv",
            output_csv_file="dataset/migrated_data.csv",
        )
    )

    migration_mapping_df, data_to_migrate_df = gather_data(
        migration_mapping_xlsx_file=migration_mapping_xlsx_path,
        csv_file_to_migrate=csv_to_migrate_path,
    )

    cleaned_data_to_migrate_df: pd.DataFrame = clean_data(data_to_migrate_df)
    migrated_data_df: pd.DataFrame = migrate_data(
        data_to_migrate_df=cleaned_data_to_migrate_df,
        migration_mapping_df=migration_mapping_df,
    )
    final_data_df: pd.DataFrame = group_data(migrated_data_df)
    save_data(data_to_save_df=final_data_df, output_csv_file=output_csv_path)


if __name__ == "__main__":
    main()
