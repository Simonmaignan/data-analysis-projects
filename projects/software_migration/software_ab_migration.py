"""Script to migrate data from SoftwareA to SoftwareB"""

import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

logger = logging.getLogger("Software AB Migration")
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)


def files_path(*args) -> Tuple[Path]:
    """Transform relative string path to files into absolute Path objects

    Args
        The 3 files relative path as str
    Returns
        a tuple of the 3 file absolute Path
    """
    # Retrieve current file absolute path
    cur_file_path: Path = Path(__file__).parent
    # Return absolute path for each give args
    return (cur_file_path / Path(arg) for arg in args)


def gather_data(
    xlsx_migration_mapping_path: Path,
    csv_to_migrate_path: Path,
    csv_validation_data_path: Path,
) -> Tuple[pd.DataFrame]:
    """Gather the data from given files and return the corresponding Pandas DataFrame

    Args
        migration_mapping_xlsx_path: the absolute Path of the migration mapping XLSX file
        csv_to_migrate_path: the absolute Path of the data to migrate csv file
        csv_validation_data_path: the absolute Path of the validation data csv file

    Returns
        The migration mapping, the data to migrate and the validation data frames as a Tuple
    """
    logger.info("Gathering the data frames from the files.")
    migration_mapping_xlsx = pd.ExcelFile(xlsx_migration_mapping_path)
    migration_mapping_df = pd.read_excel(migration_mapping_xlsx, sheet_name=0)

    data_to_migrate_df = pd.read_csv(csv_to_migrate_path)

    validation_data_df = pd.read_csv(csv_validation_data_path)

    return migration_mapping_df, data_to_migrate_df, validation_data_df


def clean_data(data_to_clean_df: pd.DataFrame) -> pd.DataFrame:
    """Clean data to migrate

    Args
        data_to_migrate_df: The data frame to clean

    Returns
        The cleaned data frame to migrate
    """
    logger.info(
        f"Cleaning the data frame with {len(data_to_clean_df)} rows and columns={list(data_to_clean_df.columns)}."
    )
    clean_data_to_migrate_df = data_to_clean_df.copy()

    # Cast the Duration field to Timedelta
    clean_data_to_migrate_df["Duration"] = pd.to_timedelta(
        clean_data_to_migrate_df["Duration"]
    )
    return clean_data_to_migrate_df


def migrate_data(
    data_to_migrate_df: pd.DataFrame, migration_mapping_df: pd.DataFrame
) -> pd.DataFrame:
    """Migrate the data from SoftwareA to SoftwareB

    Args
        data_to_migrate_df: the data frame to migrate
        migration_mapping_df: the data frame containing the migration mapping

    Returns
        The migrated data frame
    """
    logger.info(
        "Migrate the data frames from SoftwareA to SoftwareB using migration mapping."
    )
    migrated_data_df = data_to_migrate_df.copy()

    # Loop over each migration rule
    for _, row in migration_mapping_df.iterrows():
        field = row["Field"]
        logger.debug(
            f"Replacing field '{field}' from '{row['SoftwareA']}' to '{row['SoftwareB']}'"
        )
        # Special case for CustomFields since the string to replace is not the entire value
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
    """Group the data and compute extra fields

    Args
        data_to_group_df: the data frame to group

    Return
        The grouped data with the extra computed fields
    """
    logger.info("Migrate the data and compute extra fields.")
    grouped_data_df = data_to_group_df.groupby(
        ["id", "Channel", "Language", "CustomFields"], as_index=False
    ).sum()
    total_points_df = grouped_data_df.groupby("id").agg(
        TotalPointsGained=("PointsGained", "sum")
    )

    return grouped_data_df.merge(total_points_df, how="left", on="id")


def validate_data(
    data_to_validate_df: pd.DataFrame, validation_data_df: pd.DataFrame
) -> None:
    logger.info("Validate the data.")
    if not data_to_validate_df.columns.equals(validation_data_df.columns):
        raise AttributeError(
            "Dataframe does not have the right columns count and names"
        )

    if not data_to_validate_df.dtypes.equals(validation_data_df.dtypes):
        raise AttributeError(
            "Dataframe does not have the right columns count and names"
        )


def save_data(data_to_save_df: pd.DataFrame, output_csv_path: Path) -> None:
    """Save the data frame to a csv file

    Args
        data_to_save_df: the data frame to save
        output_csv_file: the absolute Path of the output csv file
    """
    logger.info(
        f"Save the data with {len(data_to_save_df)} record to csv file."
    )
    data_to_save_df.to_csv(output_csv_path, index=False)


def main() -> None:
    """Main migration function"""
    # Transform file path from relative string to absolute Path
    (
        migration_mapping_xlsx_path,
        csv_to_migrate_path,
        csv_output_path,
        csv_validation_data_path,
    ) = files_path(
        "dataset/Software_Migration_Mapping.xlsx",
        "dataset/data_to_migrate.csv",
        "dataset/migrated_data.csv",
        "dataset/solution_migrated_data.csv",
    )

    migration_mapping_df, data_to_migrate_df, validation_data_df = gather_data(
        xlsx_migration_mapping_path=migration_mapping_xlsx_path,
        csv_to_migrate_path=csv_to_migrate_path,
        csv_validation_data_path=csv_validation_data_path,
    )

    # Clean data to migration and validation data
    cleaned_data_to_migrate_df: pd.DataFrame = clean_data(data_to_migrate_df)
    validation_data_df: pd.DataFrame = clean_data(validation_data_df)

    migrated_data_df: pd.DataFrame = migrate_data(
        data_to_migrate_df=cleaned_data_to_migrate_df,
        migration_mapping_df=migration_mapping_df,
    )

    final_data_df: pd.DataFrame = group_data(migrated_data_df)

    validate_data(
        data_to_validate_df=final_data_df,
        validation_data_df=validation_data_df,
    )

    save_data(data_to_save_df=final_data_df, output_csv_path=csv_output_path)


if __name__ == "__main__":
    main()
