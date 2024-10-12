"""Helper functions for the data analysis"""

import os
from pathlib import Path
from typing import Dict, Optional

import kaggle

kaggle.api.authenticate()


def download_dataset_files(
    dataset_author: str,
    dataset_name: str,
    dataset_folder: Optional[str] = "dataset",
    force: Optional[bool] = False,
) -> None:
    """
    Download the files associated with a given Kaggle data set

    First check if the files associated with the Kaggle dataset are present locally and up to date
    Download the files using the Kaggle API if the local files are not up to date or if force flag is True

    Files are downloaded and unzipped inside the given dataset folder

    Args:
        dataset_author: the data set author
        dataset_name: the data set name
        dataset_folder: the folder where to save the files
        force: flag to force the download even if the files are up to date locally
    """
    dataset_full_name = f"{dataset_author}/{dataset_name}"
    os.makedirs(dataset_folder, exist_ok=True)

    print(f"Listing local csv files in ./{dataset_folder}.")
    local_files_dict: Dict[str, Path] = {}
    for path in Path(dataset_folder).iterdir():
        if path.is_file() and path.suffix == ".csv":
            print(
                f"File {path.name} with size {path.stat().st_size} found in ./{dataset_folder}"
            )
            local_files_dict[path.name] = path

    download_files = False
    print(f"Listing files associated with Kaggle dataset {dataset_full_name}.")
    for file in kaggle.api.datasets_list_files(dataset_author, dataset_name)[
        "datasetFiles"
    ]:
        file_name: str = file["name"]
        file_size: int = file["totalBytes"]
        print(
            f"File {file_name} with size {file_size} retrieved from Kaggle API."
        )
        if (
            file_name not in local_files_dict
            or file_size != local_files_dict[file_name].stat().st_size
        ):
            print(
                f"File {file_name} non existing locally or not having the same size."
            )
            download_files = True
            break

    if download_files or force:
        kaggle.api.dataset_download_files(
            dataset_name, path=dataset_folder, unzip=True, quiet=False
        )
