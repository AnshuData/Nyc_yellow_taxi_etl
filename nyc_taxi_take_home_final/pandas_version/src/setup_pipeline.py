import logging as log
from pathlib import Path


DATA_DIRS = [
    "landing_zone",
    "silver_layer",
    "gold_layer",
    "bronze_layer",
]


def create_directory(directory: Path) -> None:
    """
    Create a directory if it does not exist
    """
    if not directory.exists():
        print(f"Directory {directory} doesn't exist, creating...")
        directory.mkdir(parents=True)


def setup_data_dirs() -> None:
    """
    Create the data directories for storing raw data and summarized data if they do not exist
    """
    proj_path = Path("./")
    for dir in DATA_DIRS:
        data_dir = proj_path / f"data/{dir}"
        create_directory(data_dir)


def setup_logs_dir() -> None:
    """
    Create the logs directory if it does not exist for storing pipeline logs
    """
    log_dir = Path("./") / "logs"
    create_directory(log_dir)


def setup_log():
    """
    setup logging for the code
    """
    print("Setting up the logging for NYC data pipeline")
    log.basicConfig(
        level=log.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[log.FileHandler("logs/nyc_data_pipeline.log")],
    )


def run() -> None:
    """
    Run setup tasks to setup  data and logs directories as well as logging
    """
    setup_data_dirs()
    setup_logs_dir()
    setup_log()
