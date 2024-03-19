import logging as log
import pandas as pd
from dateutil import parser
from azureml.opendatasets import NycTlcYellow

from src.utils import write_to_parquet_file


def ingest_nyc_tlc_yellow_data(start_date_str: str, end_date_str: str, landing_zone_file: str) -> pd.DataFrame:
    
    """Ingest NYC TLC Yellow taxi data for the specified date range.

    Args:
        start_date_str (str): The start date in string format (YYYY-MM-DD).
        end_date_str (str): The end date in string format (YYYY-MM-DD).
        landing_zone_path(str): Path to land data, file name

    Returns:
        pd.DataFrame: The ingested NYC TLC Yellow taxi data as a pandas DataFrame.

    Raises:
        Exception: If any error occurs during data ingestion.
    """

    try:
        start_date = parser.parse(start_date_str)
        end_date = parser.parse(end_date_str)

        log.info(f"Ingesting NYC TLC Yellow taxi data from {start_date} to {end_date}")

        nyc_tlc = NycTlcYellow(start_date=start_date, end_date=end_date)
        nyc_tlc_df = nyc_tlc.to_pandas_dataframe()
        
        write_to_parquet_file(nyc_tlc_df, landing_zone_file)
        
        log.info("Data ingested Succesfully")

        return nyc_tlc_df

    except Exception as e:
        log.info(f"Error while ingesting data: {e}")
        raise  Exception(f"Error while ingesting data: {e}")
    


def run(start_date_str: str, end_date_str: str, landing_zone_file: str) -> pd.DataFrame:
    """
    Ingest NYC TLC Yellow taxi data for the specified date range.

    Parameters:
    - start_date_str (str): The start date in string format (YYYY-MM-DD).
    - end_date_str (str): The end date in string format (YYYY-MM-DD).
    - landing_zone_path(str): Path to land data, file name.

    Returns:
    - ingested_df (pd.DataFrame): The ingested NYC TLC Yellow taxi data as a pandas DataFrame.
    """
    try:
        ingested_df = ingest_nyc_tlc_yellow_data(start_date_str, end_date_str, landing_zone_file)
        print("Data ingestion completed successfully.")
        print(ingested_df.head(5))
        return ingested_df

    except Exception as e:
        print(f"An error occurred during data ingestion: {e}")
