import pandas as pd
import logging as log
from src.utils import write_to_parquet_file, read_parquet_file

def remove_null_columns(data_frame) -> pd.DataFrame:
    """
    Remove those columns that contain all null values from the DataFrame.

    Parameters:
    - data_frame (DataFrame): The input DataFrame.

    Returns:
    - cleaned_df (DataFrame): The DataFrame with null columns removed.
    """
    try:
        cleaned_df = data_frame.dropna(axis=1, how='all')
    except Exception as e:
        print(f"An error occurred while removing null columns: {e}")
    
    return cleaned_df
                          

def filter_nyc_taxi_data(data_frame) -> pd.DataFrame:
    """
    Filter the DataFrame to keep only relevant columns related to NYC taxi data.

    Parameters:
    - data_frame (DataFrame): The input DataFrame containing NYC taxi data.

    Returns:
    - filtered_df (DataFrame): The DataFrame with only relevant columns.
    """
    try:
        filtered_df = data_frame[['vendorID', 'tpepPickupDateTime', 'tpepDropoffDateTime',
                                  'passengerCount', 'rateCodeId', 'paymentType', 'fareAmount',
                                  'extra', 'mtaTax', 'improvementSurcharge', 'tipAmount',
                                  'tollsAmount', 'totalAmount']]
    except Exception as e:
        print(f"An error occurred while filtering the DataFrame: {e}")
        
    
    return filtered_df



def remove_duplicate_rows(data_frame) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.

    Parameters:
    - data_frame (DataFrame): The input DataFrame.

    Returns:
    - cleaned_df (DataFrame): The DataFrame with duplicate rows removed.
    """
    try:
        no_duplicates_df = data_frame.drop_duplicates()

    except Exception as e:
        print(f"An error occurred while removing duplicate rows: {e}")
    
    return no_duplicates_df


def run(landing_zone_file: str, bronze_layer_file: str) -> None:
    """
    This function takes the cleaned data from the bronze layer as input,
    Performs preprocessing by removing rows where the total amount for taxi fare is negative,
    Enforces that the columns used for summarization in the final steps are numerical to perform calculations.
    Writes the output to the silver layer in parquet format.

    Parameters:
    - bronze_layer_path (str): The file path to the cleaned data in the bronze layer.
    - silver_layer_path (str): The file path to write the output data to in the silver layer.

    Returns:
    - None
    """
    try:
        # Read data from the bronze layer
        raw_data = read_parquet_file(landing_zone_file)
        
        # Remove columns with all null values
        cleaned_df = remove_null_columns(raw_data)
        
        # remove duplicated rows
        no_duplicates_df = remove_duplicate_rows(cleaned_df)
        
        # Write the output to the silver layer in parquet format
        write_to_parquet_file(no_duplicates_df, bronze_layer_file)
        
        print("Data preprocessing completed successfully.")
    except Exception as e:
        print(f"An error occurred during data preprocessing: {e}")
