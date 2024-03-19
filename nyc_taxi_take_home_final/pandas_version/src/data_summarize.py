import logging as log
import pandas as pd
from src.utils import read_parquet_file, write_to_parquet_file

def create_price_column(data_frame) -> pd.DataFrame:
    """
    Creates a new column named total_price representing the price for a taxi ride in addition to the fare amount.
    Created by sum of columns: 'extra', 'mtaTax', 'tipAmount', 'tollsAmount', 'improvementSurcharge'.
    
    Parameters:
    - data_frame (pd.DataFrame): The DataFrame containing the taxi ride data.

    Returns:
    - pd.DataFrame: The DataFrame with the additional column representing the total price.
    """
    columns_to_sum = ['extra', 'mtaTax', 'tipAmount', 'tollsAmount', 'improvementSurcharge']
    
    try:
        # Add column showing price beside the actual fare
        data_frame['additional_cost'] = data_frame[columns_to_sum].sum(axis=1)
        return data_frame
    
    except KeyError as e:
        raise KeyError(f"One or more columns are missing: {e}")
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
    

def nyc_taxi_data_statistics(data_frame)-> pd.DataFrame:
    """
    Aggregate NYC taxi data based on specified columns and aggregation functions.

    Parameters:
    - data_frame (DataFrame): The input DataFrame containing NYC taxi data.

    Returns:
    - aggregated_df (DataFrame): The aggregated DataFrame.
    """
    try:
        # Define groupby columns
        data_frame['pickup_year'] = data_frame['tpepPickupDateTime'].dt.year
        data_frame['pickup_month'] = data_frame['tpepPickupDateTime'].dt.month

        groupby_cols = ['paymentType', 'pickup_year', 'pickup_month']

        # Define aggregation functions for each column
        aggregation_types = {
            'passengerCount': ['mean', 'median'],
            'totalAmount': ['mean', 'median'],
            'additional_cost': ['mean', 'median']  
        }

        # Perform aggregation
        nyc_taxi_stat_df = data_frame.groupby(groupby_cols).agg(aggregation_types).sort_values(by=groupby_cols).reset_index()

        return nyc_taxi_stat_df
    
    except KeyError as e:
        raise KeyError(f"One or more required columns are missing: {e}")
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
    

def run(silver_layer_file: str, gold_layer_file: str):
    """
    Calculates statistics on NYC taxi data and write the result to the gold layer in parquet format.

    Parameters:
    - input_file_path (str): Path to the input Parquet file containing NYC taxi data.
    - gold_layer_path (str): Path to write the output Parquet file for the gold layer.

    Returns:
    - None
    """
    try:
        # Read data from the input file
        data_frame = read_parquet_file(silver_layer_file)

        # Create a new column for total price
        cost_df = create_price_column(data_frame)

        # Calculate NYC taxi data statistics
        nyc_statistics = nyc_taxi_data_statistics(cost_df)

        # Print the first few rows of the statistics
        print(nyc_statistics.head())

        # Write the statistics to the gold layer
        write_to_parquet_file(nyc_statistics, gold_layer_file)
        
        print("Data processing completed successfully.")
    except Exception as e:
        print(f"An error occurred during data processing: {e}")






    