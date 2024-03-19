import logging as log
import pandas as pd
from src.utils import write_to_parquet_file, read_parquet_file


def remove_negative_total_amount(data_frame) -> pd.DataFrame:
    """
    Remove rows from the DataFrame where 'totalAmount' for taxi fare is negative.

    Parameters:
    - data_frame (DataFrame): The input DataFrame.

    Returns:
    - cleaned_df (DataFrame): The DataFrame with rows removed.
    """
    try:
        # Create a mask to identify rows where 'totalAmount' is negative
        mask = data_frame["totalAmount"] < 0

        cleaned_df = data_frame[~mask]

        return cleaned_df

    except KeyError as e:
        # Handle KeyError (column not found)
        print(f"Error: {e}. 'totalAmount' column not found in the DataFrame.")


def enforce_numerical_dtypes(data_frame) -> pd.DataFrame:
    """
  Enforces numerical data types (int or float) for specified columns in a DataFrame.

  Args:
      data_frame (pd.DataFrame): The DataFrame to be checked and potentially modified.

  Returns:
      pd.DataFrame: A new DataFrame with the specified columns converted to numerical types
          (if possible) or raising informative errors.

  Raises:
      AssertionError: If a column cannot be converted to a numerical type
          or if the data frame is not a pandas DataFrame.
        
  """
    # numerical_columns (list[str]): A list of column names expected to be numerical.

    numerical_columns = [
        "fareAmount",
        "extra",
        "mtaTax",
        "improvementSurcharge",
        "tipAmount",
        "tollsAmount",
        "totalAmount",
    ]

    # Loop through columns
    for column in numerical_columns:
        # Check and potentially convert data type
        if data_frame[column].dtype not in ("int", "float"):
            try:
                data_frame.loc[:, column] = data_frame[column].astype("float64")
            except ValueError as e:
                raise AssertionError(
                    f"Cannot convert column '{column}' to a numerical type: {e}"
                )

    # Return the modified DataFrame
    return data_frame


def run(bronze_layer_file: str, silver_layer_file: str):
    """
    Perform pre-processing on the data from the bronze layer and write the result to the silver layer.

    Parameters:
    - bronze_layer_path (str): Path to the input Parquet file containing data from the bronze layer.
    - silver_layer_path (str): Path to write the output Parquet file for the silver layer.

    Returns:
    - None
    """
    try:
        # Read data from the bronze layer
      raw_data = read_parquet_file(bronze_layer_file)

        # Remove rows with negative total taxi fare amount
      positive_total_amount_df = remove_negative_total_amount(raw_data)


        # Enforce numerical data types for columns used in summarization
      numerical_enforced_df = enforce_numerical_dtypes(positive_total_amount_df)

        # Write the pre-processed data to the silver layer
      write_to_parquet_file(numerical_enforced_df, silver_layer_file)

      print("Pre-processing completed successfully.")    
    except Exception as e:
      print(f"An error occurred during pre-processing: {e}")
