import pandas as pd

def read_parquet_file(file_path):
    """
    Read data from a Parquet file and return a pandas DataFrame.

    Parameters:
    - file_path (str): The path to the Parquet file.
    
    Returns:
    - df (DataFrame): The loaded DataFrame.
    """
    try:
        # Read data from Parquet file
        df = pd.read_parquet(file_path)
        return df
    
    except Exception as e:
        # Handle file reading errors
        print(f"Error reading data from '{file_path}': {str(e)}")
        return None

    
def write_to_parquet_file(df, file_path):
    """
    Read data from a Parquet file and return a pandas DataFrame.

    Parameters:
    - file_path (str): The path to the Parquet file.
    
    Returns:
    - df (DataFrame): The loaded DataFrame.
    """
    try:
        # Read data from Parquet file
        df.to_parquet(file_path)
    
    except Exception as e:
        # Handle file reading errors
        print(f"Error writing data from '{file_path}': {str(e)}")
