import pandas as pd
import numpy as np
from datetime import datetime

# Define required columns for sales and customer data
required_sales_columns = ['order_id', 'customer_id', 'quantity', 'order_date']
required_customer_columns = ['customer_id', 'customer_name', 'email', 'signup_date']

# Load the CSV data into Pandas DataFrames
def load_data(file_path):
    try:
        # Load the data into a DataFrame
        data = pd.read_csv(file_path)

        # Log data for debugging
        print("\n",data)

        # Validate columns based on file type
        if 'sales_data.csv' in file_path:
            missing_columns = [col for col in required_sales_columns if col not in data.columns]
            if missing_columns:
                print(f"Error: The following columns are missing in '{file_path}': {', '.join(missing_columns)}")
                return None
        elif 'customer_data.csv' in file_path:
            missing_columns = [col for col in required_customer_columns if col not in data.columns]
            if missing_columns:
                print(f"Error: The following columns are missing in '{file_path}': {', '.join(missing_columns)}")
                return None
        else:
            print(f"Error: Unsupported file '{file_path}'")
            return None

        return data

    except FileNotFoundError:
        print(f"File {file_path} is missing")
    except Exception as e:
        print(f"Error loading data from '{file_path}': {e}")

    return None

# List of files to process
files = ['sales_data.csv', 'customer_data.csv']

sales_df = None
customer_df = None

# Process each file
for file in files:
    df = load_data(file)
    if df is not None:
        print(f"\n File Loaded and Validated Successfully: {file}\n")