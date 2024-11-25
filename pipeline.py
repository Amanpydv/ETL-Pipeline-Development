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
        #print("\n",data)

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
        #print(f"\n File Loaded and Validated Successfully: {file}\n")
        if file == 'sales_data.csv':
            sales_df = df
            sales_df['order_date']= pd.to_datetime(sales_df['order_date'],errors='coerce').dt.date #converting order_date in sales table to datetime format
            sales_df['order_id'] = pd.to_numeric(sales_df['order_id'], errors='coerce')
            sales_df['customer_id'] = pd.to_numeric(sales_df['customer_id'], errors='coerce')
            sales_df_clean = sales_df.dropna(subset=['order_id', 'customer_id']) #Remove any invalid customer id and order id
            sales_df['total_value'] = sales_df['quantity'] * sales_df['price'] #Calculating total value of each order
            #print("\n Successfully Formatted Order_Date and Checked for any invalid customer and order id: \n\n",sales_df)
            print("\nTables after data enrichment \n\n",sales_df)
        if file== 'customer_data.csv':
            customer_df = df
            customer_df['signup_date'] = pd.to_datetime(customer_df['signup_date'], errors='coerce').dt.date #converting signup_date in customer table to datetime format
            customer_df['customer_id'] = pd.to_numeric(customer_df['customer_id'], errors='coerce')
            customer_df_clean = customer_df.dropna(subset=['customer_id']) #Remove any invalid customer id
            #print("\n Successfully Formatted Signup_Date and Checked for any invalid customer id: \n\n",customer_df)

#Join the sales data with customer data on customer_id to enrich the sales table with customer_name and email.            
if sales_df is not None and customer_df is not None:
    enriched_sales_df = pd.merge(sales_df, customer_df[['customer_id', 'customer_name', 'email']], 
                                 on='customer_id', how='left')
    print("\nEnriched Sales_df\n\n",enriched_sales_df)