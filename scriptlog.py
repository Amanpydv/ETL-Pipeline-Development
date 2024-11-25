import psycopg2
from psycopg2 import sql

# Database connection details
db_config = {
    'dbname': 'sales_db',
    'user': 'postgres',
    'password': '1507',
    'host': 'localhost',
    'port': '5432'
}

# File paths for CSVs
files = {
    'sales_summary': '/private/tmp/summary_table.csv',
    'sales_data': '/private/tmp/transformed_sales_data.csv',
    'customer_data': '/private/tmp/transformed_customer_data.csv',
    'sales_enriched_data': '/private/tmp/enriched_sales_df.csv'
}

def copy_data(cursor, table_name, file_path):
    try:
        # Execute the COPY command
        copy_query = sql.SQL("""
            COPY {table} FROM %s
            DELIMITER ',' 
            CSV HEADER
            ENCODING 'UTF8';
        """).format(table=sql.Identifier(table_name))
        
        cursor.execute(copy_query, [file_path])
        
        # Get the number of rows loaded using the `GET DIAGNOSTICS` SQL command
        cursor.execute("GET DIAGNOSTICS row_count = ROW_COUNT;")
        row_count = cursor.fetchone()[0]
        
        print(f"Table '{table_name}': {row_count} records loaded successfully.")
        return row_count
    except Exception as e:
        print(f"Error loading table '{table_name}': {e}")
        return -1

def main():
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        total_records_processed = 0
        total_records_loaded = 0
        
        # Process each table and file
        for table, file_path in files.items():
            print(f"Processing table '{table}'...")
            records_loaded = copy_data(cursor, table, file_path)
            
            if records_loaded >= 0:
                total_records_processed += records_loaded
                total_records_loaded += records_loaded
            else:
                print(f"Errors encountered while processing table '{table}'.")
        
        # Final summary
        print(f"Total records processed: {total_records_processed}")
        print(f"Total records loaded: {total_records_loaded}")
    
    except Exception as ex:
        print(f"Database connection error: {ex}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
