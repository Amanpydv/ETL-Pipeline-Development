
# ETL Pipeline Development

## Objective
Design and implement an ETL (Extract, Transform, Load) pipeline that:
- Extracts data from multiple CSV files.
- Applies transformations based on business rules.
- Loads the data into a PostgreSQL database.
- Incorporates robust error handling and logging.

---

## Setup

### Required Libraries
Install the necessary Python libraries:
```bash
pip3 install pandas
pip3 install numpy
pip3 install psycopg2
```

### PostgreSQL Installation
1. Download and install PostgreSQL [here](https://www.postgresql.org/download/) based on your OS.
2. (Optional) Install the **PostgreSQL extension** by Chris Kolkman in VSCode for easier database management.

---

## Tech Stack Used
- **Python**: For data extraction, transformation, and loading.
- **PostgreSQL**: For database management.
- **Pandas and Numpy**: For data manipulation and transformation.
- **psycopg2**: For PostgreSQL connectivity.

---

## Steps to Run the Pipeline

1. **Open `pipeline.py`**:
   - Use your preferred code editor (e.g., Visual Studio Code).
   
2. **Run the pipeline**:
   - Use the command: 
     ```bash
     python3 pipeline.py
     ```
   - Ensure your terminal is in the correct project directory.

---

## ETL Process

### 1. **Data Extraction**
   - Use the `load_data()` function to extract and validate data from `sales_data.csv` and `customer_data.csv`.
   - Example validation:
     - Check for missing or incorrect columns.
     - Log errors for debugging.

### 2. **Data Transformation**
   - **Data Cleaning**:
     - Format dates (`order_date`, `signup_date`) to `YYYY-MM-DD`. Skip invalid records.
     - Remove rows with invalid `customer_id` or `order_id`.
   - **Data Enrichment**:
     - Add columns like `total_value`, `order_type`, and `customer_tenure`.
     - Merge sales data with customer data.
   - **Business Rules**:
     - Exclude orders with `quantity <= 0`.
     - Tag orders with `total_value > $1000` as "High-Value Orders".
   - **Data Aggregation**:
     - Create a summary table with:
       - Total sales per product (`SUM(total_value)`).
       - Order counts (`COUNT(order_id)`).

### 3. **Data Loading**
   - Create tables in PostgreSQL:
     - `sales_data`, `customer_data`, `sales_summary`, and `sales_enriched_data`.
   - Use the `COPY` command to load transformed CSV data into PostgreSQL tables.

---

## PostgreSQL Table Schema

### `sales_data` Table
```sql
CREATE TABLE sales_data (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    product VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_date DATE,
    total_value DECIMAL(10, 2) NOT NULL,
    order_type VARCHAR(50)
);
```

### `sales_summary` Table
```sql
CREATE TABLE sales_summary (
    product VARCHAR(255) PRIMARY KEY,
    total_sales DECIMAL(10, 2) NOT NULL,
    order_count INT NOT NULL
);
```

### `customer_data` Table
```sql
CREATE TABLE customer_data (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    signup_date DATE,
    customer_tenure VARCHAR(255) DEFAULT NULL
);
```

### `sales_enriched_data` Table
```sql
CREATE TABLE sales_enriched_data (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    product VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_date DATE,
    total_value DECIMAL(10, 2) NOT NULL,
    order_type VARCHAR(50),
    customer_name VARCHAR(255),
    email VARCHAR(255),
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customer_data(customer_id) ON DELETE CASCADE
);
```

---

## Challenges and Resolutions

1. **Column Mismatch Errors**:
   - Issue: Validation script was incorrectly cross-checking unrelated columns.
   - Resolution: Added conditional checks to validate columns only for relevant files.

2. **Invalid Date Formatting**:
   - Issue: Errors when formatting invalid dates.
   - Resolution: Used `errors='coerce'` to convert invalid dates to `NaN`.

3. **PostgreSQL Data Loading Errors**:
   - Issue: Read permission issues while copying files.
   - Resolutions:
     1. Moved transformed files to `/tmp` for easier access.
     2. Used PgAdmin 4 to directly export data into tables.

---

## How to Validate Results
1. View the loaded tables using PostgreSQL queries:
   ```sql
   SELECT * FROM sales_data;
   SELECT * FROM customer_data;
   SELECT * FROM sales_summary;
   SELECT * FROM sales_enriched_data;
   ```

2. Cross-check results with the transformed CSV files.

---

## Summary

This ETL pipeline:
1. Extracts and validates raw data from CSV files.
2. Transforms data to meet business rules.
3. Loads cleaned and enriched data into PostgreSQL tables.

