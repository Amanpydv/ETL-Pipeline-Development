-- Create the sales table
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

-- Create the sales_summary table
CREATE TABLE sales_summary (
    product VARCHAR(255) PRIMARY KEY,
    total_sales DECIMAL(10, 2) NOT NULL,
    order_count INT NOT NULL
);

--Create customer_data table
CREATE TABLE customer_data (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    signup_date DATE, 
    customer_tenure VARCHAR(255) DEFAULT NULL
);

--Create the sales_enriched_data
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

--Copied the data from the csv file to our table
COPY sales_summary FROM '/private/tmp/summary_table.csv'
DELIMITER ',' 
CSV HEADER
ENCODING 'UTF8';

COPY sales_data FROM '/private/tmp/transformed_sales_data.csv'
DELIMITER ',' 
CSV HEADER
ENCODING 'UTF8';

COPY customer_data FROM '/private/tmp/transformed_customer_data.csv'
DELIMITER ',' 
CSV HEADER
ENCODING 'UTF8';

COPY sales_enriched_data FROM '/private/tmp/enriched_sales_df.csv'
DELIMITER ',' 
CSV HEADER
ENCODING 'UTF8';

SELECT * FROM sales_data;
SELECT * FROM customer_data;
SELECT * FROM sales_summary;
SELECT * FROM sales_enriched_data;