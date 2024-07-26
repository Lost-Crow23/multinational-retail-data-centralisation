# Multinational-Retail-Data-Centralisation

## Table of Contents

- Description
- Prerequisites
- Usage Instructions
  - Milestone 1
  - Milestone 2
    - Extracting and Cleaning the User Data
    - Extracting and Cleaning the Card Data
    - Extracting and Cleaning the Store Data
    - Extracting and Cleaning the Products Data
    - Extracting and Cleaning the Orders Data
    - Extracting and Cleaning the Order Date Data
    - Uploading all the cleaned data to local database
  - Milestone 3
    - Database Star Schema
    - Data Type Changes
    - Primary Keys and Foreign Keys
  - Milestone 4
    - SQL Data Queries
  - File Structure
  - License Information

## Description

In this project, which is scenario based project set by AiCore forming part of the data engineering bootcamp. This is aimed to build
skills in data extraction and cleaning from multiple sources in python before uploading the data to a local Postgres database.

Scenerio:
Working for a Multinational database company that sells various goods across the globe. Currently, their sales data is spread across many different data sources, making it not easily accessible or analysable by the current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

First initial goal will be to product a system that stores the current company data in a database so that it can accessed from one
centralised location which acts a single source of truth for the sales data. Queries will take place via the database to get-up to
date metrics for the business.

## Prerequisites

You may use HomeBrew (macos) to install an distribution for central software called "miniconda" which is an environment can created to have all the dependencies and packages installed all in one.

Many list of packages are required to fulfil this project which can be found in the prerequisites.txt file.

But main packages include:

- IDE = Vscode
- PostgreSQL / PGAdmin
- Pandas
- PyYaml

`pip install PyYAML`

- Tabula

`pip install tabula-py`

## Usage Instructions

### Milestone 1

#### Step 1

Setting up the environment:

Create the GitHub repo, thus cloned to the local machine.

![alt text](Github_repo_multi.png)

### Milestone 2

#### Step 1

Setting up a new database to store the data.

New database sales_data created using PgAdmin 4:

![alt text](database_sales_data.png)

#### Step 2

##### Create Project Utils

1. Create Data Extraction class. `data_extraction.py`, this will store the methods responsible for retrieving data from different
    sources into pandas DataFrame.
2. Create Data Cleaning class. In `data_cleaning.py`, we develop the class DataCleaning that cleans different tables, which is
    retrieved from the 'data_extraction.py`.
3. Create and write a DatabaseConnector for the uploading of the data. In `database_utils.py`, which initiates the database
    engine based on credentials provided in the 'yaml` file.
4. Create `db_creds` yaml file to store the credentials for both external and `db_creds_local` for the local.
    **FYI**
5. Can also create an `.env` file to store all the sensitive data, making them secure and protected (Making sure it;s within
    the `gitignore` file.)
6. Create a Database Schema which performs the wrangling for the data `star_schema.sql`. Where all the columns are converted
    into its correct data types, as dim tables are given a primary key, and where also foreign keys are added into the order
    table.
7. Create a Data querying database using SQL which queries are performed within the `scenario_queries.sql` file.

#### Step 3

1. Loading all the credentials using the `load_dotenv` and adding within the DatabaseConnector that reads the `.env` file.

Picture

2. In the DatabaseConnector, create a method `init_db_engine` which will read the credentials from the `.env` file and
    initialise and return an SQLAlchemy database engine.

picture

3. Using the engine from `init_db_engine`, create a method `list_db_tables` to list all the tables in the database, so that
    we know which tables you can extract data from.

picture

4. Create a method within the DatabaseConnector class called `upload_to_db`. This method will take in a Pandas DataFrame and
    table name to upload as a argument.

    picture

#### Extracting and cleaning user data

##### Step 1

i. Import all the dependencies required for the extraction of the user data
    - Develop a method within the DataExtractor class called `read_rds_table` which will extract the database table to a Pandas
        DataFrame. An instance will be taken of your DatabaseConnector class and the table name as an argument and return a Pandas DataFrame. Firstly, use the list_db_tables methods to grab the name of the table containing the user data, thus
        using the `read_rds_table` method to extract the table containing user data and return the Pandas DataFrame.

    picture

##### Step 2

i. Create a method called `clean_user_data` and perform the desired operations using Pandas, returning the cleaned user data
    updated DataFrame.

    picture

#### Extracting and cleaning the card data

##### Step 1

i. Import all the dependencies required for the extraction of the card data (stored in a pdf file in AWS s3 Bucket)
    - Develop a method within the DataExtractor class called `retrieve_pdf_data`, which takes the link as an argument and returns a Pandas DataFrame. Firstly, using the `tabula-py` Python Package to extract all the pages from the PDF document
    at the following link provided. Then return a DataFrame of the extracted data.

    picture

##### Step 2

i. Create a method called `clean_card_data` and perform the desired operations using Pandas, returning the cleaned card data
    updated DataFrame.

    picture

#### Extracting and cleaning the store data

##### Step 1

i. Import all the dependencies required for the extraction of the stores data (API Get Request and has TWO API Get methods)
    - Develop a method within the DataExtractor class called `list_number_of_stores` taking in the given endpoint and the api key as arguments and returning the number of stores. The API has two GET methods. One will return the n.o of stores in the business and the other will retrieve the store given a store number. To connect to the API, an API Key must be included to the API in the method header. We have this within the `.env` file. Along with the endpoints.
    - Develop a method called `retrieve_stores_data` that takes the store_endpoint and api key as an argument and extracts all the stores from the API and saves them into a Pandas DataFrame.

    picture

##### Step 2

i. Create a method called `clean_store_data` and perform the desired operations using Pandas, returning the cleaned store data
    updated DataFrame.

    picture

#### Extracting and cleaning the products data

##### Step 1

i. Import all the dependencies required for the extraction of the products data (stored in s3 Bucket in CSV file `boto3` , `bytesIO`, `numpy`)
    - Develop a method within the DataExtractor class called `extract_from_s3` taking the address(s3) as an argument which uses the `boto3` package. The s3 Bucket is under the `address` given and will return the Pandas DataFrame. You will also need to be logged into the AWS CLI before you proceed to retrieve the data from the bucket.

    picture

##### Step 2

i. Create a method called `convert_products_weight` in `DataCleaning` class which will take the weight within the DataFrame as an argument and alter and clean the weight column once it is executed within the `clean_products_data` method. The weight column in the DataFrame, has different units. This method will convert them all to a decimal value representing their weight in `kg`. Using a 1:1 ratio of mil to g as a rough estimation for the rows containing ml.

    picture

##### Step 3

i. Create a method called `clean_products_data` and perform the desired operations using Pandas, returning the cleaned products data updated DataFrame.

    picture

#### Extracting and cleaning the orders data

##### Step 1

i. Import all the dependencies required for the extraction of the orders data (`datetime`)
    i. This table which acts as the single source of truth for all the orders the company has made in the past is stored in database on AWS RDS.
        ii. As developed earlier, using the `list_db_tables` method, get the name of the table containing all the information about the products orders.
            iii. Extract the ordered data using the `read_rds_table` method, returning the pandas DataFrame.

##### Step 2

i. Create a method called `clean_orders_data` in the `DataCleaning` class and perform the desired operations using Pandas, returning the cleaned orders data updated DataFrame.
    ii. The orders data contains column headers which are identical in other tables.
        iii. This table is the center of the `star_schema.sql` database.

    picture

#### Extracting and Cleaning the Order Date Data

##### Step 1

i. Import all the dependencies required for the extraction of the orders data (`datetime`)
    ii. This is the final source of the data which is incapsulated as a JSON file containing the details of when each sale occured, as well as other related attributes. Currently stored on s3 which can be found on the link provided.
        iii. The extraction of JSON is within our method within the `DataExtraction` class called `extract_from_s3`.

    picture

##### Step 2

i. Create a method called `clean_order_date` in the `DataCleaning` class and perform the desired operations using Pandas, returning the cleaned orders date data updated DataFrame.

    picture

#### Uploading all the cleaned data to local database

i. Using the `main.py` file, the connection to the local postgres database using our local credentials.
    ii. Required functions are called using the `__name__ == `__main__`` .
        iii. Uploaded each clean data into their perspective table name
            - Order Table > order_table
            - Legacy User > dim_user
            - Card Details > dim_card_details
            - Store Details > dim_store_details
            - Product Details > dim_product_details
            - Order Time Data > dim_date_times

    picture 

### Milestone 3

#### Database Star Schema

Creating the database schema, ensuring that the columns are of the correct data types.

- Tables were updated to ensure that data were stored in the correct data types. Determine the maximum number of characters for the VARCHAR(?) data type. A query was performed, before the output were to be used in the VARCHAR data type.

- **Task 1: Changing into the Correct data types to orders table**
```
    -- Maximum card_number length
    SELECT MAX(LENGTH(card_number::TEXT)) FROM orders_table
    SET LIMIT 1; --19

    -- Maximum store_code length
    SELECT MAX(LENGTH(store_code::TEXT)) FROM orders_table
    SET LIMIT 1; --12

    -- Maximum product_code length
    SELECT MAX(LENGTH(product_code::TEXT)) FROM orders_table
    SET LIMIT 1; --11
```
##### Data Type Changes

    --Alter column of data types in the orders_table and casting columns

    ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID
        USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE UUID
        USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

- **Task 2: Dim User table converted into correct data types.**
```
    -- Maximum country_code length from dim_user
    SELECT MAX(LENGTH(country_code::TEXT)) FROM dim_user
    SET LIMIT 1;
```
##### Data Type Changes

    --Alter the data types column and casting columns

    ALTER TABLE dim_user
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE
    USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN user_uuid TYPE UUID
        USING user_uuid::uuid,
    ALTER COLUMN join_date TYPE DATE;

- **TASK 3: Change the dim store_details table columns into correct data types and merge LAT columns (This was already dropped / renamed in the DataCleaning phase)**
```
    -- Maximum country_code length from dim_store_details
    SELECT MAX(LENGTH(country_code::TEXT)) FROM dim_store_details
    SET LIMIT 1;

    -- Update the N/A values into NULL respectively(As shown in thee `star_schema.sql` file)
```
###### Data Type Changes

    -- Altering the column of data types
    ALTER TABLE dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT
        USING staff_numbers::smallint,
    ALTER COLUMN opening_date TYPE DATE
        USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255),
    ALTER COLUMN longitude TYPE FLOAT
        USING longitude::double precision,
    ALTER COLUMN latitude TYPE FLOAT
        USING latitude::double precision;

- **Task 4: Making changes to dim_products table**

- Previous alterations is shown in the `star_schema.sql`.

##### Data Type Changes

- The statement below is to be used to improve readability in dim_product table
```
    -- Categorising weight class based on weight
    UPDATE dim_products
    SET weight_class = CASE
    WHEN weight < 2 then 'light'
    WHEN weight >= 2 AND weight <40 then 'Mid_Sized'
    WHEN weight >= 40 AND weight <140 then 'Heavy'
    WHEN weight >= 140 then 'Truck_Required'
    ELSE NULL
    END;
```
- **Task 5: Correcting data types of changing Dim product table columns**

- Previous findings has been made prior as shown in `star_schema.sql`.

##### Data Type Changes

    -- Altering data types of dim_products
    ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT
        USING product_price::double precision,
    ALTER COLUMN weight TYPE FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE,
    ALTER COLUMN uuid TYPE UUID
        USING uuid::uuid,
    ALTER COLUMN still_available TYPE BOOLEAN
    USING CASE
        WHEN still_available = 'still_available' THEN TRUE
        WHEN still_available = 'removed' THEN FALSE
    END;

- **Task 6: Correcting data types of changing Dim Date Time table columns.**

- Previous findings has been solved as shown in `star_schema.sql`.

##### Data Type Changes

    -- Alterting column of data types
    ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE  VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID
        USING date_uuid::uuid;

- **Task 7: Correcting data types of changing Dim Card Details table columns.**

- Previous findings has been solved as shown in `star_schema.sql`.
```
    - Alterting dim_card_details data types
    ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE
    USING date_payment_confirmed::DATE;
```
#### Primary Keys and Foreign Keys

Primary keys are implemented as below which will serve the orders_table. We use SQL respectively to update the columns as our primary keys. Making sure our dim tables primary key matches that of the same column in the orders_table.

- **Task 8: Create Primary Key in details which are added in the dim tables.**
```
    ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);

    ALTER TABLE dim_user
    ADD PRIMARY KEY (user_uuid);

    ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);

    ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);

    ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);

    SELECT user_uuid FROM dim_user;
    SELECT _ FROM orders_table;
    SELECT _ FROM dim_store_details;
```
- **TASK 9: Creating Foriegn Key and finalising database schema**

- Used to find the difference whilst data cleaning to ensure foriegn and primary key matched. 
**Main** differences is solved in the file `star_schema.sql`.
```
    SELECT distinct(orders_table.user_uuid)
    FROM orders_table
    LEFT JOIN dim_user
    ON orders_table.user_uuid = dim_user.user_uuid
    WHERE dim_user.user_uuid IS NULL
```
- **Creating the Foreign keys**

- Data Cleaning to ensure the foriegn keys matched the primary keys is shown in thee `star_schema.sql` file and 
joining of the both tables into one column had to be done to get one single source of truth for the data.
```
    ALTER TABLE orders_table
    ADD FOREIGN KEY (date_uuid)
    REFERENCES dim_date_times(date_uuid);

    ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid
    FOREIGN KEY (user_uuid)
    REFERENCES dim_user(user_uuid);

    ALTER TABLE orders_table
    ADD FOREIGN KEY (store_code)
    REFERENCES dim_store_details(store_code);

    ALTER TABLE orders_table
    ADD CONSTRAINT fk_product_code
    FOREIGN KEY (product_code)
    REFERENCES dim_products(product_code);

    ALTER TABLE orders_table
    ADD FOREIGN KEY (card_number)
    REFERENCES dim_card_details(card_number);
```
### Milestone 4

#### SQL Data Queries

Querying and extracting the data from the local database to initialise some up-to-date metrics, thus having a data 
driven decision and a better / clear understanding of the sales data.

- **Task 1 : How many stores does the business have and in which countries ?**
```
    SELECT country_code, COUNT(store_code) as total_no_stores
    FROM dim_store_details
    GROUP BY country_code
    ORDER BY total_no_stores DESC;
```
- **Task 2 : Which locations currently have the most stores ?**
```
    SELECT locality, COUNT(store_code) as total_no_stores
    FROM dim_store_details
    GROUP BY locality
    ORDER BY total_no_stores DESC
    LIMIT 7;
```
- **Task 3: Which months produced the largest amount of sales ?**
```
    SELECT
    ROUND(SUM(dim_products.product_price \* orders_table.product_quantity)::NUMERIC, 2) as total_sales, dim_date_times.month
    FROM dim_date_times
    JOIN orders_table
    ON dim_date_times.date_uuid = orders_table.date_uuid
    JOIN dim_products
    ON orders_table.product_code = dim_products.product_code
    GROUP BY dim_date_times.month
    ORDER BY total_sales DESC
    LIMIT 6;
```
- **Task 4: How many sales are coming from online ?**
```
    SELECT
    COUNT(dim_products.product_code) as number_of_sales
    SUM(orders_table.product_quantity)as product_quantity_count,
    CASE
    WHEN dim_store_details.store_type IN ('Super Store', 'Local', 'Outlet', 'Mall Kiosk') THEN 'offline'
    ELSE 'Web'
    END as location
    FROM dim_products
    JOIN orders_table
    ON orders_table.product_code = dim_products.product_code
    JOIN dim_store_details
    ON dim_store_details.store_code = orders_table.store_code
    GROUP BY location
    ORDER BY number_of_sales ASC;
```
- **Task 5: What percentage of sales come through each type of store ?**
```
    SELECT
    dim_store_details.store_type AS store_type,
    ROUND(CAST(SUM(dim_products.product_price _ orders_table.product_quantity) AS NUMERIC), 2) Total_sales,
    ROUND((CAST(SUM(dim_products.product_price _ orders_table.product_quantity) AS NUMERIC) /
    CAST((SELECT SUM(dim_products.product_price _ orders_table.product_quantity)
    FROM orders_table
    INNER JOIN dim_products
    ON orders_table.product_code = dim_products.product_code) AS NUMERIC) _ 100), 2) AS "percentage_total(%)"
    FROM dim_products
    INNER JOIN
    orders_table ON orders_table.product_code = dim_products.product_code
    INNER JOIN
    dim_store_details ON dim_store_details.store_code = orders_table.store_code
    GROUP BY
    dim_store_details.store_type
    ORDER BY
    "percentage_total(%)" DESC;
```
- **Task 6: Which month in each year produced the highest cost of sales ?**
```
    SELECT
    ROUND(SUM(dim_products.product_price \* orders_table.product_quantity)::NUMERIC, 2) as Total_sales,
    dim_date_times.year, dim_date_times.month
    FROM dim_products
    INNER JOIN
    orders_table ON orders_table.product_code = dim_products.product_code
    INNER JOIN
    dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
    GROUP BY dim_date_times.year, dim_date_times.month
    ORDER BY total_sales DESC
    LIMIT 10;
```
- **Task 7: What is our staff headcount ?**
```
    SELECT
    SUM(staff_numbers) as total_staff_numbers, country_code
    FROM dim_store_details
    GROUP BY country_code
    ORDER BY total_staff_numbers DESC;
```
- **Task 8: Which German store type is selling the most ?**
```
    SELECT
    ROUND(SUM(dim_products.product_price \* orders_table.product_quantity)::NUMERIC, 2) AS Total_sales,
    dim_store_details.store_type, dim_store_details.country_code
    FROM dim_products
    INNER JOIN
    orders_table ON orders_table.product_code = dim_products.product_code
    INNER JOIN
    dim_store_details ON dim_store_details.store_code = orders_table.store_code
    WHERE
    Country_code = 'DE'
    GROUP BY dim_store_details.store_type, dim_store_details.country_code
    ORDER BY Total_sales;
```
- **Task 9 : How quickly is the company making sales ?**

This is shown in the `querying_data.sql` file.

### File Structure
```
    .
    ├── README.md
    ├── __pycache__
    │   ├── data_cleaning.cpython-312.pyc
    │   ├── data_extraction.cpython-312.pyc
    │   └── database_utils.cpython-312.pyc
    ├── data_cleaning.py
    ├── data_extraction.py
    ├── database_utils.py
    ├── db_creds.yaml
    ├── db_creds_local.yaml
    ├── headers.json
    ├── main.py
    ├── querying_data.sql
    ├── scratch.ipynb
    ├── star_schema.sql
    └── uncleaned_data
        └── legacy_store_details.ipynb
```
### License Information

MIT license in place, only permitted to use, copy, modify, merge publish, distribute, sublicence.