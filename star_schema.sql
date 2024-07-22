
-- Task 1 : Changing into the Correct data types to orders table 

SELECT MAX(LENGTH(card_number::TEXT)) FROM orders_table
	SET LIMIT 1; --19

-- Maximum store_code length

SELECT MAX(LENGTH(store_code::TEXT)) FROM orders_table
	SET LIMIT 1; --12

--  Maximum product_code length

SELECT MAX(LENGTH(product_code::TEXT)) FROM orders_table
	SET LIMIT 1; --11

--Alter column of data types in the orders_table 

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID
	USING date_uuid::uuid, 
ALTER COLUMN user_uuid TYPE UUID
	USING user_uuid::uuid, 
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12), 
ALTER COLUMN product_code TYPE VARCHAR(11), 
ALTER COLUMN product_quantity TYPE SMALLINT;


--Task 2: Dim User table converted into correct data types

-- Maximum country_code length from dim_user
SELECT MAX(LENGTH(country_code::TEXT)) FROM dim_user
	SET LIMIT 1;

-- Alter the data types column
ALTER TABLE dim_user
ALTER COLUMN first_name TYPE VARCHAR(255), 
ALTER COLUMN last_name TYPE VARCHAR(255), 
ALTER COLUMN date_of_birth TYPE DATE
USING date_of_birth::DATE, 
ALTER COLUMN country_code TYPE VARCHAR(3), 
ALTER COLUMN user_uuid TYPE UUID
	USING user_uuid::uuid, 
ALTER COLUMN join_date TYPE DATE;

-- TASK 3: Change the dim_store_details table columns into correct data types and merge LAT columns

/* UPDATE dim_store_details
SET latitude = COALESCE(latitude || lat, latitude);
-- Table already dropped when cleaning
ALTER TABLE dim_store_details
DROP COLUMN lat; 
*/

-- Maximum country_code length from dim_store_details
SELECT MAX(LENGTH(country_code::TEXT)) FROM dim_store_details
SET LIMIT 1; 

-- Updating the N/A values into NULL respectively
SELECT * FROM dim_store_details
	WHERE address = 'N/A';

UPDATE dim_store_details
	SET latitude = NULL
	WHERE latitude::text = 'N/A';

UPDATE dim_store_details
	SET longitude = NULL
	WHERE longitude::text = 'N/A';

UPDATE dim_store_details
	SET address = NULL
	WHERE address = 'N/A';

UPDATE dim_store_details
	SET locality = NULL 
	WHERE locality = 'N/A';

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

-- Task 4: Making changes to dim_products table 

--Replacing the £ symbol
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- Adding the weight_class column
ALTER TABLE dim_products
ADD weight_class VARCHAR(14);

-- Categorising weight class based on weight
UPDATE dim_products
SET weight_class = CASE
WHEN weight < 2 then 'light'
WHEN weight >= 2 AND weight <40 then 'Mid_Sized'
WHEN weight >= 40 AND weight <140 then 'Heavy'
WHEN weight >= 140 then 'Truck_Required'
ELSE NULL
END;

--Task 5: Correcting data types of changing Dim product table columns 

-- Renaming the removed column into still_available
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;

-- Conditional statement to prove EAN in table
ALTER TABLE dim_products
RENAME COLUMN "EAN" TO ean;
ALTER TABLE dim_products
RENAME COLUMN ean TO "EAN";

-- Maximum Length of product_code
SELECT MAX(LENGTH(product_code)) FROM dim_products
	SET LIMIT 1; --11

-- Maximum length of EAN
SELECT MAX(LENGTH("EAN")) FROM dim_products
	SET LIMIT 1; --17

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

--Task 6: Correcting data types of changing Dim Date Time table columns 

-- Maximum length of longest month
SELECT MAX(LENGTH(month::TEXT)) FROM dim_date_times
	SET LIMIT 1; --2

-- Maximum length of longest year
SELECT MAX(LENGTH(year::TEXT)) FROM dim_date_times
	SET LIMIT 1; --4

-- Maximum length of longest day 
SELECT MAX(LENGTH(day::TEXT)) FROM dim_date_times
	SET LIMIT 1; --2

-- Maximum length of longest time_period
SELECT MAX(LENGTH(time_period::TEXT)) FROM dim_date_times
	SET LIMIT 1; --10

-- Alterting column of data types
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE  VARCHAR(2), 
ALTER COLUMN year TYPE VARCHAR(4), 
ALTER COLUMN day TYPE VARCHAR(2), 
ALTER COLUMN time_period TYPE VARCHAR(10), 
ALTER COLUMN date_uuid TYPE UUID
	USING date_uuid::uuid; 

--Task 7: Correcting data types of changing Dim Card Details table columns

-- Maximum lenght of longest card_number
SELECT MAX(LENGTH(card_number)) FROM dim_card_details
SET LIMIT 1; --19

-- Maximum length of longest expiry_date
SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details
SET LIMIT 1; --5

-- Altering dim_card_details data types
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19), 
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE
	USING date_payment_confirmed::DATE;

-- Task 8: Create Primary Key in details
	
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
SELECT * FROM orders_table;
SELECT * FROM dim_store_details;

--TASK 9: Creating Foriegn Key and finalising database schema

-- Fixing the differences between the tables
	-- Some cards in orders_table but not in dim_card_details
	SELECT DISTINCT card_number
	FROM orders_table
		WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

	INSERT INTO dim_card_details (card_number, date_payment_confirmed, expiry_date, card_provider)
	SELECT card_number, NULL, NULL, NULL
	FROM (
		SELECT DISTINCT card_number
		FROM orders_table
		WHERE card_number NOT IN  (SELECT card_number FROM dim_card_details)
	) AS missing_cards

	--- WEB in orders_details not in dim_store_details
	SELECT DISTINCT store_code
	FROM orders_table
		WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

	INSERT INTO dim_store_details (store_code, latitude, longitude, staff_numbers, opening_date, country_code, continent, address, store_type, locality, index)
	SELECT store_code, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
	FROM (
		SELECT DISTINCT store_code
		FROM orders_table
		WHERE store_code NOT IN (SELECT store_code FROM dim_store_details)
		) AS WEB;

	--- Product_Code in orders_details and not in dim_products
	SELECT DISTINCT product_code
	FROM orders_table
		WHERE product_code NOT IN (SELECT product_code FROM dim_products);
	INSERT INTO dim_products (product_code)
	SELECT UPPER(product_code)
	FROM dim_products
		WHERE product_code ~ '[a-z]';

	INSERT INTO dim_store_details (store_code, latitude, longitude, staff_numbers, opening_date, country_code, continent, address, store_type, locality, index)
		SELECT store_code, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
	FROM (
		SELECT DISTINCT store_code
		FROM orders_table
		WHERE store_code NOT IN (SELECT store_code FROM dim_store_details)
		) AS WEB;

	-- Used to find the difference
	SELECT distinct(orders_table.user_uuid)
	FROM orders_table
	LEFT JOIN dim_user
	ON orders_table.user_uuid = dim_user.user_uuid
	WHERE dim_user.user_uuid IS NULL

--Creating Foriegn Keys
ALTER TABLE orders_table
	ADD FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times(date_uuid);

	INSERT INTO dim_user (user_uuid)
	SELECT distinct(orders_table.user_uuid)
	FROM orders_table
	LEFT JOIN dim_user
	ON orders_table.user_uuid = dim_user.user_uuid
	WHERE dim_user.user_uuid IS NULL;

	ALTER TABLE orders_table
		ADD CONSTRAINT fk_user_uuid
		FOREIGN KEY (user_uuid)
		REFERENCES dim_user(user_uuid);

ALTER TABLE orders_table
	ADD FOREIGN KEY (store_code)
	REFERENCES dim_store_details(store_code);

	INSERT INTO dim_products (product_code)
	SELECT distinct(orders_table.product_code)
	FROM orders_table
	LEFT JOIN dim_products
	ON orders_table.product_code = dim_products.product_code
	WHERE dim_products.product_code IS NULL;

	ALTER TABLE orders_table
		ADD CONSTRAINT fk_product_code
		FOREIGN KEY (product_code)
		REFERENCES dim_products(product_code);

ALTER TABLE orders_table
	ADD FOREIGN KEY (card_number)
	REFERENCES dim_card_details(card_number);