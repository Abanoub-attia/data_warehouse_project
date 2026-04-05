------------------------------------------------------------------
-- check the quality of the customer dimension in the golden layer
------------------------------------------------------------------
--1. check the uniqueness of the gender table
SELECT DISTINCT 
    gender
FROM gold.dim_customer;
--2. check if there duplicates in the customer_id column
SELECT
    customer_id,
    COUNT(*) AS count
FROM gold.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;
--3. check if there duplicates in the customer_key column
SELECT
    customer_key,
    COUNT(*) AS count
FROM gold.dim_customer
GROUP BY customer_key
HAVING COUNT(*) > 1;
--4. have a look at the data in the customer dimension table
SELECT * FROM gold.dim_customer;
------------------------------------------------------------------
-- check the quality of the customer dimension in the golden layer
------------------------------------------------------------------
--1. check if there duplicates in the category_id column
SELECT
    product_number,
    COUNT(*) AS count
FROM gold.dim_product
GROUP BY product_number
HAVING COUNT(*) > 1;
--2. check if there duplicates in the product_key column
SELECT
    product_key,
    COUNT(*) AS count
FROM gold.dim_product
GROUP BY product_key
HAVING COUNT(*) > 1;
--2. have a look at the data in the product dimension table
SELECT * FROM gold.dim_product;
------------------------------------------------------------------
-- check the quality of the sales fact in the golden layer
------------------------------------------------------------------
--1. forign key integrity
SELECT *
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customer AS dc ON fs.customer_key = dc.customer_key
LEFT JOIN gold.dim_product AS dp ON fs.product_key = dp.product_key
WHERE dc.customer_key IS NULL OR dp.product_key IS NULL;
--2. have a look at the data in the sales fact table
SELECT * FROM gold.fact_sales;