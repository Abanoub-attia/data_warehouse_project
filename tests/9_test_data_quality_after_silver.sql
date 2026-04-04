USE DataWarehouse;
GO
----------------------------------------------------------------------------------------------------------------------------
--define data quality issues for the crm_cust_info table after the data cleaning and transformation in the silver layer
----------------------------------------------------------------------------------------------------------------------------
--1. Missing values(NULL) in the cust_id column
--2. Duplicate values in the cust_id column
SELECT
    cst_id,
    COUNT(*) AS count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
--3. Check for unwanted spaces in the cst_firstname
SELECT
    cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
--4. Check for unwanted spaces in the cst_lastname
SELECT
    cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
--5. Check for unwanted spaces in the cst_marital_status 
SELECT
    cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);
--6. Check for unwanted spaces in the cst_gndr
SELECT
    cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);
--7. Check for data inconsistencies in the cst_gndr column (e.g. values other than 'M' and 'F')
SELECT
    DISTINCT cst_gndr
FROM silver.crm_cust_info;
--8. Check for data inconsistencies in the cst_marital_status column (e.g. values other than 'M' and 'S')
SELECT
    DISTINCT cst_marital_status
FROM silver.crm_cust_info;
--9. Final look at the data after the data cleaning and transformation in the silver layer
SELECT * FROM silver.crm_cust_info;
----------------------------------------------------------------------------------------------------------------------------
--define data quality issues for the crm_prd_info table after the data cleaning and transformation in the silver layer
----------------------------------------------------------------------------------------------------------------------------
--1. Missing values(NULL) in the prd_id column
--2. Duplicate values in the prd_id column
SELECT
    prd_id,
    COUNT(*) AS count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
--3. Check for unwanted spaces or NULL values in the prd_nm column
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) OR prd_nm IS NULL;
--4. Check NULL values or negative values in the prd_cost column
SELECT
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL;
--5. Check for data inconsistencies in the prd_line column (e.g. values other than 'M' , 'R' , 'S' and 'T')
SELECT
    DISTINCT prd_line
FROM silver.crm_prd_info;
--6. Check for invalid date oders
SELECT 
    prd_start_dt,
    prd_end_dt,
    prd_key
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;
--7. Final look at the data after the data cleaning and transformation in the silver layer
SELECT * FROM silver.crm_prd_info;
----------------------------------------------------------------------------------------------------------------------------
--define data quality issues for the crm_sales_details table after the data cleaning and transformation in the silver layer
----------------------------------------------------------------------------------------------------------------------------
--1. Check for unwanted spaces or NULL values in the sls_ord_num column
SELECT
    sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num) OR sls_ord_num IS NULL;
--2. NULL values in the sls_prd_key column
SELECT
    sls_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key IS NULL;
--3. NULL values in the sls_cust_id column
SELECT
    sls_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id IS NULL;
--4. Check for invalid dates in the sls_order_dt column (e.g. negative values or values with incorrect format)
SELECT 
    sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL;

SELECT 
    sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > '2025-01-01';

SELECT 
    sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt < '1900-01-01';
--5. Check for invalid dates in the sls_ship_dt column (e.g. negative values or values with incorrect format)
SELECT 
    sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt IS NULL;

SELECT 
    sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt > '2025-01-01';

SELECT 
    sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt < '1900-01-01';
--6. Check for invalid dates in the sls_due_dt column (e.g. negative values or values with incorrect format)
SELECT 
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt IS NULL;


SELECT 
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt > '2025-01-01';

SELECT 
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt < '1900-01-01';

--7. Check for invalid date orders
SELECT 
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_ord_num
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt;
--7. Check for invalid sales
SELECT 
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price;

SELECT 
    sls_sales
FROM silver.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0;

SELECT 
    sls_quantity
FROM silver.crm_sales_details
WHERE sls_quantity IS NULL OR sls_quantity <= 0;

SELECT 
    sls_price
FROM silver.crm_sales_details
WHERE sls_price IS NULL OR sls_price <= 0;
--final look at the data after the data cleaning and transformation in the silver layer
SELECT * FROM silver.crm_sales_details;
----------------------------------------------------------------------------------------------------------------------------
--define data quality issues for the erp_cust_az12 table after the data cleaning and transformation in the silver layer
----------------------------------------------------------------------------------------------------------------------------
--1. Check for invalid dates in the bdate column
SELECT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();
--2. Check for data inconsistencies in the gen column
SELECT
    DISTINCT gen
FROM silver.erp_cust_az12;
--3. Final look at the data after the data cleaning and transformation in the silver layer
SELECT * FROM silver.erp_cust_az12;
----------------------------------------------------------------------------------------------------------------------------
--define data quality issues for the erp_loc_a101 table after the data cleaning and transformation in the silver layer
----------------------------------------------------------------------------------------------------------------------------
--1. Check for data inconsistencies in the cntry column
SELECT
    DISTINCT cntry
FROM silver.erp_loc_a101;
--2. Final look at the data after the data cleaning and transformation in the silver layer
SELECT * FROM silver.erp_loc_a101;
----------------------------------------------------------------------------------------------------------------------------
--define data quality issues for the erp_px_cat_g1v2 table after the data cleaning and transformation in the silver layer
----------------------------------------------------------------------------------------------------------------------------
--1. Check for unwanted spaces or NULL values in the cat column
SELECT
    cat
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR cat IS NULL;
--2. Check for unwanted spaces or NULL values in the subcat column
SELECT
    subcat
FROM silver.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat) OR subcat IS NULL;
--3. Check for unwanted spaces or NULL values in the maintenance column
SELECT
    maintenance
FROM silver.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance) OR maintenance IS NULL;
--4. Check for data inconsistencies in the cat column
SELECT
    DISTINCT cat
FROM silver.erp_px_cat_g1v2;
--5. Check for data inconsistencies in the subcat column
SELECT
    DISTINCT subcat
FROM silver.erp_px_cat_g1v2;
--6. Check for data inconsistencies in the maintenance column
SELECT
    DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;
--7. Final look at the data after the data cleaning and transformation in the silver layer
SELECT * FROM silver.erp_px_cat_g1v2;