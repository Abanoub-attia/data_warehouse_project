--Create Database 'DataWarehouse'

USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

--Create for each layer of the Data Warehouse a schema
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

SELECT TOP 10 * FROM bronze.crm_cust_info;
GO
SELECT TOP 10 * FROM bronze.crm_prd_info;
GO
SELECT TOP 10 * FROM bronze.crm_sales_details;

SELECT TOP 10 * FROM bronze.erp_px_cat_g1v2; 


SELECT * FROM silver.crm_cust_info;


SELECT
    *
FROM
   (SELECT
        cst_firstname,
        CASE 
            WHEN cst_firstname LIKE '% %' THEN 1
            ELSE 0
        END AS first_name_quality
    FROM bronze.crm_cust_info) AS subquery
WHERE first_name_quality = 1;

SELECT cntry, LEN(cntry) as actual_length
FROM bronze.erp_loc_a101
WHERE cntry LIKE 'USA%';