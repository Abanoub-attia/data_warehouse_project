USE DataWarehouse;
GO

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

--Test the quality of the loaded data by selecting the top 10 rows from the table
SELECT TOP 10 * FROM bronze.crm_cust_info;
--Test the quality of the loaded data by counting the number of rows in the table
SELECT COUNT(*) AS total_rows FROM bronze.crm_cust_info;

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

--Test the quality of the loaded data by selecting the top 10 rows from the table
SELECT TOP 10 * FROM bronze.crm_prd_info;
--Test the quality of the loaded data by counting the number of rows in the table
SELECT COUNT(*) AS total_rows FROM bronze.crm_prd_info;

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

--Test the quality of the loaded data by selecting the top 10 rows from the table
SELECT TOP 10 * FROM bronze.crm_sales_details;
--Test the quality of the loaded data by counting the number of rows in the table
SELECT COUNT(*) AS total_rows FROM bronze.crm_sales_details;

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

--Test the quality of the loaded data by selecting the top 10 rows from the table
SELECT TOP 10 * FROM bronze.erp_cust_az12;
--Test the quality of the loaded data by counting the number of rows in the table
SELECT COUNT(*) AS total_rows FROM bronze.erp_cust_az12;

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

--Test the quality of the loaded data by selecting the top 10 rows from the table
SELECT TOP 10 * FROM bronze.erp_loc_a101;
--Test the quality of the loaded data by counting the number of rows in the table
SELECT COUNT(*) AS total_rows FROM bronze.erp_loc_a101;

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------


--Test the quality of the loaded data by selecting the top 10 rows from the table
SELECT TOP 10 * FROM bronze.erp_px_cat_g1v2;
--Test the quality of the loaded data by counting the number of rows in the table
SELECT COUNT(*) AS total_rows FROM bronze.erp_px_cat_g1v2;