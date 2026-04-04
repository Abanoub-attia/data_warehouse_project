--Use the DataWarehouse database
USE DataWarehouse;
GO


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, 
            @end_time DATETIME, 
            @start_time_patch DATETIME, 
            @end_time_patch DATETIME;

    BEGIN TRY
        SET @start_time_patch = GETDATE(); -- Start time for the entire loading process  
        -- Print a message to indicate the start of the loading process
        PRINT '===========================================================================';
        PRINT 'Starting to insert data into the silver schema...';
        PRINT '===========================================================================';
        PRINT ' ';
        ---------------------------------------------------------------------------------------------------------------------
        -- Print a message to indicate the start of the loading crm tables
        PRINT '---------------------------------------------------------------------------';
        PRINT 'Loading CRM tables...';
        PRINT '---------------------------------------------------------------------------';
        PRINT ' ';
        ---------------------------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE(); -- Start time for loading the crm_cust_info table
        PRINT  '>> Truncating table: silver.crm_cust_info';
        --truncate the silver tables before inserting the cleaned and transformed data from the bronze layer to the silver layer
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT  '>> Inserting data into: silver.crm_cust_info';
        --insert data into the silver layer after the data cleaning and transformation for crm_cust_info table
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key, 
            cst_firstname, 
            cst_lastname, 
            cst_gndr, 
            cst_marital_status, 
            cst_create_date)
        --data cleaning and transformation for crm_cust_info table    
        SELECT
            cst_id,
            cst_key,
            --removing unwanted spaces
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            --data transformation to make the data more consistent and easier to analyze
            --handling missing values
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            cst_create_date
        FROM 
            (SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            --cst_id = NULL does not have a other valid values so we will not consider it as a duplicate and we will not consider it in the data quality check for duplicates
            WHERE cst_id IS NOT NULL) AS subquery 
        WHERE flag_last = 1; 

        SET @end_time = GETDATE(); -- End time for loading the crm_cust_info table
        PRINT '>> Insert Time for silver.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';
        ---------------------------------------------------------------------------------------------------------------------
        PRINT  '>> Truncating table: silver.crm_prd_info';
        --truncate the silver tables before inserting the cleaned and transformed data from the bronze layer to the silver layer
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT  '>> Inserting data into: silver.crm_prd_info';
        --insert data into the silver layer after the data cleaning and transformation for crm_prd_info table
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt)
        --data cleaning and transformation for crm_prd_info table    
        SELECT
            prd_id,
            --Extracting new columns
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            --Handling Null values
            CASE
                /*WHEN prd_cost IS NULL THEN 
                    (SELECT 
                        AVG(prd_cost) AS avg_prd_cost
                    FROM 
                        (SELECT 
                            ISNULL(prd_cost, 0) AS prd_cost
                        FROM bronze.crm_prd_info) as subquery;)*/
                WHEN prd_cost IS NULL THEN 0
                ELSE prd_cost
            END AS prd_cost,
            --Data transformation to make the data more consistent and easier to analyze
            --handling missing values
            CASE UPPER(TRIM(prd_line)) 
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            prd_start_dt,
            --Data enrichment by adding a new values 
            DATEADD(day, -1 ,(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))) AS prd_end_dt 
        FROM
            bronze.crm_prd_info;

        SET @end_time = GETDATE(); -- End time for loading the crm_prd_info table
        PRINT '>> Insert Time for silver.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';
        ---------------------------------------------------------------------------------------------------------------------
        PRINT  '>> Truncating table: silver.crm_sales_details';
        --truncate the silver tables before inserting the cleaned and transformed data from the bronze layer to the silver layer
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT  '>> Inserting data into: silver.crm_sales_details';
        --insert data into the silver layer after the data cleaning and transformation for crm_sales_details table
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price)
        --data cleaning and transformation for crm_sales_details table  
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            --Handling Null values and invalid values for dates
            --Type casting
            CASE
                WHEN sls_order_dt IS NULL OR sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) AS sls_ship_dt,
            CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) AS sls_due_dt,
            --Handling Null values and invalid values for sales and price   
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) 
                    THEN sls_quantity * ABS(NULLIF(sls_price, 0))
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                    THEN  sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE(); -- End time for loading the crm_sales_details table
        PRINT '>> Insert Time for silver.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';
        ----------------------------------------------------------------------------------------------------------------------------
        PRINT '---------------------------------------------------------------------------';
        PRINT 'Loading ERP tables...';
        PRINT '---------------------------------------------------------------------------';
        PRINT ' ';
        ----------------------------------------------------------------------------------------------------------------------------
        PRINT  '>> Truncating table: silver.erp_cust_az12';
        --truncate the silver tables before inserting the cleaned and transformed data from the bronze layer to the silver layer
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT  '>> Inserting data into: silver.erp_cust_az12';
        --insert data into the silver layer after the data cleaning and transformation for erp_cust_az12 table
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen)
        --data cleaning and transformation for erp_cust_az12 table
        SELECT
            --data transformation 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            --Handling invalid dates
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            --data nromalization
            CASE  
                -- We replace Carriage Returns and Line Feeds with nothing, then TRIM spaces
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE(); -- End time for loading the erp_cust_az12 table
        PRINT '>> Insert Time for silver.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';
        ----------------------------------------------------------------------------------------------------------------------------
        PRINT  '>> Truncating table: silver.erp_loc_a101';
        --truncate the silver tables before inserting the cleaned and transformed data from the bronze layer to the silver layer
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT  '>> Inserting data into: silver.erp_loc_a101';
        --insert data into the silver layer after the data cleaning and transformation for erp_loc_a101 table
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry)
        --data cleaning and transformation for erp_loc_a101 table
        SELECT
            --data transformation
            REPLACE(cid, '-', '') AS cid,
            --data normalization and handling missing values
            CASE 
                WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE(); -- End time for loading the erp_loc_a101 table
        PRINT '>> Insert Time for silver.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';
        ----------------------------------------------------------------------------------------------------------------------------
        PRINT  '>> Truncating table: silver.erp_px_cat_g1v2';
        --truncate the silver tables before inserting the cleaned and transformed data from the bronze layer to the silver layer
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT  '>> Inserting data into: silver.erp_px_cat_g1v2';
        --insert data into the silver layer after the data cleaning and transformation for erp_px_cat_g1v2
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance)
        --data cleaning and transformation for erp_px_cat_g1v2 table
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE(); -- End time for loading the erp_px_cat_g1v2 table
        PRINT '>> Insert Time for silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';
        ----------------------------------------------------------------------------------------------------------------------------
        SET @end_time_patch = GETDATE(); -- End time for the entire loading process
        PRINT '---------------------------------------------------------------------------';  
        PRINT 'Finished Inserting data into the silver schema!';
        PRINT 'Total Load Time: ' + CAST(DATEDIFF(SECOND, @start_time_patch, @end_time_patch) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------------------------------------------------------';
        PRINT ' ';
    END TRY
    ----------------------------------------------------------------------------------------------------------------------------
    BEGIN CATCH
        -- If an error occurs, print the error message
    PRINT '===========================================================================';
    PRINT 'An error occurred while inserting data into the silver schema:';
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
    PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(50));        
    PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(50));
    PRINT '===========================================================================';
    END CATCH;
END;