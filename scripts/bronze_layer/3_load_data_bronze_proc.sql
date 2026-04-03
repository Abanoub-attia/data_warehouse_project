--Use the DataWarehouse database
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, 
            @end_time DATETIME, 
            @start_time_patch DATETIME, 
            @end_time_patch DATETIME;

    BEGIN TRY
        SET @start_time_patch = GETDATE(); -- Start time for the entire loading process
        -- Print a message to indicate the start of the loading process
        PRINT '===========================================================================';
        PRINT 'Starting to load data into the bronze schema...';
        PRINT '===========================================================================';
        PRINT ' ';

        -- Print a message to indicate the start of the loading crm tables
        PRINT '---------------------------------------------------------------------------';
        PRINT 'Loading CRM tables...';
        PRINT '---------------------------------------------------------------------------';
        PRINT ' ';

        ------------------------------------------------------------------------------------------
        ------------------------------------------------------------------------------------------

        SET @start_time = GETDATE(); -- Start time for loading the crm_cust_info table
        PRINT  '>> Truncating table: bronze.crm_cust_info';
        --Truncate the table before loading the data to avoid duplicates if the script is run multiple times
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT  '>> Inserting table: bronze.crm_cust_info';
        --Bulk load the data to the bronze schema for the from the crm source cust_info.csv file
        BULK INSERT bronze.crm_cust_info
        FROM '/home/abanoub/data_warehouse_project/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); -- End time for loading the crm_cust_info table
        PRINT '>> Load Time for bronze.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';

        ------------------------------------------------------------------------------------------
        ------------------------------------------------------------------------------------------

        SET @start_time = GETDATE(); -- Start time for loading the crm_prd_info table
        PRINT  '>> Truncating table: bronze.crm_prd_info';
        --Truncate the table before loading the data to avoid duplicates if the script is run multiple times
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT  '>> Inserting table: bronze.crm_prd_info';
        --Bulk load the data to the bronze schema for the from the crm source prd_info.csv file
        BULK INSERT bronze.crm_prd_info
        FROM '/home/abanoub/data_warehouse_project/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); -- End time for loading the crm_prd_info table
        PRINT '>> Load Time for bronze.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';

        ------------------------------------------------------------------------------------------
        ------------------------------------------------------------------------------------------

        SET @start_time = GETDATE(); -- Start time for loading the crm_sales_details table
        PRINT  '>> Truncating table: bronze.crm_sales_details';
        --Truncate the table before loading the data to avoid duplicates if the script is run multiple times
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT  '>> Inserting table: bronze.crm_sales_details';
        --Bulk load the data to the bronze schema for the from the crm source sales_details.csv file
        BULK INSERT bronze.crm_sales_details
        FROM '/home/abanoub/data_warehouse_project/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); -- End time for loading the crm_sales_details table
        PRINT '>> Load Time for bronze.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';

        PRINT '---------------------------------------------------------------------------';
        PRINT 'Loading ERP tables...';
        PRINT '---------------------------------------------------------------------------';
        PRINT ' ';

        ------------------------------------------------------------------------------------------
        ------------------------------------------------------------------------------------------

        SET @start_time = GETDATE(); -- Start time for loading the erp_cust_az12 table
        PRINT  '>> Truncating table: bronze.erp_cust_az12';
        --Truncate the table before loading the data to avoid duplicates if the script is run multiple times
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT  '>> Inserting table: bronze.erp_cust_az12';
        --Bulk load the data to the bronze schema for the from the crm source CUST_AZ12.csv file
        BULK INSERT bronze.erp_cust_az12
        FROM '/home/abanoub/data_warehouse_project/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); -- End time for loading the erp_cust_az12 table
        PRINT '>> Load Time for bronze.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';

        ------------------------------------------------------------------------------------------
        ------------------------------------------------------------------------------------------
        
        SET @start_time = GETDATE(); -- Start time for loading the erp_loc_a101 table
        PRINT  '>> Truncating table: bronze.erp_loc_a101';
        --Truncate the table before loading the data to avoid duplicates if the script is run multiple times
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT  '>> Inserting table: bronze.erp_loc_a101';
        --Bulk load the data to the bronze schema for the from the crm source LOC_A101.csv file
        BULK INSERT bronze.erp_loc_a101
        FROM '/home/abanoub/data_warehouse_project/datasets/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); -- End time for loading the erp_loc_a101 table
        PRINT '>> Load Time for bronze.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';

        ------------------------------------------------------------------------------------------
        ------------------------------------------------------------------------------------------
        
        SET @start_time = GETDATE(); -- Start time for loading the erp_px_cat_g1v2 table
        PRINT  '>> Truncating table: bronze.erp_px_cat_g1v2';
        --Truncate the table before loading the data to avoid duplicates if the script is run multiple times
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT  '>> Inserting table: bronze.erp_px_cat_g1v2';
        --Bulk load the data to the bronze schema for the from the crm source PX_CAT_G1V2.csv file
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/home/abanoub/data_warehouse_project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); -- End time for loading the erp_px_cat_g1v2 table
        PRINT '>> Load Time for bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT ' ';
        SET @end_time_patch = GETDATE(); -- End time for the entire loading process

        PRINT '---------------------------------------------------------------------------';  
        PRINT 'Finished loading data into the bronze schema!';
        PRINT 'Total Load Time: ' + CAST(DATEDIFF(SECOND, @start_time_patch, @end_time_patch) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------------------------------------------------------';
        PRINT ' ';
    END TRY

    BEGIN CATCH
        -- If an error occurs, print the error message
        PRINT '===========================================================================';
        PRINT 'An error occurred while loading data into the bronze schema:';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(50));        
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(50));
        PRINT '===========================================================================';
    END CATCH;
END;