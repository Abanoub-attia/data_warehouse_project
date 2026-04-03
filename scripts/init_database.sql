/**/

USE master;
GO

--Drop and recreate database if exists DataWarehouse
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

--Use the created database
USE DataWarehouse;
GO

--Create for each layer of the Data Warehouse a schema
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO