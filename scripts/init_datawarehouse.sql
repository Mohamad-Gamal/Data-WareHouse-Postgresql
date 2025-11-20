-- Drop the 'DataWarehouse' database if it exists
DROP DATABASE IF EXISTS "DataWarehouse";

-- Create the 'DataWarehouse' database
CREATE DATABASE "DataWarehouse";


-- Create Schemas
CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS gold;