/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `copy` command to load whole data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
   call bronze.load_bronze();
===============================================================================
*/



create or replace PROCEDURE bronze.load_bronze() 
LANGUAGE plpgsql
as $$
	declare 
	start_time time; 
	end_time time;
	batch_start_time time;
	batch_end_time time;
	duration_seconds DOUBLE PRECISION;
    duration_minutes INT;
    remaining_seconds INT;
begin
	batch_start_time := CURRENT_TIME;
	RAISE NOTICE '--Load data in bronze layer';
	RAISE NOTICE '-------------------------------------------------------------------';
	RAISE NOTICE '--Load data cust_info csv from crm into bronze.crm_cust_info table';
	RAISE NOTICE '-------------------------------------------------------------------';
	start_time := CURRENT_TIME::time;
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	Truncate table bronze.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
	COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	FROM 'C:\\Users\\gimep\\Downloads\\workspace\\dataWareHouseProject\\project\\datasets\\source_crm\\cust_info.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := now()::time;
	-- Calculate duration in seconds
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    -- Convert to minutes and remaining seconds
    duration_minutes := FLOOR(duration_seconds / 60);
    remaining_seconds := MOD(duration_seconds::INT, 60);
    -- Print duration
    RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
	
	----------------------------------------------------------------------------------------
	
	RAISE NOTICE '--Load data prd_info csv from crm into bronze.crm_prd_info table';
	RAISE NOTICE '-------------------------------------------------------------------';
	start_time := now()::time;
	RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	Truncate table bronze.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
	COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	FROM 'C:\\Users\\gimep\\Downloads\\workspace\\dataWareHouseProject\\project\\datasets\\source_crm\\prd_info.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := now()::timestamp;
	-- Calculate duration in seconds
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    -- Convert to minutes and remaining seconds
    duration_minutes := FLOOR(duration_seconds / 60);
    remaining_seconds := MOD(duration_seconds::INT, 60);
    -- Print duration
    RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
	
	----------------------------------------------------------------------------------------
	
	RAISE NOTICE '--Load data sales_details csv from crm into bronze.crm_sales_details table';
	RAISE NOTICE '-------------------------------------------------------------------';
	start_time := now()::timestamp;
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	Truncate table bronze.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
	COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	FROM 'C:\\Users\\gimep\\Downloads\\workspace\\dataWareHouseProject\\project\\datasets\\source_crm\\sales_details.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := now()::timestamp;
	-- Calculate duration in seconds
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    -- Convert to minutes and remaining seconds
    duration_minutes := FLOOR(duration_seconds / 60);
    remaining_seconds := MOD(duration_seconds::INT, 60);
    -- Print duration
    RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
	
	----------------------------------------------------------------------------------------

	RAISE NOTICE '--Load data cust_az12 csv from crm into bronze.erp_cust_az12 table';
	RAISE NOTICE '-------------------------------------------------------------------';
	start_time := now()::timestamp;
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	Truncate table bronze.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
	COPY bronze.erp_cust_az12 (cid, bdate, gen)
	FROM 'C:\\Users\\gimep\\Downloads\\workspace\\dataWareHouseProject\\project\\datasets\\source_erp\\cust_az12.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := now()::timestamp;
	-- Calculate duration in seconds
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    -- Convert to minutes and remaining seconds
    duration_minutes := FLOOR(duration_seconds / 60);
    remaining_seconds := MOD(duration_seconds::INT, 60);
    -- Print duration
    RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
	
	----------------------------------------------------------------------------------------

	RAISE NOTICE '--Load data loc_a101 csv from crm into bronze.erp_loc_a101 table';
	RAISE NOTICE '-------------------------------------------------------------------';
	start_time := now()::timestamp;
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	Truncate table bronze.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
	COPY bronze.erp_loc_a101 (cid, cntry)
	FROM 'C:\\Users\\gimep\\Downloads\\workspace\\dataWareHouseProject\\project\\datasets\\source_erp\\loc_a101.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := now()::timestamp;
	-- Calculate duration in seconds
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    -- Convert to minutes and remaining seconds
    duration_minutes := FLOOR(duration_seconds / 60);
    remaining_seconds := MOD(duration_seconds::INT, 60);
    -- Print duration
    RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
	
	----------------------------------------------------------------------------------------

	RAISE NOTICE '--Load data px_cat_g1v2 csv from crm into bronze.erp_px_cat_g1v2 table';
	RAISE NOTICE '-------------------------------------------------------------------';  
	start_time := now()::timestamp;
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	Truncate table bronze.erp_px_cat_g1v2;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2 (id, cat, subcat,maintenance)
	FROM 'C:\\Users\\gimep\\Downloads\\workspace\\dataWareHouseProject\\project\\datasets\\source_erp\\px_cat_g1v2.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := now()::timestamp;
	-- Calculate duration in seconds
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    -- Convert to minutes and remaining seconds
    duration_minutes := FLOOR(duration_seconds / 60);
    remaining_seconds := MOD(duration_seconds::INT, 60);
    -- Print duration
    RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
	
	----------------------------------------------------------------------------------------
	batch_end_time := CURRENT_TIMESTAMP;
	-- Calculate batch duration in seconds
    duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    -- Convert to minutes and remaining seconds
    duration_minutes := FLOOR(duration_seconds / 60);
    remaining_seconds := MOD(duration_seconds::INT, 60);
    -- Print batch duration
	RAISE NOTICE '==========================================';
	RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '>> batch duration: % minutes and % seconds', duration_minutes, remaining_seconds;
	RAISE NOTICE '=========================================='

	-----------------------------------------------
	
	EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'Error SQLSTATE: %', SQLSTATE;
            RAISE NOTICE '==========================================';
end; $$;
