/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    call Silver.load_silver();
===============================================================================
*/
create or replace PROCEDURE silver.load_silver() 
LANGUAGE plpgsql
as $$
    DECLARE 
	start_time time;
	end_time time;
	duration_seconds DOUBLE PRECISION;
    duration_minutes INT;
    remaining_seconds INT;
	batch_start_time time;
	batch_end_time time; 
BEGIN
        batch_start_time := current_time;
        RAISE NOTICE '================================================';
        RAISE NOTICE 'Loading Silver Layer';
        RAISE NOTICE '================================================';

		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '------------------------------------------------';

		-- Loading silver.crm_cust_info
		start_time := CURRENT_TIME;
		raise NOTICE '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		raise NOTICE '>> Inserting Data Into: silver.crm_cust_info';
		insert into silver.crm_cust_info
		(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 	cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
			
			from(SELECT *,
				row_number() over(PARTITION by cst_id order by cst_create_date desc) as flag_last
				from bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
				) where flag_last = 1;
			end_time := CURRENT_TIME;
			duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
			-- Convert to minutes and remaining seconds
			duration_minutes := FLOOR(duration_seconds / 60);
			remaining_seconds := MOD(duration_seconds::INT, 60);
			RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
			raise NOTICE '>> -------------';

		-- Loading silver.crm_prd_info
		start_time := CURRENT_TIME;
		raise NOTICE '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		raise NOTICE '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT
				prd_id,
				REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
				SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,        -- Extract product key
				prd_nm,
				COALESCE(prd_cost, 0) AS prd_cost,
				CASE UPPER(TRIM(prd_line))
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'
				END AS prd_line, -- Map product line codes to descriptive values
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(
					(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' )
					AS DATE
				) AS prd_end_dt -- Calculate end date as one day before the next start date
			FROM bronze.crm_prd_info;
	        end_time := CURRENT_TIME;
			duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		    -- Convert to minutes and remaining seconds
		    duration_minutes := FLOOR(duration_seconds / 60);
		    remaining_seconds := MOD(duration_seconds::INT, 60);
			RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
			raise NOTICE '>> -------------';
	
	
		-- Loading crm_sales_details
		start_time := CURRENT_TIME;
		RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
		end_time := CURRENT_TIME;
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		-- Convert to minutes and remaining seconds
		duration_minutes := FLOOR(duration_seconds / 60);
		remaining_seconds := MOD(duration_seconds::INT, 60);
		RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
		raise NOTICE '>> -------------';

		-- Loading erp_cust_az12
		start_time := CURRENT_TIME;
		RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > CURRENT_DATE THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
		end_time := CURRENT_TIME;
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
	    -- Convert to minutes and remaining seconds
	    duration_minutes := FLOOR(duration_seconds / 60);
	    remaining_seconds := MOD(duration_seconds::INT, 60);
		RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
		raise NOTICE '>> -------------';
		
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '------------------------------------------------';
		
		-- Loading erp_loc_a101
		start_time := CURRENT_TIME;
		RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
		end_time := CURRENT_TIME;
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
	    -- Convert to minutes and remaining seconds
	    duration_minutes := FLOOR(duration_seconds / 60);
	    remaining_seconds := MOD(duration_seconds::INT, 60);
		RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
		raise NOTICE '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		start_time := CURRENT_TIME;
		RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		end_time := CURRENT_TIME;
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
	    -- Convert to minutes and remaining seconds
	    duration_minutes := FLOOR(duration_seconds / 60);
	    remaining_seconds := MOD(duration_seconds::INT, 60);
		RAISE NOTICE '>> Task duration: % minutes and % seconds', duration_minutes, remaining_seconds;
		raise NOTICE '>> -------------';
		
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
	RAISE NOTICE '==========================================';

	-----------------------------------------------
		
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		RAISE NOTICE 'Error Message: %', SQLERRM;
		RAISE NOTICE 'Error SQLSTATE: %', SQLSTATE;
		RAISE NOTICE '==========================================';

END;  $$;
