/*

===================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===================================================================================================

Script Purpose:

- This Procedure loads data into 'silver' schema from bronze schema.
- Truncates the silver tables before loading data.
- Inserts Transformed and cleaned data from Bronze into Silver Tables.

----------------------------------
To Run: EXEC silver.load_silver;
---------------------------------
*/


create or alter procedure silver.load_silver as 

BEGIN
	BEGIN TRY
		
		DECLARE @Start_time DATETIME, @END_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;

		print '=================================';
		print ' --- Loading Silver layer --- ';
		print '================================= ';

		SET @batch_start_time=GETDATE();

		print CHAR(13) + CHAR(10)+'- Loading CRM Tables -';

		SET @Start_time=GETDATE();

		print CHAR(13) + CHAR(10)+'Truncating table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print 'Inserting Data into table: silver.crm_cust_info';
		insert into silver.crm_cust_info(
		cust_id,
		cust_key,
		cst_firstname,
		cst_lastname,
		cst_gndr,
		cst_marital_status,
		cst_create_date
		)

		select 
		cust_id,
		cust_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,

		case 
		when upper(trim(cst_gndr)) = 'M' then 'Male'
		when upper(trim(cst_gndr)) = 'F' then 'Female'
		else 'n/a'
		end as cst_gndr,

		case 
		when upper(trim(cst_marital_status)) = 'S' then 'Single'
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
		else 'n/a'
		end as cst_marital_status,
		cst_create_date


		from(
		select *,
		ROW_NUMBER() over(partition by cust_id order by cst_create_date desc) as rn
		from bronze.crm_cust_info
		)t where rn = 1 and cust_id is not null;

		SET @END_time=GETDATE();

		print 'Time required to load table: silver.crm_cust_info ' + cast(datediff(second,@Start_time,@END_time) as NVARCHAR)

		SET @Start_time=GETDATE();

		print CHAR(13) + CHAR(10)+'Truncating table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print 'Inserting Data into table: silver.crm_prd_info';

		insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, 
		SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'n/a' 
		end as prd_line,
		prd_start_dt,
		prd_end_dt
		--dateadd(DAY,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt2
		from bronze.crm_prd_info        

		SET @END_time=GETDATE();

		print 'Time required to load table: silver.crm_prd_info ' + cast(datediff(second,@Start_time,@END_time) as NVARCHAR)


		SET @Start_time=GETDATE();

		print CHAR(13) + CHAR(10)+'Truncating table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print 'Inserting Data into table: silver.crm_sales_details';

		insert into silver.crm_sales_details(
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
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		--sls_order_dt,
		case 
			when sls_order_dt = 0 OR len(sls_order_dt) !=8 then null
			else cast(cast(sls_order_dt as VARCHAR ) as DATE)
		end as sls_order_dt,


		--sls_ship_dt,
		case 
			when sls_ship_dt = 0 OR len(sls_ship_dt) !=8 then null
			else cast(cast(sls_ship_dt as VARCHAR ) as DATE)
		end as sls_ship_dt,

		--sls_due_dt,
		case 
			when sls_due_dt = 0 OR len(sls_due_dt) !=8 then null
			else cast(cast(sls_due_dt as VARCHAR ) as DATE)
		end as sls_due_dt,

		--sls_sales

		--sls_price,
		case 
			when sls_sales != (abs(sls_price)*sls_quantity) or sls_sales is null or sls_sales<=0
				then sls_quantity * abs(sls_price)
			else sls_sales
		end as sls_sales,

		sls_quantity,

		case 
			when sls_price<=0 or sls_price is null then sls_sales/nullif(sls_quantity,0)
			else sls_price
		end as sls_price

		from bronze.crm_sales_details;



		SET @END_time=GETDATE();

		print 'Time required to load table: silver.crm_sales_details ' + cast(datediff(second,@Start_time,@END_time) as NVARCHAR)


		print CHAR(13) + CHAR(10)+'- Loading ERP Tables -';
		
		SET @Start_time=GETDATE();

		print CHAR(13) + CHAR(10)+'Truncating table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print 'Inserting Data into table: silver.erp_cust_az12';
		
		insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)

		select
		--cid
		case 
			when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
			else cid
		end as cid,
		--bdate,
		case 
			when bdate>GETDATE() then null
			else bdate
		end as bdate,
		--gen,
		case 
			when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			when upper(trim(gen)) in ('M','MALE') then 'Male'
			else 'n/a'
		end as gen
		from bronze.erp_cust_az12;

		SET @END_time=GETDATE();

		print 'Time required to load table: silver.erp_cust_az12 ' + cast(datediff(second,@Start_time,@END_time) as NVARCHAR)

		SET @Start_time=GETDATE();

		print CHAR(13) + CHAR(10)+'Truncating table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print 'Inserting Data into table: silver.erp_loc_a101';

		insert into silver.erp_loc_a101(
		cid,
		cntry)

		select 
		replace(cid,'-','') as cid,
		--CNTRY,
		case 
			when upper(trim(CNTRY)) in ('US','USA') then 'United Kingdom'
			when upper(trim(cntry)) = 'DE' then 'Germany'
			when trim(cntry) ='' or trim(cntry) is null then 'n/a'
			else trim(cntry)
		end as cntry

		from bronze.erp_loc_a101;

		SET @END_time=GETDATE();

		print 'Time required to load table: silver.erp_loc_a101 ' + cast(datediff(second,@Start_time,@END_time) as NVARCHAR)

		SET @Start_time=GETDATE();

		print CHAR(13) + CHAR(10)+'Truncating table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print 'Inserting Data into table: silver.erp_px_cat_g1v2';

		insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		SUBCAT,
		MAINTENANCE
		)
		select 
		id,
		cat,
		subcat,
		MAINTENANCE
		from bronze.erp_px_cat_g1v2;

		SET @END_time=GETDATE();

		print 'Time required to load table: silver.erp_px_cat_g1v2 ' + cast(datediff(second,@Start_time,@END_time) as NVARCHAR)
	
	END TRY
	BEGIN CATCH
	print '-------------------------------------------';
	print 'Error Occured During Loading Bronze Layer'
	print 'Error Number:  ' + cast(Error_message() as NVARCHAR)
	print 'Error State: ' + cast(error_state() as NVARCHAR)
	print '-------------------------------------------';

	END CATCH

	SET @batch_end_time=GETDATE();

	print CHAR(13) + CHAR(10)+'=================================';
	print 'Loading Silver layer Completed';
	print 'Time Required to load whole Silver Layer: '+ cast(datediff(second,@batch_start_time,@batch_end_time) as NVARCHAR);
	print '=================================';
END
