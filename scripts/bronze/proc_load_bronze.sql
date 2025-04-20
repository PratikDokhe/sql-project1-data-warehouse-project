/*

===================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===================================================================================================

Script Purpose:

- This Procedure loads data into 'bronze' schema from external CSV files (Source).
- Truncates the bronze tables before loading data.
- Uses 'BULK INSERT' command.

----------------------------------
To Run: EXEC bronze.load_bronze;
---------------------------------
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_s_time DATETIME, @batch_e_time DATETIME;

	set @batch_s_time=GETDATE();

	BEGIN TRY
		print '=================================';
		print ' --- Loading Bronze layer --- ';
		print '================================= ';

		print '- Loading CRM Tables -';

		set @start_time=getdate();
		print CHAR(13) + CHAR(10)+'Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		print 'Inserting Data into table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		from 'K:\Learning 2024\SQL\SQL Projects\Project 1 Data warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
	
		)
		set @end_time=getdate();
		print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) AS NVARCHAR)

		set @start_time=getdate();
		print CHAR(13) + CHAR(10) +'Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		print 'Inserting Data into table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		from 'K:\Learning 2024\SQL\SQL Projects\Project 1 Data warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
	
		)
		set @end_time=getdate();
		print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) AS NVARCHAR)

		set @start_time=getdate();
		print CHAR(13) + CHAR(10)+'Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		print 'Inserting Data into table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		from 'K:\Learning 2024\SQL\SQL Projects\Project 1 Data warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
	
		)
		set @end_time=getdate();
		print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) AS NVARCHAR)


		print CHAR(13) + CHAR(10)+'- Loading ERP Tables -';

		set @start_time=getdate();

		print CHAR(13) + CHAR(10)+'Truncating table: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		print 'Inserting Data into table: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		from 'K:\Learning 2024\SQL\SQL Projects\Project 1 Data warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
	
		)
		set @end_time=getdate();
		print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) AS NVARCHAR)

		set @start_time=getdate();
		print CHAR(13) + CHAR(10)+'Truncating table: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;

		print 'Inserting Data into table: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		from 'K:\Learning 2024\SQL\SQL Projects\Project 1 Data warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
	
		)
		set @end_time=getdate();
		print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) AS NVARCHAR)

		set @start_time=getdate();
		print CHAR(13) + CHAR(10)+'Truncating table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		print 'Inserting Data into table: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		from 'K:\Learning 2024\SQL\SQL Projects\Project 1 Data warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
	
		)

		set @end_time=getdate();
		print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) AS NVARCHAR)

	END TRY
	BEGIN CATCH
		print 'Error Occured During Loading Bronze Layer';
		print 'Error Number: ' + cast(error_number() as NVARCHAR);
		print 'Error State: ' + cast(error_state() as NVARCHAR);
	END CATCH

	set @batch_e_time=GETDATE();

	print '=================================';
	print 'Loading Bronze layer Completed';
	print 'Time Required to load whole Bronze Layer: '+ cast(datediff(second,@batch_s_time,@batch_e_time) as NVARCHAR);
	print '=================================';
END
