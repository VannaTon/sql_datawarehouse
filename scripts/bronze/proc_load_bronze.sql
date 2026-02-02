

create or alter procedure bronzen.load_bronze as
begin
declare @start_time datetime , @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
begin try


set @batch_start_time = GETDATE();
set @start_time = GETDATE();
truncate table bronzen.crm_cust_info
bulk insert bronzen.crm_cust_info from  'F:\BC\bigdata\assign work\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' 
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>> load duration: ' + cast( datediff(second, @start_time,@end_time) as nvarchar)
print '---------------------'
set @start_time = GETDATE();
truncate table bronzen.crm_prd_info
bulk insert bronzen.crm_prd_info from  'F:\BC\bigdata\assign work\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>> load duration: ' + cast( datediff(second, @start_time,@end_time) as nvarchar)
print '---------------------'
set @start_time = GETDATE();
truncate table bronzen.crm_sales_details
bulk insert bronzen.crm_sales_details from  'F:\BC\bigdata\assign work\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' 
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>> load duration: ' + cast( datediff(second, @start_time,@end_time) as nvarchar)
print '---------------------'

set @start_time = GETDATE();
truncate table bronzen.erp_cust_az102
bulk insert bronzen.erp_cust_az102 from  'F:\BC\bigdata\assign work\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>> load duration: ' + cast( datediff(second, @start_time,@end_time) as nvarchar)
print '---------------------' 
set @start_time = GETDATE();
truncate table bronzen.erp_loc_a101
bulk insert bronzen.erp_loc_a101 from  'F:\BC\bigdata\assign work\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' 
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>> load duration: ' + cast( datediff(second, @start_time,@end_time) as nvarchar)
print '---------------------'
set @start_time = GETDATE();
truncate table bronzen.erp_px_cat_g1v2
bulk insert bronzen.erp_px_cat_g1v2 from  'F:\BC\bigdata\assign work\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>> load duration: ' + cast( datediff(second, @start_time,@end_time) as nvarchar)
print '---------------------'

set @batch_end_time = GETDATE();

print'batch load duration:' + cast( datediff(second, @start_time,@end_time) as nvarchar)
end try
begin catch
print '=================='
print 'error massage:' + error_message();
print'error message: ' + cast (error_number() as nvarchar);
print'error message:' + cast (error_state() as nvarchar);
print '=================='
end catch
end



x
