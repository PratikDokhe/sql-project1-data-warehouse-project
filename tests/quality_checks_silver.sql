/* This are some of the quality check performed on bronze table and resolved them during loading silver layer.
note: this are some of the queries as I have reused them for every table.
*/



 --Check for duplicate or Nulls in Primary key
select prd_id,count(*) as x from silver.crm_prd_info 
group by prd_id having count(*)>1 or prd_id is null;

-- check unwanted spaces for string values
select prd_nm from silver.crm_prd_info 
where prd_nm != trim(prd_nm);


-- Data Standarization & Consistancy:
select distinct gen from silver.erp_cust_az12;

select
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != (sls_price*sls_quantity)
or sls_sales<=0 or sls_quantity <=0 or sls_price<=0
or sls_sales is null or sls_quantity is null or sls_price is null;


-- Check for invalid date orders
select * 
from silver.crm_prd_info 
where prd_end_dt<prd_start_dt;



select * from silver.erp_cust_az12; 
