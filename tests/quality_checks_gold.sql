-- verifying fact and dimention tables joining conditions 
-- Validates the integrity,consistancy and accuracy of the Gold Layer.


select * from gold.fact_sales f
left join gold.dim_customers d on f.customer_key=d.customer_key
where d.customer_key is null;

select * from gold.fact_sales f
left join gold.dim_products d on f.product_key=d.product_key
where d.product_key is null;
