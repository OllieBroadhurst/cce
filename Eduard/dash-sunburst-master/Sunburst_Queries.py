Overall_Query = """with cte as (
SELECT DISTINCT
orders.source,
orders.ORIGINAL_SALES_CHANNEL_DESC,
orders.ACCOUNT_NO_ANON,  
if(disputes.ACCOUNT_NO_ANON is null, 0, 1) has_dispute
FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon` orders
left join (SELECT DISTINCT ACCOUNT_NO_ANON FROM `bcx-insights.telkom_customerexperience.disputes_20190903_00_anon`) disputes
on orders.ACCOUNT_NO_ANON = disputes.ACCOUNT_NO_ANON)

select * except(CUSTOMER_NO_ANON) from cte

join  (select 
CUSTOMER_BRAND,
SERVICE_TYPE,
CUSTOMER_NO_ANON from `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon` 

group by 
CUSTOMER_NO_ANON,
CUSTOMER_BRAND,
SERVICE_TYPE,
CREDIT_CLASS_DESC,
SOURCE) customers

on customers.CUSTOMER_NO_ANON = cte.ACCOUNT_NO_ANON
GROUP BY cte.source,cte.ORIGINAL_SALES_CHANNEL_DESC ,cte.ACCOUNT_NO_ANON ,cte.has_dispute,customer_brand,service_type
ORDER BY cte.source,cte.ORIGINAL_SALES_CHANNEL_DESC DESC
limit 500000
"""

