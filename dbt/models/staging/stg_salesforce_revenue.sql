with sales_data as (
    select
        customer_id,
        revenue,
        created_at
    from {{ source('salesforce', 'revenue') }}
)
select
    customer_id,
    sum(revenue) as total_revenue,
    max(created_at) as last_purchase_date
from sales_data
group by customer_id
