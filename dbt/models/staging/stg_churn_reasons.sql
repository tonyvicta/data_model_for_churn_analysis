with churn_data as (
    select
        customer_id,
        churn_reason,
        churn_date
    from {{ source('api', 'churn_reasons') }}
)
select
    customer_id,
    churn_reason,
    churn_date
from churn_data
where churn_date >= dateadd(month, -12, current_date)
