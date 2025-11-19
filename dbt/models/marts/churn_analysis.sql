with revenue as (
    select * from {{ ref('stg_salesforce_revenue') }}
),
churn as (
    select * from {{ ref('stg_churn_reasons') }}
)
select
    r.customer_id,
    r.total_revenue,
    c.churn_reason,
    count(c.churn_reason) as churn_count
from revenue r
left join churn c on r.customer_id = c.customer_id
group by r.customer_id, r.total_revenue, c.churn_reason
