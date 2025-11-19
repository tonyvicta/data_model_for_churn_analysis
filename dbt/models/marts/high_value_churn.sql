with revenue_12m as (
  select
    customer_id,
    sum(revenue) as revenue_12m
  from {{ source('salesforce', 'revenue') }}
  where created_at >= dateadd(month, -12, current_date)
  group by customer_id
),
ranked as (
  select
    customer_id,
    revenue_12m,
    ntile(10) over (order by revenue_12m desc) as decile
  from revenue_12m
),
churn as (
  select * from {{ ref('stg_churn_reasons') }}
)
select
  r.customer_id,
  r.revenue_12m,
  c.churn_reason,
  count(*) as churn_count
from ranked r
left join churn c using (customer_id)
where r.decile = 1
group by r.customer_id, r.revenue_12m, c.churn_reason
order by churn_count desc
