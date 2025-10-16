# Document: Delivering a Data Model for Churn Analysis

## Objective

To create a data model that enables business analysts to analyze churn across the business, specifically focusing on identifying the main reasons for churn among our highest value customers over the last 12 months.

## Breakdown of Tasks

1. **Data Ingestion**
   * Ingest revenue data from Salesforce (already integrated into Snowflake).
   * Fetch churn reasons from the product API and load them into Snowflake.

2. **Data Transformation**
   * Create dbt models to transform the raw data into a usable format for analysis.
   * Join revenue data with churn reasons.

3. **Data Modeling**
   * Develop a final data model that aggregates churn reasons by customer segment and revenue.

4. **Documentation and Training**
   * Document the data model and provide guidance for business analysts on how to use the dataset.

---

## Project Structure

* **dbt Project Structure:**

```
my_dbt_project/
├── models/
│   ├── staging/
│   │   ├── stg_salesforce_revenue.sql
│   │   └── stg_churn_reasons.sql
│   ├── marts/
│   │   └── churn_analysis.sql
├── seeds/
├── snapshots/
├── macros/
└── dbt_project.yml
```

---

## SQL Snippets for dbt Models

### 1. Staging Model for Salesforce Revenue Data

```sql
-- models/staging/stg_salesforce_revenue.sql
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
```

### 2. Staging Model for Churn Reasons

```sql
-- models/staging/stg_churn_reasons.sql
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
```

### 3. Final Model for Churn Analysis

```sql
-- models/marts/churn_analysis.sql
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
```

---

## Data Ingestion from API

* **Fetching Churn Reasons:**
  * Use Python to call the API and retrieve churn reasons.
  * Save the data in a structured format and load it into Snowflake using a staging table.

```python
import requests
import pandas as pd
from sqlalchemy import create_engine

# API call to fetch churn reasons
response = requests.get('https://api.example.com/churn_reasons')
churn_data = response.json()

# Convert to DataFrame and load to Snowflake
churn_df = pd.DataFrame(churn_data)
engine = create_engine('snowflake://user:password@account/db/schema')
churn_df.to_sql('churn_reasons', con=engine, if_exists='replace', index=False)
```

---

## Connecting Data Sets

* **Joining Revenue and Churn Data:**
  * Ensure that the customer IDs are consistent across both datasets.
  * Use appropriate joins in the final dbt model to connect revenue data with churn reasons.

## Potential Problems and Solutions

* **Data Quality Issues:** Inconsistent customer IDs may lead to failed joins.  
  * **Solution:** Implement data validation checks before loading data into Snowflake.

* **API Rate Limiting:** The API may limit the number of requests.  
  * **Solution:** Implement backoff strategies and batch requests to avoid hitting limits.

* **Performance:** Large datasets may slow down queries.  
  * **Solution:** Optimize SQL queries and consider partitioning data in Snowflake.

---

## Ensuring Robustness of the Pipeline

* **Testing:** Implement tests in dbt for data integrity and transformation logic.
* **Monitoring:** Set up alerts for failures in the data ingestion process.
* **Documentation:** Maintain clear documentation for the pipeline and data model for future reference.

---

## Supporting Business Analysts

* **Documentation:** Provide a clear guide on how to access and query the final dataset.
* **Training Sessions:** Offer training sessions to familiarize analysts with the dataset and its structure.
* **User Friendly Views:** Create views in Snowflake to simplify access to the most relevant data for analysis.

This structured approach will ensure that the business analysts can effectively analyze churn and derive actionable insights to improve customer retention.
