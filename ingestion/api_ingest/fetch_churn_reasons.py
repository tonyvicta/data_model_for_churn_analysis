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
