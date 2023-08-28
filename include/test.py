from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.snowpark import Session

print("Hello")
print(SnowflakeHook.__module__)
print(Session.__module__)