from transfiPacks.executors.Executor import BigQueryExecutor,SnowflakeExecutor,SalesforceExecutor

if __name__ == '__main__':
    config = {'Snowflake':{'conn_id':'','username':'','database_name':'ra','schema_name':'','table_name':''},
              'Bigquery':{'project_id':'','database_name':'','table_name':''},
              'Salesforce':{'object_name':'','username':'','password':'','token':''}}
    bqex = BigQueryExecutor(config)
    sflkex = SnowflakeExecutor(config)