from ..config.config import BigQueryConfig,SnowflakeConfig,SalesForceConfig
import os
import pandas as pd
import pandas_gbq
from snowflake.connector.pandas_tools import write_pandas


class BigQueryExecutor(BigQueryConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()
    
    def loader(self,df):
        self.table_ref = self.database_name + '.' + self.table_name
        pandas_gbq.to_gbq(df,self.table_ref,self.project_id,if_exists='append')

    def leecher(self):
        sql = self.get_sql()
        df = self.con.query(sql)
        return df


class SnowflakeExecutor(SnowflakeConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()

    def loader(self,df):
        write_pandas(self.con,df,self.table_name,self.schema_name)

    def leecher(self):
        sql = self.get_sql()
        cur = self.con.cur()
        df = cur.execute(sql)
        return df

class SalesforceExecutor(SalesForceConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()
    
    def leecher(self):
        sql = self.get_sql()
        data = self.con.query_all(sql)
        df = pd.DataFrame(data['records']).drop(columns='attributes')
        return df


