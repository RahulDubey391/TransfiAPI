from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from google.cloud import bigquery
from simple_salesforce import Salesforce

class SnowflakeConfig:
    def __init__(self,configDict):
        self.sflkconfigDict = configDict['Snowflake']
        self.sflk_conn_id = self.sflkconfigDict['conn_id']
        self.database_name = self.sflkconfigDict['database_name']
        self.schema_name = self.sflkconfigDict['schema_name']
        self.table_name = self.sflkconfigDict['table_name']
        if 'columns' in self.sflkconfigDict.keys():
            self.fields = self.sflkconfigDict['columns']
        else:self.fields = None

    def __repr__(self):
        return '<SnowflakeConfig %s %s %s>'%(self.database_name,self.schema_name,self.table_name)

    def get_con(self):
        return SnowflakeHook(snowflake_conn_id=self.sflk_conn_id,
                                database=self.database_name)
    
    def get_sql(self):
        if self.fields != None:
            return "SELECT %s FROM %s.%s.%s"%(self.fields,self.database_name,self.schema_name,self.table_name)
        else:
            return "SELECT * FROM %s.%s.%s"%(self.database_name,self.schema_name,self.table_name) 


class BigQueryConfig:
    def __init__(self,configDict):
        self.bqconfigDict = configDict['Bigquery']
        self.project_id = self.bqconfigDict['project_id']
        self.database_name = self.bqconfigDict['database_name']
        self.table_name = self.bqconfigDict['table_name']
        if 'columns' in self.bqconfigDict.keys():
            self.fields = self.bqconfigDict['columns']
        else:self.fields = None

    def __repr__(self):
        return '<BigqueryConfig %s %s %s>'%(self.project_id,self.database_name,self.table_name)

    def get_con(self):
        bq = bigquery.Client(self.project_id)
        return bq

    def get_sql(self):
        if self.fields != None:
            return "SELECT %s FROM %s.%s.%s"%(self.fields,self.project_id,self.database_name,self.table_name)
        else:
            return "SELECT * FROM %s.%s.%s"%(self.project_id,self.database_name,self.table_name)        



class SalesForceConfig:
    def __init__(self,configDict):
        self.salesconfigDict = configDict['Salesforce']
        self.object_name = self.salesconfigDict['object_name']
        self.username = self.salesconfigDict['username']
        self.password = self.salesconfigDict['password']
        self.token = self.salesconfigDict['token']
        if 'columns' in self.salesconfigDict.keys():
            self.fields = self.salesconfigDict['columns']
        else:self.fields = None

    def __repr__(self):
        return '<SalesfroceConfig %s %s %s %s>'%(self.object_name,self.username,self.password,self.token)
    
    def get_con(self):
        sf = Salesforce(username=self.username,
                          password=self.password,
                          security_token=self.token)
        return sf

    def get_sql(self):
        if self.fields == None:
            con = self.get_con()
            desc = getattr(con,self.object_name).describe()
            fieldNames = [field['name'] for field in desc['fields']]
            self.fields = fieldNames
            soql = "SELECT {} FROM {}".format(i = ','.join(self.fields),j=self._object_name)
            return soql
        else:
            soql = "SELECT {} FROM {}".format(i = ','.join(self.fields),j=self._object_name)
            return soql



