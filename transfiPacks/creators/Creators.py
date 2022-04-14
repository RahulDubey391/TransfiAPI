from ..config.config import BigQueryConfig, SnowflakeConfig, SalesForceConfig

class BigQueryCreator(BigQueryConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()
    
    def create_table(self):
        create_query = f"""CREATE OR REPLACE TABLE %s.%s.%s"""%(self.project_id,self.database_name,self.table_name)
        self.con.query(create_query)

class SnowfalkeCreator(SnowflakeConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()
    
    def create_table(self):
        create_query = f"""CREATE OR REPALCE TABLE %s.%s.%s"""%(self.database_name,self.schema_name,self.table_name)
        self.con.cursor().execute(create_query)

class SalesforceCreator(SalesForceConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()
    
    def create_table(self):
        return 'Done'