from ..config.config import BigQueryConfig, SnowflakeConfig, SalesForceConfig

class BigQueryCreator(BigQueryConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()
    
    def create_schema(self):
        if self.fields !=None:
            self.target_schema = {}
            for i in self.fields:
                self.target_schema[i] = 'String'
        return self.target_schema

    
    def create_table(self):
        target_schema = self.create_schema()
        create_query = f"""CREATE OR REPLACE TABLE %s.%s.%s (%s)"""%(self.project_id,self.database_name,self.table_name,target_schema)
        self.con.query(create_query)

class SnowfalkeCreator(SnowflakeConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()
    
    def create_schema(self):
        if self.fields !=None:
            self.target_schema = {}
            for i in self.fields:
                self.target_schema[i] = 'String'
        return self.target_schema

    def create_table(self):
        target_schema = self.create_schema()
        create_query = f"""CREATE OR REPALCE TABLE %s.%s.%s (%s)"""%(self.database_name,self.schema_name,self.table_name,target_schema)
        self.con.cursor().execute(create_query)

class SalesforceCreator(SalesForceConfig):
    def __init__(self,configDict):
        super().__init__(configDict)
        self.con = self.get_con()
    
    def create_schema(self):
        if self.fields !=None:
            self.target_schema = {}
            for i in self.fields:
                self.target_schema[i] = 'String'
        return self.target_schema

    def create_table(self):
        return 'Done'