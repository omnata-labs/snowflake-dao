"""
Generates the Data Access Objects
"""
import json
import os
import re
from pathlib import Path
from typing import Dict, Optional
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
from jinja2 import Environment, FileSystemLoader

class ObjectsGenerator:
    """
    Uploads plugins to a Snowflake account and registers them with the Omnata app
    """
    def __init__(self,snowflake_connection_parameters,database:str, schema:str, include_schema:bool = True,
                 ignore_table_regex:Optional[str]=None):
        if snowflake_connection_parameters.__class__.__name__ == 'SnowflakeConnection':
            builder = Session.builder
            builder._options['connection']=snowflake_connection_parameters
            self.session = builder.create()
        else:
            self.session = Session.builder.configs(snowflake_connection_parameters).create()
        self.database = database
        self.schema = schema
        self.tables={}
        self.include_schema = include_schema
        self.ignore_table_regex = ignore_table_regex

    
    def snowflake_data_type_to_python(self,snowflake_type:str):
        if snowflake_type=='NUMBER':
            return 'int'
        if snowflake_type=='VARCHAR':
            return 'str'
        if snowflake_type=='OBJECT':
            return 'Dict'
        if snowflake_type=='VARIANT':
            return 'Any'
        if snowflake_type=='ARRAY':
            return 'List'
        if snowflake_type=='BOOLEAN':
            return 'bool'
        return 'str'
    
    def default_declaration(self,column):
        # If it's a sequence, we need to handle it differently.
        # The value will be generated first and stored, so that we know what it is before inserting
        # (Snowflake doesn't provide a way to determine the sequence value from last insert)
        if '.NEXTVAL' in column['COLUMN_DEFAULT']:
            sequence_name = column['COLUMN_DEFAULT'].split('.')[-2]
            full_sequence_name = f"{column['TABLE_SCHEMA']}.{sequence_name}" if self.include_schema else sequence_name
            return f"Sequence('{full_sequence_name}')"
        # for other defaults, we wrap it in a snowpark sql_expr so it gets evaluated
        return f"sql_expr(\"\"\"{column['COLUMN_DEFAULT']}\"\"\")"
    
    def get_unique_keys(self):
        self.session.sql(f"""SHOW UNIQUE KEYS in SCHEMA {self.database}.{self.schema};""").collect()
        return self.session.sql("""select * from table(result_scan(LAST_QUERY_ID()));""").collect()
    
    def get_primary_keys(self):
        self.session.sql(f"""SHOW PRIMARY KEYS in SCHEMA {self.database}.{self.schema};""").collect()
        return self.session.sql("""select * from table(result_scan(LAST_QUERY_ID()));""").collect()

    def get_foreign_keys(self):
        self.session.sql(f"""SHOW IMPORTED KEYS in SCHEMA {self.database}.{self.schema};""").collect()
        # creates a "single foreign key relationship" record by aggregating all the columns into an array
        foreign_keys = []
        rows = self.session.sql("""select "pk_database_name",
            "pk_schema_name",
            "pk_table_name", 
            "fk_database_name",
            "fk_schema_name",
            "fk_table_name",
            "fk_name",
            array_agg("pk_column_name") within group (order by "key_sequence") as "pk_column_names",
            array_agg("fk_column_name") within group (order by "key_sequence") as "fk_column_names"
    from table(result_scan(LAST_QUERY_ID()))
    group by "pk_database_name",
            "pk_schema_name",
            "pk_table_name", 
            "fk_database_name",
            "fk_schema_name",
            "fk_table_name",
            "fk_name"
    order by "pk_database_name",
            "pk_schema_name",
            "pk_table_name";""").collect()
        for row in rows:
            row_dict = row.as_dict()
            if self.ignore_table_regex is not None:
                if re.match(self.ignore_table_regex,row['fk_table_name']) or \
                    re.match(self.ignore_table_regex,row['pk_table_name']):
                    continue
            row_dict['pk_column_names'] = json.loads(row_dict['pk_column_names'])
            row_dict['fk_column_names'] = json.loads(row_dict['fk_column_names'])
            foreign_keys.append(row_dict)
        return foreign_keys

    def get_tables(self):
        return self.session.table(f'{self.database}.INFORMATION_SCHEMA.TABLES') \
            .filter((col('TABLE_CATALOG')==lit(self.database)) & (col('TABLE_SCHEMA')==lit(self.schema))) \
            .order_by([col('TABLE_NAME')]).collect()

    def get_columns(self):
        return self.session.table(f'{self.database}.INFORMATION_SCHEMA.COLUMNS') \
            .filter((col('TABLE_CATALOG')==lit(self.database)) & (col('TABLE_SCHEMA')==lit(self.schema))) \
            .order_by([col('TABLE_NAME'),col('ORDINAL_POSITION')]).collect()

    def analyse(self):
        unique_keys = self.get_unique_keys()
        print(f"Found {len(unique_keys)} unique keys")
        primary_keys = self.get_primary_keys()
        print(f"Found {len(primary_keys)} primary keys")
        foreign_keys = self.get_foreign_keys()
        print(f"Found {len(foreign_keys)} foreign keys")

        tables = self.get_tables()
        print(f"Found {len(tables)} tables")

        columns = self.get_columns()
        print(f"Found {len(columns)} columns")
        
        for table in tables:
            if self.ignore_table_regex is not None and re.match(self.ignore_table_regex,table['TABLE_NAME']):
                print(f"Ignoring table {table['TABLE_NAME']}")
                continue
            table_name = table['TABLE_NAME']
            pks = [key_column['column_name'] for key_column in primary_keys if key_column['table_name']==table_name]
            uniq = [key_column['column_name'] for key_column in unique_keys if key_column['table_name']==table_name]
            
            self.tables[table_name]={
                "primary_key_columns": pks,
                "unique_columns": pks+uniq,
                "multi_lookups": {
                    fk['fk_name']:{
                        "other_table":fk['fk_table_name'],
                        "local_cols":fk['pk_column_names'],
                        "remote_cols":fk['fk_column_names']
                    }
                    for fk in foreign_keys if fk['pk_table_name']==table_name
                },
                "single_lookups": {
                    fk['fk_name']:{
                        "other_table":fk['pk_table_name'],
                        "local_cols":fk['fk_column_names'],
                        "remote_cols":fk['pk_column_names']
                    }
                    for fk in foreign_keys if fk['fk_table_name']==table_name
                },
                "schema":table['TABLE_SCHEMA'],
                "columns":{col['COLUMN_NAME']:col for col in columns if col['TABLE_NAME']==table_name}
            }
    def python_data_type(self,type_overrides:Dict,column:Dict):
        if column['TABLE_NAME'] in type_overrides:
            if column['COLUMN_NAME'] in type_overrides[column['TABLE_NAME']]:
                return type_overrides[column['TABLE_NAME']][column['COLUMN_NAME']]
        return self.snowflake_data_type_to_python(column['DATA_TYPE'])

    def generate(self,file_name:str,exta_imports:str = None,type_overrides:Dict=None):
        if len(self.tables)==0:
            print("No tables to output. Did you analyse the schema first?")
            return
        print(f"Generating {file_name}")
        templates_path = os.path.join(Path(__file__).parent)
        environment = Environment(loader=FileSystemLoader(templates_path))
        template = environment.get_template('file_template.jinja')
        content = template.render({
            "imports": exta_imports
        })

        with open(file_name, 'w',encoding='utf-8') as output_file:
            output_file.write(content)
            for table_name,table_data in self.tables.items():
                column_parameters = [f"{' '*16}{column['COLUMN_NAME']}:{self.python_data_type(type_overrides,column)}" for column in table_data['columns'].values()]
                column_parameters_joined = ',\n'.join(column_parameters)
                # these will appear first in the create method, since no default value can be provided
                column_parameters_not_nullable_no_default = [f"{' '*4}{column['COLUMN_NAME']}:{self.python_data_type(type_overrides,column)}" for column in table_data['columns'].values() if column['IS_NULLABLE']=='NO' and (column['COLUMN_DEFAULT'] is None)]
                # these will appear next in the create method, since a default value can be set
                column_parameters_not_nullable_default = [f"{' '*4}{column['COLUMN_NAME']}:{self.python_data_type(type_overrides,column)} = {self.default_declaration(column)}" for column in table_data['columns'].values() if column['IS_NULLABLE']=='NO' and (column['COLUMN_DEFAULT'] is not None)]
                # these will appear next in the create method, since a default value of null can be set
                column_parameters_nullable = [f"{' '*4}{column['COLUMN_NAME']}:Optional[{self.python_data_type(type_overrides,column)}] = field(default=None)" for column in table_data['columns'].values() if column['IS_NULLABLE']=='YES']
                primary_key_param='None'
                if len(table_data['primary_key_columns']) > 0:
                    # just use the first primary key column
                    primary_key_param=f"'{table_data['primary_key_columns'][0]}'"
                all_column_parameters_joined = '\n'.join(column_parameters_not_nullable_no_default+column_parameters_not_nullable_default+column_parameters_nullable)
                #column_assignments = [f"{' '*4}{column['COLUMN_NAME']} = {column['COLUMN_NAME']}" for column in table_data['columns'].values()]
                #column_assignments_joined = '\n'.join(column_assignments)
                full_table_name=f"{table_data['schema']}.{table_name}" if self.include_schema else table_name
                output_file.write(f"""
@dataclass
class {table_name}(SnowflakeTable):
    _table_name='{full_table_name}'
    _primary_key_column={primary_key_param}

{all_column_parameters_joined}

    def create(self,
            session) -> {table_name}:
        return self._create_object(session)
""")
                # That's the constructor and the standard create method taken care of
                # now we'll generate methods for doing lookups.
                # Any column which is marked as UNIQUE, we can do a point lookup on
                for unique_column_name in table_data['unique_columns']:
                    column = table_data['columns'][unique_column_name]
                    column_name_lower = unique_column_name.lower()
                    output_file.write(f"""
    @classmethod
    def lookup_by_{column_name_lower}(cls,session,{column_name_lower}:{self.python_data_type(type_overrides,column)}) -> {table_name}:
        return {table_name}._lookup_by_id(session,'{unique_column_name}',{column_name_lower})
""")
                # now handle foreign key lookups, these are instance methods
                # if the foreign key is in the inbound, the other table will only have a single record
                for fk_name,relationship in table_data['single_lookups'].items():
                    other_table_name = relationship['other_table']
                    other_table_name_lower = other_table_name.lower()
                    local_cols = relationship['local_cols']
                    remote_cols = relationship['remote_cols']
                    
                    output_file.write(f"""
    def get_related_{other_table_name_lower}_for_{('_and_'.join(local_cols)).lower()}(self) -> {other_table_name}:
        column_values = dict(zip({remote_cols},[getattr(self,local_col) for local_col in {local_cols}]))
        return {other_table_name}.lookup_by_column_values(self._session,column_values,True)
""")
                # if the foreign key is in the outbound, the other table will have many records
                for fk_name,relationship in table_data['multi_lookups'].items():
                    other_table_name = relationship['other_table']
                    other_table_name_lower = other_table_name.lower()
                    other_table_name_plural = f"{other_table_name_lower}s" if not other_table_name_lower.endswith('s') else other_table_name_lower
                    local_cols = relationship['local_cols']
                    remote_cols = relationship['remote_cols']
                    
                    output_file.write(f"""
    def get_related_{other_table_name_plural}_for_{'_and_'.join(local_cols).lower()}_to_{'_and_'.join(remote_cols).lower()}(self) -> List[{other_table_name}]:
        column_values = dict(zip({remote_cols},[getattr(self,local_col) for local_col in {local_cols}]))
        return {other_table_name}.find_by_column_values(self._session,column_values)
""")
