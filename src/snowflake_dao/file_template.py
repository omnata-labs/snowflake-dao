"""
This file was generated by snowflake-dao and should not be directly modified.
"""
# pylint: disable=line-too-long,invalid-name
from __future__ import annotations
from abc import ABC
from snowflake.snowpark import Column,Table,Session
from snowflake.snowpark.functions import col, lit, when_matched, iff, to_timestamp,parse_json,to_varchar,object_construct,when_not_matched,sql_expr
from typing import Any, Dict, List,Optional, Tuple
import typing
import json
from dataclasses import dataclass,field,InitVar

class Sequence():
    def __init__(self,sequence_name:str):
        self.sequence_name = sequence_name

    def nextval(self,session):
        return session.sql(f"select {self.sequence_name}.nextval").collect()[0].NEXTVAL

class SnowflakeTable(ABC):
    _table_name:str = None
    _primary_key_column:str = None

    def __getstate__(self):
       """Used for serializing instances"""
       
       # start with a copy so we don't accidentally modify the object state
       # or cause other conflicts
       return {k:v for k,v in self.__dict__.items() if not k.startswith('_')}.copy()
    
    # This breaks Streamlit pickling :(
    #def __reduce__(self):
    #    return self.__getstate__()

    def __setstate__(self, state):
        """Used for deserializing"""
        # restore the state which was picklable
        self.__dict__.update(state)
    
    def __getitem__(self, item):
        """Makes the object subscriptable"""
        return getattr(self, item)

    def __init__(self,session:Session):
        self._session = session

    def __repr__(self):
        """
        Returns a printable string representation of the fields in this object
        """
        return str({k:v for k,v in self.__dict__.items() if not k.startswith('_')})
    
    def to_json(self):
        """
        Returns a JSON representation of the fields in this object
        """
        return json.dumps({k:v for k,v in self.__dict__.items() if not k.startswith('_')})
    
    def snowpark_table(self) -> Table:
        """
        Returns a Snowpark Table object for the underlying table
        """
        return self._session.table(self._table_name)
    
    def primary_key_match_clause(self) -> Column:
        """
        Generates a Snowpark matching clause for this record, useful for incorporating into other queries
        like updates
        """
        if self._primary_key_column is None:
            raise ValueError(f"Can't generate a match clause as the {self._table_name} table does not have a primary key defined")
        # Note that we shouldn't need to do type processing on the value as that would have occurred during instantiation
        return (col(self._primary_key_column)==lit(getattr(self,self._primary_key_column)))

    def _create_object(self,session):
        if self._primary_key_column is None:
            raise ValueError("Can't create objects unless a primary key is defined")
        state = self.__getstate__()
        params = {x:state[x] for x in state if x not in ['cls','table_class','session']}
        for k,v in params.items():
            if isinstance(v,Sequence):
                params[k] = v.nextval(session)
        target_table = session.table(self._table_name)
        dummy_df = session.create_dataframe([1], schema=["col_a"])
        # snowpark doesn't have an insert abstraction, so we use an unmatchable merge
        target_table.merge(dummy_df,lit(1)==lit(0),[when_not_matched().insert(params)])
        # we want literal values to populate the object with, but the kwargs
        # may contain snowpark expressions like lits and cols.
        # So we do a lookup by ID value to fetch the record back
        # Note that if the primary key value came from a sequence, it will have been resolved above
        return self._lookup_by_id(session,self._primary_key_column,params[self._primary_key_column])
    
    @classmethod
    def _lookup_by_id(cls,session,id_column:str,id_value:any) -> SnowflakeTable:
        """
        Fetches a record via its primary key value. Automatically does type conversion to combat ints-as-floats etc
        """
        type_hint = typing.get_type_hints(cls.__init__)[id_column]
        results = session.table(cls._table_name).where((col(id_column)==lit(cls._process_type(type_hint,id_column,id_value)))).collect()
        if len(results) == 0:
            raise ValueError(f'No {cls._table_name} found with {id_column} {id_value}')
        if len(results) > 1:
            raise ValueError(f'Too many results for {cls._table_name} with {id_column} {id_value} ({len(results)})')
        return cls._process_types(session,results[0].as_dict())
        
    @classmethod
    def _process_type(cls,column_type_hint:Any,column_name:str,column_value:Any):
        # properties without Optional typings are not allowed to be None
        if column_value is None:
            if typing.get_origin(column_type_hint)!=typing.Union or type(None) not in typing.get_args(column_type_hint):
                raise ValueError(f"A null value was found in column {column_name}, but it was marked as NOT NULL at the time of DAO generation")
            return None
        else:
            actual_type = column_type_hint
            # if it's an optional type but the value isn't null, we'll find the actual type to cast with
            if typing.get_origin(column_type_hint)==typing.Union:
                actual_types = [t for t in typing.get_args(column_type_hint) if t!=type(None)]
                if len(actual_types)!=1:
                    raise ValueError(f"Can't determine the real type of column {column_name} based on its type hints ({column_type_hint})")
                actual_type = actual_types[0]
            if actual_type==int:
                return int(column_value)
            elif actual_type == typing.Dict or actual_type == typing.List:
                return json.loads(column_value) if column_value else None # Python connector returns strings for variant columns
            return column_value

    @classmethod
    def _process_types(cls,session,values):
        type_hints = typing.get_type_hints(cls.__init__)
        for k,v in values.items():
            values[k] = cls._process_type(type_hints[k],k,v)
        instance = cls(**values)
        # Normally we could pass the session in and mark it as an InitVar, but that doesn't
        # work with get_type_hints (https://bugs.python.org/issue44799)
        instance._session=session
        return instance

    @classmethod
    def lookup_by_column_values(cls,session:Session,values:Dict[str,any],error_on_no_result:bool=False,query_stream:Optional[str]=None):
        """
        Retrieves a single record by providing a list of column->value pairs, matched on equality.
        Provide a value for query_stream to instead query a stream created for the table.
        """
        non_null_values = {k:v for k,v in values.items() if v}
        if len(non_null_values)==0:
            # no point doing a lookup if no actual values were provided
            return None
        results = cls.find_by_column_values(session,values,query_stream)
        if len(results) == 0:
            if error_on_no_result:
                raise ValueError(f"No matching {cls} record was found")
            return None
        if len(results) > 1:
            raise ValueError(f"Multiple matching {cls} records were found")
        return results[0]

    @classmethod
    def find_by_column_values(cls,session:Session,values:Dict[str,any],query_stream:Optional[str]=None):
        """
        Retrieves a list of records by providing a list of column->value pairs, matched on equality.
        Provide a value for query_stream to instead query a stream created for the table.
        """
        non_null_values = {k:v for k,v in values.items() if v}
        if len(non_null_values)==0:
            # no point doing a lookup if no actual values were provided
            return []
        if len(non_null_values) != len(values):
            raise ValueError("Performing a lookup is not supported when some of the foreign key values are null")
        where_clauses = (lit(1)==lit(1))
        type_hints = typing.get_type_hints(cls.__init__)
        for column_name,column_value in values.items():
            where_clauses = where_clauses & (col(column_name)==lit(cls._process_type(type_hints[column_name],column_name,column_value)))
        return_values:List[cls]=[]
        table = cls._table_name if query_stream is None else query_stream
        query = session.table(table).where(where_clauses)
        if query_stream is not None:
            query = query.drop("METADATA$ISUPDATE","METADATA$ACTION","METADATA$ROW_ID")
        results = query.collect()
        for result in results:
            return_values.append(cls._process_types(session,result.as_dict()))
        return return_values
