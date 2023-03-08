# snowflake-dao
A Data Access Objects generator for Snowflake.

This is a highly experimental prototype in its earliest stages of development.


## Background and motivation
Snowpark provides a flexible Python abstraction over Snowflake, enabling a variety of data engineering/analytics workloads over any set of tables.

When building data applications on Snowflake, it will become more common for a set of pre-defined tables to hold application data and for more simpler transactional queries to be needed.

When working this way, you really want:
1) Less Snowpark boilerplate for things like fetching records by primary key
2) Intellisense/Pylance to be fully enabled for things like related record lookups

## Why not just use SqlAlchemy?
I may yet still, especially when version 2 is released if the typing support is good.

Right now I just wanted something simple and self-contained.

# To install
Right now there's no PyPi package, so:
```
pip install build
python3 -m build
pip install dist/snowflake_dao-0.0.1-py3-none-any.whl --force-reinstall
```

# To run
```
snowflake_dao SFC_SAMPLES_SAMPLE_DATA TPCDS_SF10TCL
```

# To use
After running the above command, you can do stuff like this:
```
import getpass
import snowflake_tables

snowflake_connection_parameters = {
    'user':'me',
    'account':'aa123456.ap-southeast-2',
    'warehouse':'COMPUTE_WH',
    'schema':'TPCDS_SF100TCL',
    'database':'SFC_SAMPLES_SAMPLE_DATA',
    'role':'SYSADMIN',
    'password':getpass.getpass() # This should prompt for a password
}
from snowflake.snowpark import Session
session = Session.builder.configs(snowflake_connection_parameters).create()

cc_record = snowflake_tables.CALL_CENTER.lookup_by_cc_call_center_sk(session,2)
print(cc_record)
date_dim = cc_record.get_related_date_dim_for_cc_open_date_sk()
print(date_dim)
date_dim.get_related_call_centers_for_d_date_sk_to_cc_open_date_sk()
```

You can also create a file named `python_types.yaml` and use it to override the python class for a particular column.
This is intended to be used with OBJECT/ARRAY columns to designate pydantic classes extended from BaseModel. In doing this, you can enforce a schema inside semi-structured columns.

The file should look like this:
```
imports: |
  from my_module import MyClass
type_overrides:
  TABLE_1:
    COLUMN_A: MyClass
```