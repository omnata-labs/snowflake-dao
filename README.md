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
python3 -m build
pip install dist/snowflake_dao-0.0.1-py3-none-any.whl --force-reinstall
```

# To run
```
snowflake_dao SFC_SAMPLES_SAMPLE_DATA TPCDS_SF100TCL
```