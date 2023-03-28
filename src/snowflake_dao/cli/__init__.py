from pathlib import Path
import typer
import yaml
import os
from snowcli import config
from snowcli.config import AppConfig
from ..generator import ObjectsGenerator

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})

@app.command()
def generate(
            database: str = typer.Argument(...,
                help='Database'
            ),
            schema: str = typer.Argument(...,
                help='Schema'
            ),
            environment: str = typer.Option(
                'dev', '-e', '--environment',
                help='Name of environment (e.g. dev, prod, staging)'
            ),
            output_file: str = typer.Option(
                'snowflake_tables.py', '-o', '--output-file',
                help='Name of file to output'
            ),
            include_schema: bool = typer.Option(
                True, '-s', '--include-schema',
                help='Include the schema name in the Data Access Objects'
            ),
            ignore_table_regex: str = typer.Option(
                None, '-i', '--ignore-table-regex',
                help='A Regular Expression for tables to ignore'
            )
            ):
    """
    Generates Data Access Objects for a Snowflake schema
    """
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            f"The {environment} environment is not configured in app.toml "
            f"yet, please run `snow configure {environment}` first before continuing.",
        )
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        generator = ObjectsGenerator(snowflake_connection_parameters=config.snowflake_connection.ctx,
                                database=database,schema=schema, include_schema=include_schema,
                                ignore_table_regex=ignore_table_regex)
        generator.analyse()
        type_overrides = None
        exta_imports = None
        if os.path.exists('python_types.yaml'):
            with open('python_types.yaml', 'r', encoding='utf-8') as file:
                data = yaml.load(file, Loader=yaml.FullLoader)
                exta_imports = data['imports']
                type_overrides = data['type_overrides']
        generator.generate(output_file,exta_imports,type_overrides)