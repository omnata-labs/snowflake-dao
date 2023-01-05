from pathlib import Path
import typer
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
            snowflake_tables: str = typer.Option(
                'snowflake_tables.py', '-o', '--output-file',
                help='Name of file to output'
            )):
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
                                database=database,schema=schema)
        generator.analyse()
        generator.generate(snowflake_tables)