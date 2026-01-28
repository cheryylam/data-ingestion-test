from dagster import ConfigurableResource, Config
from dagster_dbt import DbtProject
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine
from requests import post
import random 
import string 
import openpyxl
import numpy as np
from typing import Literal


class PostgresResource(ConfigurableResource):
    host: str
    port: int = 5432
    username: str
    password: str
    database: str

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_engine(self):
        """Returns a SQLAlchemy Engine. Useful for pandas.to_sql."""
        return create_engine(self.connection_string)

    @contextmanager
    def get_connection(self):
        """Yields a raw SQLAlchemy connection for executing SQL queries."""
        engine = self.get_engine()
        with engine.connect() as conn:
            yield conn

    def write_table(self, df: pd.DataFrame, table_name: str, schema: str = 'raw', if_exists: str = "replace", index: bool = False, **kwargs):
        engine = self.get_engine()

        df["_timestamp"] = pd.Timestamp.now()
        df.to_sql(table_name, engine, schema = schema, if_exists=if_exists, index=index, **kwargs)

    def read_table(self, table_name: str) -> pd.DataFrame:
        engine = self.get_engine()
        
        df = pd.read_sql(table_name, engine)
        return df
    
    def read_excel(self, file_path: str, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            return df
        except Exception as e:
            raise Exception(f"Gagal membaca file Excel di {file_path}: {e}")

manifest = DbtProject(
        project_dir="dbt",
        profiles_dir="dbt",
        profile="postgre"
    ).manifest_path