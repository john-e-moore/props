import os
import duckdb
import pandas as pd
from typing import List
from utils.utils import load_config
from handlers.s3_handler import S3Handler

class DuckDBHandler:
    def __init__(self, db_path: str):
        """
        Initializes the DuckDBHandler with the path to the DuckDB database file and an instance of the S3Handler class.
        
        :param db_path: The file path for the DuckDB database.
        :param s3_handler: An instance of the S3Handler class for handling operations with S3.
        """
        # S3Handler needs a bucket
        environment = os.getenv('PROPS_ENVIRONMENT')
        environment_config = load_config(environment)
        s3_bucket = environment_config['aws']['s3_bucket']

        self.db_path = db_path
        self.s3_handler = S3Handler(s3_bucket)
        self.conn = duckdb.connect(database=self.db_path, read_only=False)
        print(f"DuckDBHandler initialized with database at {db_path}")

    def backup_to_s3(self, backup_file_name: str) -> None:
        """
        Backs up the DuckDB database file to an S3 bucket using the S3Handler.
        
        :param backup_file_name: The name of the file in the S3 bucket.
        """
        self.s3_handler.upload_file(self.db_path, backup_file_name)
        print(f"Database backed up to S3 as {backup_file_name}")

    def restore_from_s3(self, backup_file_name: str) -> None:
        """
        Restores the DuckDB database file from an S3 bucket using the S3Handler.
        
        :param backup_file_name: The name of the file in the S3 bucket.
        """
        self.s3_handler.download_file(self.db_path, backup_file_name)
        print(f"Database restored from S3 from {backup_file_name}")

    def insert_data(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Inserts data into a specified table from a pandas DataFrame.
        
        :param table_name: The name of the table to insert data into.
        :param data: pandas DataFrame containing the data to insert.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError('Data should be a pandas DataFrame')
        self.conn.register('temp_df', data)
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        print(f"Data inserted into {table_name}")

    def upsert_data(self, table_name: str, data: pd.DataFrame, key_columns: List[str]) -> None:
        """
        Upserts data into a specified table using a pandas DataFrame.
        
        :param table_name: The name of the table.
        :param data: pandas DataFrame containing the data.
        :param key_columns: List of columns that define the uniqueness constraint.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError('Data should be a pandas DataFrame')
        temp_table = f"{table_name}_temp"
        self.conn.register('temp_df', data)
        self.conn.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM temp_df")
        key_condition = " AND ".join([f"{table_name}.{col} = {temp_table}.{col}" for col in key_columns])
        insert_columns = ", ".join(data.columns)
        insert_values = ", ".join([f"{temp_table}.{col}" for col in data.columns])

        upsert_query = f"""
        BEGIN TRANSACTION;
        DELETE FROM {table_name} WHERE EXISTS (
            SELECT 1 FROM {temp_table} WHERE {key_condition}
        );
        INSERT INTO {table_name} ({insert_columns}) SELECT {insert_values} FROM {temp_table};
        DROP TABLE {temp_table};
        COMMIT;
        """
        self.conn.execute(upsert_query)
        print(f"Data upserted into {table_name}")

    def query(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query and returns a pandas DataFrame of the results.
        
        :param query: The SQL query string.
        :return: pandas DataFrame.
        """
        result = self.conn.execute(query).fetchdf()
        print(f"Query executed successfully")
        return result

    def __del__(self):
        """
        Ensure that the DuckDB connection is closed when the object is deleted.
        """
        self.conn.close()
        print(f"Connection to DuckDB closed")
