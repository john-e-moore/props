import duckdb
import pandas as pd
from io import BytesIO
from utils.s3_utils import S3Utility

class DuckDBUtility:
    def __init__(self, db_path, s3_bucket_name, aws_access_key, aws_secret_key, aws_region):
        self.db_path = db_path
        self.s3_bucket_name = s3_bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        self.conn = duckdb.connect(database=self.db_path, read_only=False)

    def backup_to_s3(self, backup_file_name):
        with open(self.db_path, 'rb') as f:
            self.s3_client.upload_fileobj(f, self.s3_bucket_name, backup_file_name)
        print(f'Backup successful: {backup_file_name}')

    def restore_from_s3(self, backup_file_name):
        with open(self.db_path, 'wb') as f:
            self.s3_client.download_fileobj(self.s3_bucket_name, backup_file_name, f)
        self.conn = duckdb.connect(database=self.db_path, read_only=False)
        print(f'Restore successful: {backup_file_name}')

    def insert_data(self, table_name, data):
        if isinstance(data, pd.DataFrame):
            self.conn.register('temp_df', data)
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        else:
            raise ValueError('Data should be a pandas DataFrame')
        print(f'Data inserted into {table_name}')

    def upsert_data(self, table_name, data, key_columns):
        if isinstance(data, pd.DataFrame):
            temp_table = f"{table_name}_temp"
            self.conn.register('temp_df', data)
            self.conn.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM temp_df")
            key_condition = " AND ".join([f"{table_name}.{col} = {temp_table}.{col}" for col in key_columns])
            update_set = ", ".join([f"{table_name}.{col} = {temp_table}.{col}" for col in data.columns])
            insert_columns = ", ".join(data.columns)
            insert_values = ", ".join([f"{temp_table}.{col}" for col in data.columns])
            
            upsert_query = f"""
            BEGIN TRANSACTION;
            DELETE FROM {table_name} USING {temp_table} WHERE {key_condition};
            INSERT INTO {table_name} ({insert_columns}) SELECT {insert_values} FROM {temp_table};
            DROP TABLE {temp_table};
            COMMIT;
            """
            self.conn.execute(upsert_query)
        else:
            raise ValueError('Data should be a pandas DataFrame')
        print(f'Data upserted into {table_name}')

    def query(self, query):
        result = self.conn.execute(query).fetchdf()
        return result

    def __del__(self):
        self.conn.close()

