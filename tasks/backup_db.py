import boto3
import os

def backup_duckdb_to_s3(db_path, bucket_name, s3_path):
    s3 = boto3.client('s3')
    s3.upload_file(db_path, bucket_name, s3_path)

if __name__ == "__main__":
    db_path = 'data/warehouse/warehouse.duckdb'
    bucket_name = 'your-s3-bucket'
    s3_path = 'backups/warehouse_backup.duckdb'
    
    backup_duckdb_to_s3(db_path, bucket_name, s3_path)
