# Props

This is the respository for sports ELT jobs. The first application will be to project fantasy output from vegas prop markets.

Everything will run on a small EC2 server using S3 for data lake and db backup. Duckdb will be used for data warehousing and will be backed up to S3 periodically. Jobs will be run and observed with Prefect. Dev environment will be local.

A pipeline is a DAG with each node being a job.

Example pipeline: 
- GET request from API
- Stage raw data in S3
- Refine a time-partitioned batch of data and stage in processed folder in S3
- Insert processed data into DuckDB
- Daily (?) write the DuckDB database file to S3 for backup

## Note
My flow was failing with 'permission denied' when the .duckdb file was connected to DBeaver. I guess DBeaver places a lock on it.

