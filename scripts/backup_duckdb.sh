#!/bin/bash

aws s3 cp $PROPS_DEV_DUCKDB_LOCATION $PROPS_S3_DESTINATION_DEV_DUCKDB_BACKUP
echo DuckDB file backed up to $PROPS_S3_DESTINATION_DEV_DUCKDB_BACKUP