#!/bin/bash

aws s3 cp configs/base_config.yaml $PROPS_S3_DESTINATION_BASE_CONFIG
echo Base config backed up to $PROPS_S3_DESTINATION_BASE_CONFIG

aws s3 cp configs/dev_config.yaml $PROPS_S3_DESTINATION_DEV_CONFIG
echo Dev config backed up to $PROPS_S3_DESTINATION_DEV_CONFIG

aws s3 cp configs/prod_config.yaml $PROPS_S3_DESTINATION_PROD_CONFIG
echo Prod config backed up to $PROPS_S3_DESTINATION_PROD_CONFIG