#!/bin/sh
export LEAS_CAB_COSDB_ACCTKEY="..."
export LEAS_CAB_COSDB_DBNAME="..."
export LEAS_CAB_COSDB_ENDPOINT="https://....documents.azure.com:443/"
export LEAS_CAB_CAMPAIGN="BPMGM1"

./cos-cli  -cfg sample-001-cfg.yml
