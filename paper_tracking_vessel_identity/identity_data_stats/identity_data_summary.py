#-------------------------------------------------------------
#-- Create a dataset that summarizes the identity core data
#-- This script creates data tables for the analysis of identity
#-- data by flag and by year, and in-depth Chinese DWF analysis
#--
#-- Run the following command (with date version as YYYYMMDD):
#-- `python identity_data_summary.py YYYYMMDD`
#--
#-- Destination BQ bucket:
#-- `vessel_identity_staging` for staging datasets
#-- `vessel_identity` for final datasets
#-------------------------------------------------------------
import sys
from builtins import len, ValueError

import os
from config import PROJECT, DATASET, STAGING, VESSEL_INFO_TABLE, \
    VESSEL_INFO_BYYEAR_TABLE, VESSEL_DATABASE, IDENTITY_CORE_DATA, \
    FISHING_EFFORT_TABLE, FISHING_SEGMENT_TABLE, EEZ_INFO

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def run_queries (YYYYMMDD):
    """
    This module runs all the query scripts in the directory
    to generate data tables to `vessel_identity` bucket

    :return: None
    """

    #
    # Read all SQL file names in the directory
    sqlfiles = [
        "create_identity_data_summary_allyears.sql.j2",
        "create_identity_data_summary_byyear.sql.j2",
        "create_identity_data_summary_chinese_dwf.sql.j2"
    ]

    for sf in sqlfiles:
        print(f"{sf} is now being executed...")

        #
        # NEW SCRIPT TO RUN QUERIES VIA JINJA2 TEMPLATE
        command = f"""jinja2 {sf} \
            -D PROJECT={PROJECT}\
            -D DATASET={DATASET}\
            -D STAGING={STAGING}\
            -D YYYYMMDD={YYYYMMDD}\
            -D VESSEL_DATABASE={VESSEL_DATABASE}\
            -D IDENTITY_CORE_DATA={IDENTITY_CORE_DATA}\
            -D VESSEL_INFO_TABLE={VESSEL_INFO_TABLE}\
            -D VESSEL_INFO_BYYEAR_TABLE={VESSEL_INFO_BYYEAR_TABLE}\
            -D FISHING_EFFORT_TABLE={FISHING_EFFORT_TABLE}\
            -D FISHING_SEGMENT_TABLE={FISHING_SEGMENT_TABLE}\
            -D EEZ_INFO={EEZ_INFO}\
            -D END_DATE=2022-01-01\
            | bq query \
            --destination_table={PROJECT}:{DATASET}.{sf.split('.sql.j2')[0].split('create_')[1]}_v{YYYYMMDD} \
            --replace --use_legacy_sql=false"""
        assert os.system(command) == 0, "Query failed"


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Use example: python identity_data_summary.py YYYYMMDD")
        raise ValueError('Version date is supposed to be passed as "YYYYMMDD"')

    #
    # Setting
    YYYYMMDD = sys.argv[1]

    #
    # Run
    run_queries(YYYYMMDD)
    print("\nAll queries executed.\n")
