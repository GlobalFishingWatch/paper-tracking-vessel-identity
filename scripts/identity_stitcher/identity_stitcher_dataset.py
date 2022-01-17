#-------------------------------------------------------------
#-- Create a dataset derived from identity stitcher
#-- This script creates ports of identity changes
#-- and ship building/scrapping information
#--
#-- Run the following command (with date version as YYYYMMDD):
#-- `python identity_stitcher_dataset.py YYYYMMDD`
#--
#-- Destination BQ bucket:
#-- `vessel_identity_staging` for staging datasets
#-- `vessel_identity` for final datasets
#-------------------------------------------------------------
import sys
import os
from config import PROJECT, DATASET, STAGING, IDENTITY_STITCHER_CORE, IDENTITY_CORE_DATA, PIPELINE_DATA

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def run_queries (YYYYMMDD):
    """
    This module runs all the query scripts in the directory
    to generate data tables with regard to vessel reflagging `vessel_identity` bucket

    :return: None
    """

    #
    # Read all SQL file names in the directory
    sqlfiles = [
        "staging_identity_stitcher_core.sql.j2", ## Be careful, this query consumes 2 TB and takes about 1 hr to complete
        "create_identity_change_ports.sql.j2"
    ]

    for sf in sqlfiles:
        print(f"{sf} is now being executed...")

        #
        # NEW SCRIPT TO RUN QUERIES VIA JINJA2 TEMPLATE

        if sf == "create_identity_change_ports.sql.j2":
            #
            # Core reflagging dataset
            category_map = {
                "all": 'TRUE',
                "fishing": '"is_fishing AND pair_is_fishing"',
                "support": '"is_support AND pair_is_support"'
            }

            for cat in category_map.keys():
                command = f"""jinja2 {sf} \
                    -D PROJECT={PROJECT}\
                    -D DATASET={DATASET}\
                    -D STAGING={STAGING}\
                    -D YYYYMMDD={YYYYMMDD}\
                    -D IDENTITY_STITCHER_CORE={IDENTITY_STITCHER_CORE}\
                    -D CATEGORY={category_map[cat]}\
                    -D END_DATE={YYYYMMDD[0:4] + "-" + YYYYMMDD[4:6] + "-" + YYYYMMDD[6:8]}\
                    -D START_DATE=2012-01-01 \
                    | bq query \
                    --destination_table={PROJECT}:{DATASET}.{sf.split('.sql.j2')[0].split('create_')[1]}_{cat}_v{YYYYMMDD} \
                    --replace --use_legacy_sql=false"""
                assert os.system(command) == 0, "Query failed"

        elif "staging" in sf:
            command = f"""jinja2 {sf} \
                -D PROJECT={PROJECT}\
                -D DATASET={DATASET}\
                -D YYYYMMDD={YYYYMMDD}\
                -D IDENTITY_CORE_DATA={IDENTITY_CORE_DATA}\
                -D PIPELINE_DATA={PIPELINE_DATA}\
                -D END_DATE={YYYYMMDD[0:4] + "-" + YYYYMMDD[4:6] + "-" + YYYYMMDD[6:8]}\
                -D START_DATE=2012-01-01 \
                | bq query \
                --destination_table={PROJECT}:{STAGING}.{sf.split('.sql.j2')[0].split('staging_')[1]}_v{YYYYMMDD} \
                --replace --use_legacy_sql=false"""
            assert os.system(command) == 0, "Query failed"

        else:
            command = f"""jinja2 {sf} \
                -D PROJECT={PROJECT}\
                -D DATASET={DATASET}\
                -D STAGING={STAGING}\
                -D YYYYMMDD={YYYYMMDD}\
                -D IDENTITY_STITCHER_CORE={IDENTITY_STITCHER_CORE}\
                -D END_DATE={YYYYMMDD[0:4] + "-" + YYYYMMDD[4:6] + "-" + YYYYMMDD[6:8]}\
                -D START_DATE=2012-01-01 \
                | bq query \
                --destination_table={PROJECT}:{DATASET}.{sf.split('.sql.j2')[0].split('create_')[1]}_v{YYYYMMDD} \
                --replace --use_legacy_sql=false"""
            assert os.system(command) == 0, "Query failed"


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Use example: python identity_stitcher_dataset.py YYYYMMDD")
        raise ValueError('Version date is supposed to be passed as "YYYYMMDD"')

    #
    # Setting
    YYYYMMDD = sys.argv[1]

    #
    # Run
    run_queries(YYYYMMDD)
    print("\nAll queries executed.\n")
