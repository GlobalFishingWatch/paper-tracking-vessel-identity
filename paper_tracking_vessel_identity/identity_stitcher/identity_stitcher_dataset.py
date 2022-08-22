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
from config import PROJECT, DATASET, STAGING, IDENTITY_STITCHER_CORE, IDENTITY_CORE_DATA, PIPELINE_DATA, VESSEL_INFO, IDENTITY_STITCHER_CORE_FILTERED

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
        "staging_identity_stitcher_core_filtered.sql.j2",
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
                "support": '"(is_carrier AND pair_is_carrier) OR (is_bunker AND pair_is_bunker)"'
            }

            for cat in category_map.keys():
                command = f"""jinja2 {sf} \
                    -D PROJECT={PROJECT}\
                    -D DATASET={DATASET}\
                    -D STAGING={STAGING}\
                    -D YYYYMMDD={YYYYMMDD}\
                    -D IDENTITY_STITCHER_CORE={IDENTITY_STITCHER_CORE}\
                    -D IDENTITY_STITCHER_CORE_FILTERED={IDENTITY_STITCHER_CORE_FILTERED}\
                    -D CATEGORY={category_map[cat]}\
                    -D END_DATE=2022-01-01 \
                    -D START_DATE=2012-01-01 \
                    | bq query \
                    --destination_table={PROJECT}:{DATASET}.{sf.split('.sql.j2')[0].split('create_')[1]}_{cat}_v{YYYYMMDD} \
                    --replace --use_legacy_sql=false"""
                assert os.system(command) == 0, "Query failed"

        elif "staging" in sf:
            command = f"""jinja2 {sf} \
                -D PROJECT={PROJECT}\
                -D DATASET={DATASET}\
                -D STAGING={STAGING}\
                -D YYYYMMDD={YYYYMMDD}\
                -D IDENTITY_CORE_DATA={IDENTITY_CORE_DATA}\
                -D IDENTITY_STITCHER_CORE={IDENTITY_STITCHER_CORE}\
                -D VESSEL_INFO={VESSEL_INFO}\
                -D PIPELINE_DATA={PIPELINE_DATA}\
                -D END_DATE=2022-01-01 \
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
                -D END_DATE=2022-01-01 \
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
