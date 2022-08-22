#-------------------------------------------------------------
#-- Create vessel reflagging dataset
#-- This script creates datasets on reflagging vessels
#--
#-- Run the following command (with date version as YYYYMMDD):
#-- `python create_reflagging_dataset.py YYYYMMDD`
#--
#-- Destination BQ bucket:
#-- `vessel_identity_staging` for staging datasets
#-- `vessel_identity` for final datasets
#-------------------------------------------------------------
import sys
import os
import re
from config import PROJECT, DATASET, STAGING, IDENTITY_CORE_DATA, \
    REFLAGGING_CORE_ALL, REFLAGGING_CORE_FISHING, REFLAGGING_CORE_SUPPORT, ALL_FLAGGING_SUPPORT


def j2_command (sf, table_name, YYYYMMDD, category=None):
    """
    This module returns a command that runs the corresponding Jinja2 file.

    :param sf: String, the sql file name
    :param category: String, if there's any parameter about category to pass
    :return: String, j2 command
    """

    if (category == category) and (category is not None):
        param_cat = f"-D CATEGORY={category}"
    else:
        param_cat = ""

    command = f"""jinja2 {sf} \
        -D PROJECT={PROJECT}\
        -D DATASET={DATASET}\
        -D STAGING={STAGING}\
        -D YYYYMMDD={YYYYMMDD}\
        -D IDENTITY_CORE_DATA={IDENTITY_CORE_DATA}\
        -D REFLAGGING_CORE_ALL={REFLAGGING_CORE_ALL}\
        -D REFLAGGING_CORE_FISHING={REFLAGGING_CORE_FISHING}\
        -D REFLAGGING_CORE_SUPPORT={REFLAGGING_CORE_SUPPORT}\
        -D ALL_FLAGGING_SUPPORT={ALL_FLAGGING_SUPPORT}\
        {param_cat}\
        -D END_DATE=2022-01-01\
        | bq query \
        --destination_table={table_name} \
        --replace --use_legacy_sql=false"""

    return command


def run_queries (YYYYMMDD):
    """
    This module runs all the query scripts in the directory
    to generate data tables with regard to vessel reflagging `vessel_identity` bucket

    :return: None
    """

    #
    # Read all SQL file names in the directory
    sqlfiles = [
        "create_reflagging_core.sql.j2",
        "create_reflagging_flag_in_out.sql.j2",
        "create_reflagging_history_map_top15.sql.j2",
        "staging_all_flagging_support.sql.j2",
        "create_all_flagging_support.sql.j2",
        "create_reflagging_counts_byyear.sql.j2"
    ]

    for sf in sqlfiles:
        print(f"{sf} is now being executed...")

        #
        # NEW SCRIPT TO RUN QUERIES VIA JINJA2 TEMPLATE
        if sf == "create_reflagging_core.sql.j2":
            #
            # Core reflagging dataset
            category_map = {
                "all": '"is_fishing OR is_carrier OR is_bunker"',
                "fishing": "is_fishing",
                "support": '"is_carrier OR is_bunker"'
            }

            for cat in category_map.keys():
                table_name = f"{PROJECT}:{DATASET}." + \
                             re.sub(r"(.sql.j2|create_)", "", sf) + \
                             f"_{cat}_v{YYYYMMDD}"
                command = j2_command(sf, table_name, YYYYMMDD, category_map[cat])
                assert os.system(command) == 0, "Query failed"
                print(f"{table_name} is now available...\n")
        #
        # Reflagging history map data and byyear plot data
        elif sf in ("create_reflagging_history_map_top15.sql.j2",
                    "create_reflagging_counts_byyear.sql.j2"):
            category_map = {
                "fishing": REFLAGGING_CORE_FISHING,
                "support": REFLAGGING_CORE_SUPPORT
            }

            for cat in category_map.keys():
                table_name = f"{PROJECT}:{DATASET}." + \
                             re.sub(r"(.sql.j2|create_)", "", sf) + \
                             f"_{cat}_v{YYYYMMDD}"
                command = j2_command(sf, table_name, YYYYMMDD, category_map[cat])
                assert os.system(command) == 0, "Query failed"
                print(f"{table_name} is now available...\n")
        #
        # All support figure staging data
        elif "staging" in sf:
            table_name = f"{PROJECT}:{STAGING}." + \
                         re.sub(r"(.sql.j2|staging_)", "", sf) + \
                         f"_v{YYYYMMDD}"
            command = j2_command(sf, table_name, YYYYMMDD)
            assert os.system(command) == 0, "Query failed"
            print(f"{table_name} is now available...\n")

        else:

            table_name = f"{PROJECT}:{DATASET}." + \
                         re.sub(r"(.sql.j2|create_)", "", sf) + \
                         f"_v{YYYYMMDD}"
            command = j2_command(sf, table_name, YYYYMMDD)
            assert os.system(command) == 0, "Query failed"
            print(f"{table_name} is now available...\n")


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Use example: python create_reflagging_dataset.py YYYYMMDD")
        raise ValueError("Incorrect number of parameters")

    #
    # Setting
    YYYYMMDD = sys.argv[1]
    if len(YYYYMMDD) != 8:
        raise ValueError("Version date is supposed to be passed as YYYYMMDD")

    #
    # Run
    run_queries(YYYYMMDD)
    print("\nAll queries executed.\n")
