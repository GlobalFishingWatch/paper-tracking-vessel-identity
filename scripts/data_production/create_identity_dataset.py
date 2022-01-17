# ----------------------------------------------------------------------
# -- Create vessel identity dataset
# -- This script creates datasets on vessel identity
# -- (identity, ownership, authoriztion, and vessel info) to be released.
# --
# -- Run the following command (with date version as YYYYMMDD):
# -- `python create_identity_dataset.py YYYYMMDD`
# --
# -- Destination BQ bucket:
# -- `vessel_identity_staging` for staging datasets
# -- `vessel_identity` for final datasets
# -----------------------------------------------------------------------
import sys
import os
import pandas as pd
from subprocess import check_output
import json
from config import PROJECT, VESSEL_DATABASE, DATASET, STAGING, VESSEL_INFO, SINGLE_MMSI_MATCHED_VESSELS

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


desc = {
    "core": '"This table contains the core identity attributes, characteristics, and time ranges of their activity in AIS. Vessel record ID is GFW-generated temporary vessel ID to distinguish vessel hulls that may be associated with multiple vessel identities. The script to create this table is available at https://github.com/GlobalFishingWatch/vessel-identity/tree/create_identity_dataset/database-publication-Q1-2021/data_production"',
    "authorization": '"This table contains information about vessel authorization. The information about authorization is provided either by regional fisheries management organizations or national fishing authorities.  The authorization information is aggregated to generate authorized periods for a given vessel at a given region (RFMOs or national EEZs). To minimize possible reporting errors from registry, any gap of less than 3 months between authorization periods for the same vessels is accepted as potentially authorized. The script to create this table is available at https://github.com/GlobalFishingWatch/vessel-identity/tree/create_identity_dataset/database-publication-Q1-2021/data_production"',
    "owner": '"This table contains information about vessel ownership. We consider the owner information in the sources without specific indication to be a registered owner. Additionally, Carmine et al. (2020) conducted an extensive desktop research to manually determine likely beneficial owners of a few thousand vessels and made it publicly available. We placed a higher priority on the data from this research work and considered them to be a beneficial owner. The script to create this table is available at https://github.com/GlobalFishingWatch/vessel-identity/tree/create_identity_dataset/database-publication-Q1-2021/data_production"',
    "ais_activity": '"This table contains information about vessel characteristics from various sources including 1) the neural network model developed by GFW based on vessel movements in AIS, 2) the aggregated registries, and 3) values determined by GFW to be the most representative by combining all sources of information. In this table, vessel_class_registry may differ from geartype in the core identity table and some fishing vessels may not exist in the core identity table (see the AIS Activity Table Notes part of Limitation and caveat below). The script to create this table is available at https://github.com/GlobalFishingWatch/vessel-identity/tree/create_identity_dataset/database-publication-Q1-2021/data_production"'
}

def update_schema(cat, desc, YYYYMMDD):
    """
    Add schema descriptions for all fields to BigQuery using CSV

    :param cat: data table category
    :param desc: data table description
    :param YYYYMMDD: vessel identity data version
    :return: None
    """
    #
    # Read in field descriptions from CSV
    descriptions = pd.read_csv('{ROOT_DIR}/schema_for_identity_{CAT}.csv'
                               .format(ROOT_DIR=ROOT_DIR, CAT=cat), index_col='field')

    #
    # Get blank schema (no descriptions) from BigQuery
    command = "bq show --format=json {PROJECT}:{DATASET}.identity_{CAT}_v{YYYYMMDD}"\
        .format(PROJECT=PROJECT,
                DATASET=DATASET,
                CAT=cat,
                YYYYMMDD=YYYYMMDD)
    out = check_output(command.split(" "))
    j = json.loads(out)

    #
    # Get schema fields
    schema_fields = j['schema']['fields']

    #
    # Update descriptions for schema. STRUCTS are nested and need additional loop.
    for f in schema_fields:
        if f['type'] != 'RECORD':
            d = descriptions.loc[[f['name']]]['description'][0]
            f['description'] = d
        else:
            #
            # Loop through field names in the same group of STRUCT
            for a in f['fields']:
                group = descriptions[descriptions['group'] == f['name']]
                a['description'] = group.loc[[a['name']]]['description'][0]

    # load this new schema into Big Query
    with open('temp.json', 'w') as f:
        f.write(json.dumps(schema_fields))
    command = "bq update {DATASET}.identity_{CAT}_v{YYYYMMDD} temp.json"\
        .format(DATASET=DATASET,
                CAT=cat,
                YYYYMMDD=YYYYMMDD)

    assert os.system(command) == 0, "Query failed"
    os.remove("./temp.json")

    #
    # Update the table description
    command = "bq update --description {desc} {DATASET}.identity_{CAT}_v{YYYYMMDD}"\
        .format(desc=desc,
                DATASET=DATASET,
                CAT=cat,
                YYYYMMDD=YYYYMMDD)
    assert os.system(command) == 0, "Query failed"

    print('Schema and description for {DATASET}.identity_{CAT}_v{YYYYMMDD} updated on BigQuery'
          .format(DATASET=DATASET,
                  CAT=cat,
                  YYYYMMDD=YYYYMMDD))


def run_queries (YYYYMMDD):
    """
    This module runs all the query scripts in the directory
    to generate UDFs under `world-fishing-827.udf`

    :return: None
    """

    #
    # Read all SQL file names in the directory
    sqlfiles = [
        # Identity table
        "staging_identity_core_base.sql.j2",
        "staging_identity_core_list_uvi.sql.j2",
        "staging_identity_core_vessel_record_id.sql.j2",
        "staging_identity_core_timestamp_overlap.sql.j2",
        "create_identity_core.sql.j2",
        # Owner table
        "create_identity_owner.sql.j2",
        # Authorization table
        "create_identity_authorization.sql.j2",
        # Vessel info table
        "create_identity_ais_activity.sql.j2"
    ]

    for sf in sqlfiles:
        print(f"{sf} is now being executed...")

        #
        # Check if dataset exists, otherwise create dataset with version date
        # command = f"""bq ls vessel_identity || bq mk vessel_identity"""
        # os.system(command)

        #
        # NEW SCRIPT TO RUN QUERIES VIA JINJA2 TEMPLATE
        # Staging part
        if "staging" in sf:
            command = f"""jinja2 {sf} \
            -D VESSEL_DATABASE={VESSEL_DATABASE}\
            -D PROJECT={PROJECT}\
            -D STAGING={STAGING}\
            -D YYYYMMDD={YYYYMMDD}\
            -D VESSEL_INFO={VESSEL_INFO}\
            | bq query \
            --destination_table={PROJECT}:{STAGING}.{sf.split('.sql.j2')[0].split('staging_')[1]}_v{YYYYMMDD} \
            --replace --use_legacy_sql=false"""
            assert os.system(command) == 0, "Query failed"

        # Vessel Identity bucket part
        else:
            command = f"""jinja2 {sf} \
            -D VESSEL_DATABASE={VESSEL_DATABASE}\
            -D PROJECT={PROJECT}\
            -D DATASET={DATASET}\
            -D STAGING={STAGING}\
            -D YYYYMMDD={YYYYMMDD}\
            -D VESSEL_INFO={VESSEL_INFO}\
            -D SINGLE_MMSI_MATCHED_VESSELS={SINGLE_MMSI_MATCHED_VESSELS}\
            | bq query \
            --destination_table={PROJECT}:{DATASET}.{sf.split('.sql.j2')[0].split('create_')[1]}_v{YYYYMMDD} \
            --replace --use_legacy_sql=false"""
            assert os.system(command) == 0, "Query failed"

    for cat in ['core', 'authorization', 'owner', 'ais_activity']:
        update_schema(cat, desc[cat], YYYYMMDD)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Use example: python create_identity_dataset.py")
        raise ValueError('Version date is supposed to be passed as "YYYYMMDD"')

    #
    # Setting
    YYYYMMDD = sys.argv[1]

    #
    # Run
    run_queries(YYYYMMDD)
    print("\nAll queries executed.\n")
