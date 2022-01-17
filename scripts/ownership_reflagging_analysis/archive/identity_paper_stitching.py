# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: gfw-rad
#     language: python
#     name: gfw-rad
# ---

# +
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
from jinja2 import Template
from google.cloud import bigquery

import pyseas
pyseas._reload()
import pyseas.maps as psm
from pyseas.maps import bivariate

# # Show all dataframe rows
pd.set_option("max_rows", 20)
# -

# # Setup

# + [markdown] tags=[]
# ## Table Versions
#
# All tables are passed in as parameters so that changing here changes everywhere.

# + tags=[]
from config import PROJECT, IDENTITY_TABLE
#PROJECT_PUBLIC, EEZ_INFO_TABLE, OWNER_TABLE, OWNERSHIP_BY_MMSI_TABLE, PUBLIC_FISHING_EFFORT_TABLE
# -


# # Overview

q = f'''
WITH 

vessel_record_id_multi AS (
    SELECT vessel_record_id,
    COUNT(*) num_ids,
    COUNTIF(imo IS NOT NULL) as num_valid_imo,
    COUNT(DISTINCT flag) as num_flags,
    FROM `{PROJECT}.{IDENTITY_TABLE}`
    WHERE multi_identity
    GROUP BY vessel_record_id
)

SELECT 
COUNT(*) as num_multi_id,
COUNTIF(num_valid_imo < num_ids) AS num_multi_id_missing_imo,
COUNTIF((num_valid_imo < num_ids) AND (num_flags > 1)) as num_multi_id_missing_imo_multi_flags
FROM vessel_record_id_multi
'''
# print(q)
df_multi_id_stats = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

print(f"{df_multi_id_stats.iloc[0].num_multi_id} vessel_record_id linked to multiple identities")
print(f"{df_multi_id_stats.iloc[0].num_multi_id_missing_imo} ({df_overview_stats.iloc[0].num_multi_id_missing_imo/df_overview_stats.iloc[0].num_multi_id*100:0.1f})% of those vessel_record_id have at least one identity missing IMO")
print(f"{df_multi_id_stats.iloc[0].num_multi_id_missing_imo_multi_flags} vessel_record_id have identities with missing IMO and with different flags")


# +
q = f'''
WITH 

vessel_record_id_missing_imo AS (
    SELECT vessel_record_id,
    COUNT(*) num_ids,
    COUNTIF(imo IS NOT NULL) as num_valid_imo,
    COUNT(DISTINCT ssvid) as num_ssvid,
    COUNT(DISTINCT n_shipname) as num_shipname,
    COUNT(DISTINCT n_callsign) as num_callsign,
    COUNT(DISTINCT flag) as num_flag,
    FROM `{PROJECT}.{IDENTITY_TABLE}`
    WHERE multi_identity
    GROUP BY vessel_record_id
    HAVING num_valid_imo < num_ids
)

SELECT 
COUNTIF(num_ssvid > 1) as multi_ssvid,
COUNTIF(num_shipname > 1) as mutli_shipname,
COUNTIF(num_callsign > 1) as mutli_callsign,
COUNTIF(num_flag > 1) as mutli_flag,
FROM vessel_record_id_missing_imo 
'''

df_missing_imo_stats = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_missing_imo_stats

# +
q = f'''
WITH 

vessel_record_id_missing_imo AS (
    SELECT vessel_record_id,
    COUNT(*) num_ids,
    COUNTIF(imo IS NOT NULL) as num_valid_imo,
    COUNT(DISTINCT ssvid) as num_ssvid,
    COUNT(DISTINCT n_shipname) as num_shipname,
    COUNT(DISTINCT n_callsign) as num_callsign,
    COUNT(DISTINCT flag) as num_flag,
    FROM `{PROJECT}.{IDENTITY_TABLE}`
    WHERE multi_identity
    GROUP BY vessel_record_id
    HAVING num_valid_imo = num_ids
)

SELECT
COUNT(*) as total,
COUNTIF(num_ssvid > 1) as multi_ssvid,
COUNTIF(num_shipname > 1) as mutli_shipname,
COUNTIF(num_callsign > 1) as mutli_callsign,
COUNTIF(num_flag > 1) as mutli_flag,
FROM vessel_record_id_missing_imo 
'''

df_connect_by_imo_stats = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_connect_by_imo_stats



# +
q = f'''
WITH 

vessel_record_id_multi AS (
    SELECT vessel_record_id,
    COUNT(*) num_ids,
    COUNTIF(imo IS NOT NULL) as num_valid_imo,
    FROM `{PROJECT}.{IDENTITY_TABLE}`
    WHERE multi_identity
    GROUP BY vessel_record_id
)

SELECT 
*
FROM `{PROJECT}.{IDENTITY_TABLE}`
WHERE vessel_record_id IN (
    SELECT vessel_record_id
    FROM vessel_record_id_multi
    WHERE num_valid_imo < num_ids
)
'''

df_missing_imo = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_missing_imo




