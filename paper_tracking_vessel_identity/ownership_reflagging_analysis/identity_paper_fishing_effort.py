# -*- coding: utf-8 -*-
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
#     display_name: rad
#     language: python
#     name: rad
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
# ## Query Parameters
#
# All tables are passed in as parameters so that changing here changes everywhere.

# + tags=[]
from config import PROJECT, PROJECT_PUBLIC, VERSION, EEZ_INFO_TABLE, IDENTITY_TABLE, OWNER_TABLE, OWNERSHIP_BY_MMSI_TABLE, PUBLIC_FISHING_EFFORT_TABLE

# Set raster resolution.
DEGREE = 1
# -


# ## Figures folder setup
#
# If folder not already available, create it.

# +
figures_folder = f'./figures_v{VERSION}/'

if not os.path.exists(figures_folder):
    os.makedirs(figures_folder)
# -

# ## Fishing Effort Maps

# + [markdown] tags=[]
# ### Transform ownership information to be able to be referenced "by MMSI"
#
# Saved in the table defined by `ownership_by_mmsi_table` for reference.
# Should eventually be moved out of scratch and to a permanent project dataset in BQ.
#
# This query gets ownership information at the MMSI level rather than the identity level.
# This is necessary because fishing effort data is generated at the MMSI level.
# This is complicated because identities are more specific than MMSI. Also, multiple
# identities may use the same MMSI during overlapping periods and these MMSI are marked as
# such so that they can be removed when pulling fishing effort for speific ownership types.

# +
# Set to True if you'd like to run the query and save the table (overwriting if it already exists)
# Set to False if you'd like to just pull the information from an existing table
run_ownership_by_mmsi = True

# If this table needs to be run (or rerun), do that and save/overwrite to BQ.
if run_ownership_by_mmsi:
    # Open ownership_by_mmsi.sql.j2 file
    with open('queries/ownership_by_mmsi.sql.j2') as f:
        sql_template = Template(f.read())

    # Format the query according to the desired features
    q = sql_template.render(
        PROJECT=PROJECT,
        VERSION=VERSION,
        EEZ_INFO_TABLE=EEZ_INFO_TABLE,
        IDENTITY_TABLE=IDENTITY_TABLE,
        OWNER_TABLE=OWNER_TABLE
    )
    
    ### Uncomment print statement is query desired
#     print(q)

    ### Save the table to `scratch_jenn` in BigQuery to be used by the fishing effort queries
    # Construct a BigQuery client object.
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=f'{PROJECT}.{OWNERSHIP_BY_MMSI_TABLE}{VERSION}',
                                         write_disposition='WRITE_TRUNCATE')

    # Start the query, passing in the extra configuration.
    query_job = client.query(q, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print(f"Query results loaded to the table {OWNERSHIP_BY_MMSI_TABLE}{VERSION}")

# +
# Now pull that table into a dataframe.
q = f'''
SELECT *
FROM {OWNERSHIP_BY_MMSI_TABLE}{VERSION}
'''

df_ownership_by_mmsi = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_ownership_by_mmsi

# + [markdown] tags=[]
# ### Get gridded fishing effort for all identities with foreign ownership

# +
with open('queries/public_fishing_effort_foreign.sql.j2') as f:
    sql_template = Template(f.read())
    
q = sql_template.render(
    PROJECT=PROJECT,
    PROJECT_PUBLIC=PROJECT_PUBLIC,
    VERSION=VERSION,
    OWNERSHIP_BY_MMSI_TABLE=OWNERSHIP_BY_MMSI_TABLE,
    PUBLIC_FISHING_EFFORT_TABLE=PUBLIC_FISHING_EFFORT_TABLE,
    DEGREE=DEGREE,
)

# Uncomment `print(q)` to get the query being run for QA purposes
# print(q)
df_public_fishing_effort_foreign = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

# ### Get gridded fishing effort for all identities with unknown ownership
#
# This includes matched vessels without known ownership flag AND all fishing
# effort in the public data set not attributed to a matched vessel.

# +
with open('queries/public_fishing_effort_unknown.sql.j2') as f:
    sql_template = Template(f.read())
    
q = sql_template.render(
    PROJECT=PROJECT,
    PROJECT_PUBLIC=PROJECT_PUBLIC,
    VERSION=VERSION,
    OWNERSHIP_BY_MMSI_TABLE=OWNERSHIP_BY_MMSI_TABLE,
    PUBLIC_FISHING_EFFORT_TABLE=PUBLIC_FISHING_EFFORT_TABLE,
    DEGREE=DEGREE,
)

# Uncomment `print(q)` to get the query being run for QA purposes
# print(q)
df_public_fishing_effort_unknown = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

# ### Get total gridded fishing effort in public data
#
# Needed to calulate the ratio of foreign and unknown fishing to total fishing
# IN ADDITION TO being the y-axis in the bivariate map. Thus why it was queried 
# separately and ratio was not calculated in the previous fishing effort queries 
# for identities with foreign or unknown ownership.

# +
with open('queries/public_fishing_effort_total.sql.j2') as f:
    sql_template = Template(f.read())
    
q = sql_template.render(
    PROJECT=PROJECT,
    PROJECT_PUBLIC=PROJECT_PUBLIC,
    PUBLIC_FISHING_EFFORT_TABLE=PUBLIC_FISHING_EFFORT_TABLE,
    DEGREE=DEGREE,
)

# Uncomment `print(q)` to get the query being run for QA purposes
# print(q)
df_public_fishing_effort_total = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# + [markdown] tags=[]
# ### Rasterize and calculate ratios

# +
grid_foreign = psm.rasters.df2raster(df_public_fishing_effort_foreign, 'cell_ll_lon', 'cell_ll_lat', 
                                     'fishing_hours', xyscale=1/DEGREE, per_km2=True)
grid_unknown = psm.rasters.df2raster(df_public_fishing_effort_unknown, 'cell_ll_lon', 'cell_ll_lat', 
                                     'fishing_hours', xyscale=1/DEGREE, per_km2=True)
grid_total = psm.rasters.df2raster(df_public_fishing_effort_total, 'cell_ll_lon', 'cell_ll_lat', 
                                     'total_fishing_in_cell', xyscale=1/DEGREE, per_km2=True)

grid_foreign_ratio = np.divide(grid_foreign, grid_total, out=np.zeros_like(grid_foreign), where=grid_total!=0)
grid_unknown_ratio = np.divide(grid_unknown, grid_total, out=np.zeros_like(grid_unknown), where=grid_total!=0)

# -

# ### Produce figure for each of two bivariate maps: foreign-owned and unknown ownership

cmap = bivariate.TransparencyBivariateColormap(pyseas.cm.misc.blue_orange)
with psm.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='#f7f7f7')

    norm1 = mpcolors.LogNorm(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.1, vmax=3, clip=True)
    
    ## Plot fishing by foreign-owned vessels
    bivariate.add_bivariate_raster(grid_foreign_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    cb_ax = bivariate.add_bivariate_colorbox(cmap, norm1, norm2,
                                     xlabel='hours fished by\nforeign-owned vessels\n(as fraction of total fishing hours)',
                                     ylabel='total hours\nfished\n(per $\mathregular{km^2}$)', fontsize=24,
                                     loc = (0.65, -0.23), aspect_ratio=3.0,
                                     xformat='{x:.2f}', yformat='{x:.2f}', fig=fig, ax=ax)
    cb_ax.tick_params(labelsize=18)
    cb_ax.set_yticks([0.1, 1.0, 3.0])
    
    ax1 = ax.inset_axes([0.38, -0.32, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=24,
             s="Ratio of Fishing Hours\nby Foreign-Owned Vessels\n"
             "to Those by All Vessels Broadcasting AIS\n"
             "at One Degree Resolution")
        
    ax.set_title("Fishing Effort by Foreign-Owned Fishing Vessels (2012-2020)", 
                  fontsize=30, pad=15)
    plt.tight_layout()
    plt.savefig(f'{figures_folder}/fishing_effort_bivariate_foreign_only.png', dpi=300, bbox_inches='tight')
    plt.show()

cmap = bivariate.TransparencyBivariateColormap(pyseas.cm.misc.blue_orange)
with psm.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='white')

    norm1 = mpcolors.LogNorm(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.1, vmax=10, clip=True)
    
    ## Plot fishing by foreign-owned vessels
    bivariate.add_bivariate_raster(grid_unknown_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    cb_ax = bivariate.add_bivariate_colorbox(cmap, norm1, norm2,
                                     xlabel='hours fished\nby vessels with unknown ownership\n(as fraction of total fishing hours)',
                                     ylabel='total hours\nfished\n(per $\mathregular{km^2}$)', fontsize=24,
                                     loc = (0.65, -0.23), aspect_ratio=3.0,
                                     xformat='{x:.2f}', yformat='{x:.2f}', fig=fig, ax=ax)
    cb_ax.tick_params(labelsize=18)
    
    ax1 = ax.inset_axes([0.38, -0.32, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=24,
             s="Ratio of Fishing Hours\nby Vessels with Unknown Ownership\n"
             "to Those by All Vessels Broadcasting AIS\n"
             "at One Degree Resolution")
    
    
    ax.set_title("Fishing Effort by Fishing Vessels with Unknown Ownership (2012-2020)", 
                  fontsize=30, pad=15)
    plt.tight_layout()
    plt.savefig(f'{figures_folder}/fishing_effort_bivariate_unknown_only.png', dpi=300, bbox_inches='tight')
    plt.show()


# ### Produce figure with two bivariate maps: foreign-owned and unknown ownership

# +
# OLD CODE - would need to be revamped before use to have the 
# same layout, fontsizes, etc of the single figures above
#

# cmap = bivariate.TransparencyBivariateColormap(pyseas.cm.misc.blue_orange)
# with psm.context(psm.styles.dark):
#     fig, (ax0, ax1) = psm.create_maps(2, 1, figsize=(15, 15), dpi=300, facecolor='white')

#     norm1 = mpcolors.LogNorm(vmin=0.01, vmax=1.0, clip=True)
#     norm2 = mpcolors.LogNorm(vmin=0.1, vmax=10, clip=True)
    
#     ## Plot fishing by foreign-owned vessels
#     bivariate.add_bivariate_raster(grid_foreign_ratio, grid_total, cmap, norm1, norm2, ax=ax0)
#     psm.add_land(ax0)
#     cb_ax = bivariate.add_bivariate_colorbox(cmap, norm1, norm2,
#                                      xlabel='hours fished by foreign-owned vessel\n(as fraction of total fishing hours)',
#                                      ylabel='total hours fished\n(per $\mathregular{km^2}$)', fontsize=10,
#                                      loc = (0.6, -0.17), aspect_ratio=3.0,
#                                      xformat='{x:.2f}', yformat='{x:.2f}', fig=fig, ax=ax0)
    
#     ## Plot fishing by vessels with unknown ownership
#     bivariate.add_bivariate_raster(grid_unknown_ratio, grid_total, cmap, norm1, norm2, ax=ax1)
#     psm.add_land(ax1)
#     cb_ax = bivariate.add_bivariate_colorbox(cmap, norm1, norm2,
#                                      xlabel='hours fished by vessels with unknown ownership\n(as fraction of total fishing hours)',
#                                      ylabel='total hours fished\n(per $\mathregular{km^2}$)', fontsize=10,
#                                      loc = (0.6, -0.17), aspect_ratio=3.0,
#                                      xformat='{x:.2f}', yformat='{x:.2f}', fig=fig, ax=ax1)
#     ax0.set_title("Fishing Effort by Foreign-Owned Fishing Vessels (2012-2020)", 
#                   fontsize=16)
#     ax1.set_title("Fishing Effort by Fishing Vessels with Unknown Ownership (2012-2020)", 
#                   fontsize=16)
#     plt.subplots_adjust(hspace=.05)
#     plt.tight_layout()
#     plt.savefig(figures_folder + 'fishing_effort_bivariate_foreign_unknown.png', dpi=300, bbox_inches='tight')
#     plt.show()
# -

# ## Exploring hotspots of fishing by foreign-owned vessels

def get_vessels_fishing_in_eezs(eez_list, eez_info_table, version, owner_table, ownership_by_mmsi_table, public_fishing_effort_table, ownership_type=None, fishing_min=24):

    if not isinstance(eez_list, list):
        print("ERROR: must give eezs as a list")
        return

    if not ownership_type:
        ownership_check = 'TRUE'
    else:
        ownership_check = ownership_type

    q = f'''
    CREATE TEMP FUNCTION eez_id_to_name(_eez_id ANY TYPE) AS ((
      WITH
        mapped AS (
          SELECT ARRAY_AGG (DISTINCT territory1 IGNORE NULLS) AS name_agg
          FROM `{eez_info_table}`
          WHERE SAFE_CAST (_eez_id AS INT64) = eez_id
        )

      SELECT
        CASE
          WHEN ARRAY_LENGTH (name_agg) > 1
            THEN ERROR ("Multiple names match to the given EEZ ID.")
          WHEN ARRAY_LENGTH (name_agg) = 1
            THEN name_agg[OFFSET(0)]
          ELSE NULL
        END
      FROM mapped
    ));

    WITH 

    ## Get the fishing vessels of interest (i.e. foreign owned, domestic owned, etc)
    ## Remove MMSI that have identities with overlapping time ranges
    fishing_identities_of_interest AS (
        SELECT 
        mmsi,
        vessel_record_id,
        n_shipname,
        n_callsign,
        flag,
        geartype,
        first_timestamp,
        last_timestamp,
        FROM `{ownership_by_mmsi_table}{version}`
        WHERE is_fishing
        AND is_foreign
        AND TRUE
        AND NOT overlapping_identities_for_mmsi
    ),

    ## Get the fishing activity for each mmsi and their time ranges
    ## The DISTINCT is crucial as it prevent duplication of fishing activity
    ## When multiple identities are attached to one MMSI and have
    ## overlapping time ranges.
    fishing_of_interest_by_mmsi AS (
        SELECT DISTINCT
        a.*, b.* EXCEPT (mmsi),
        FORMAT ("lon:%+07.2f_lat:%+07.2f", 
            ROUND (cell_ll_lon/0.01)*0.01, 
            ROUND (cell_ll_lat/0.01)*0.01) AS gridcode,
        FROM fishing_identities_of_interest a
        LEFT JOIN `{public_fishing_effort_table}` b
        ON a.mmsi = b.mmsi
        AND b.date BETWEEN DATE(a.first_timestamp) AND DATE(a.last_timestamp)
        WHERE b.fishing_hours IS NOT NULL
    ),

    fishing_of_interest_by_mmsi_with_eez AS (
        SELECT 
          *, 
          udfs.eez_id_to_iso3 (eez) AS eez_iso3,
          eez_id_to_name(eez) AS eez_name,
        FROM fishing_of_interest_by_mmsi 
        INNER JOIN (
          SELECT gridcode, eez
          FROM `pipe_static.regions`
          LEFT JOIN UNNEST(regions.eez) AS eez )
        USING (gridcode)
    ),

    ## Group fishing effort for all identities in each EEZ
    fishing_effort_by_eez AS (
        SELECT 
        mmsi AS ssvid, vessel_record_id, n_shipname, n_callsign, flag, geartype,
        eez, eez_iso3, eez_name,
        SUM(fishing_hours) AS fishing_hours
        FROM fishing_of_interest_by_mmsi_with_eez
        GROUP BY mmsi, vessel_record_id, n_shipname, n_callsign, flag, geartype, eez, eez_iso3, eez_name
    )

    SELECT *
    FROM fishing_effort_by_eez
    LEFT JOIN `{owner_table}{version}`
    USING(ssvid, vessel_record_id, n_shipname, n_callsign, flag)
    WHERE eez_iso3 IN UNNEST({eez_list})
    AND fishing_hours > {fishing_min}
    ORDER BY fishing_hours DESC
    '''

    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


def get_vessels_fishing_in_bbox(lon_start, lat_start, lon_end, lat_end, version, owner_table, ownership_by_mmsi_table, ownership_type=None, fishing_min=24):

    ## Handle date line crossings for longitude
    if lon_start > lon_end:
        ## Then we're crossing the date line and we have to query a little differently.
        lon_search = f'((cell_ll_lon BETWEEN {lon_start} AND 180) OR (cell_ll_lon BETWEEN 0 AND {lon_end}))'
    else:
        lon_search = f'cell_ll_lon BETWEEN {lon_start} AND {lon_end}'
        
    
    if not ownership_type:
        ownership_check = 'TRUE'
    else:
        ownership_check = ownership_type
        
    q = f'''
    WITH 

    mmsi_in_bbox AS (
        SELECT
        mmsi as ssvid,
        SUM(fishing_hours) as fishing_hours
        FROM `global-fishing-watch.gfw_public_data.fishing_effort_byvessel_v2`
        -- lon_start, lat_start, lon_end, lat_end
        -- EXAMPLE
        -- -26.07,-17.92,14.61,9.48
        -- WHERE cell_ll_lat BETWEEN -17.92 AND 9.48
        -- AND cell_ll_lon BETWEEN -26.07 AND 14.61
        WHERE cell_ll_lat BETWEEN {lat_start} AND {lat_end}
        AND {lon_search}
        GROUP BY mmsi
        HAVING fishing_hours > {fishing_min}
    )

    SELECT a.*, b.fishing_hours, c.* EXCEPT(vessel_record_id, mmsi, n_shipname, n_callsign, flag)
    FROM `{owner_table}{version}` a
    JOIN mmsi_in_bbox b
    USING (ssvid)
    LEFT JOIN `{ownership_by_mmsi_table}{version}` c
    ON a.ssvid = c.mmsi
    AND a.vessel_record_id = c.vessel_record_id
    AND a.n_shipname = c.n_shipname
    AND a.n_callsign = c.n_callsign
    AND a.flag = c.flag
    WHERE {ownership_check}
    AND (is_fishing OR is_fishing IS NULL)
    AND fishing_hours > {fishing_min}
    ORDER BY fishing_hours DESC
    '''

    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# Parameters used for visualizations
color_dark_pink = '#d73b68'
color_gray = '#b2b2b2'
color_dark_blue = '#204280'
color_purple = '#742980'
color_light_pink = '#d73b68'
color_orange = '#ee6256'
color_light_orange = '#f8ba47'
color_yellow = '#ebe55d'


def plot_stats_for_vessels(df):
    
    fig, ((ax0, ax1, ax2), (ax3, ax4, ax5)) = plt.subplots(2, 3, figsize=(15, 10))

    count_flag = df.flag.value_counts()
    count_flag.plot.bar(ax=ax0, color=color_dark_blue)
    ax0.set_title("Registered flags")
    ax0.set_ylabel("Number of identities")

    count_owner_flag = df.owner_flag.value_counts()
    count_owner_flag.plot.bar(ax=ax1, color=color_dark_blue)
    ax1.set_title("Owner flags")
    
    count_geartype = df.geartype.value_counts()
    count_geartype.plot.bar(ax=ax2, color=color_dark_blue)
    ax2.set_title("Geartype")
    
    df.groupby('flag').apply(lambda x: sum(x.fishing_hours)).reindex(count_flag.index).plot.bar(ax=ax3, color=color_dark_pink)
    ax3.set_ylabel("Fishing hours")

    df.groupby('owner_flag').apply(lambda x: sum(x.fishing_hours)).reindex(count_owner_flag.index).plot.bar(ax=ax4, color=color_dark_pink)
    
    df.groupby('geartype').apply(lambda x: sum(x.fishing_hours)).reindex(count_geartype.index).plot.bar(ax=ax5, color=color_dark_pink)

    plt.tight_layout()
    plt.show()
    return(fig, (ax0, ax1, ax2))


# + [markdown] tags=[]
# ## South Pacific
# -

foreign_fishing_SPacific = get_vessels_fishing_in_bbox(124.9, -30.4, -81.2, 13.8, VERSION, OWNER_TABLE, OWNERSHIP_BY_MMSI_TABLE, ownership_type='is_foreign')

foreign_fishing_SPacific[['flag', 'owner_flag', 'geartype']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_SPacific)

# ## Northwest Indian Ocean

# + [markdown] tags=[]
# ### Kenya and Seychelles EEZs (KEN and SYC)
# -

foreign_fishing_KEN_SYC = get_vessels_fishing_in_eezs(['KEN', 'SYC'], EEZ_INFO_TABLE, VERSION, OWNER_TABLE, OWNERSHIP_BY_MMSI_TABLE, f"{PROJECT_PUBLIC}.{PUBLIC_FISHING_EFFORT_TABLE}", 'is_foreign')


# + tags=[]
foreign_fishing_KEN_SYC[['flag', 'owner_flag', 'geartype']].value_counts()
# -

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_KEN_SYC)

# ### Full Region (by bounding box)

foreign_fishing_NWIO = foreign_fishing_IO = get_vessels_fishing_in_bbox(38.2, -14.8, 81.7, 12.13, VERSION, OWNER_TABLE, OWNERSHIP_BY_MMSI_TABLE, ownership_type='is_foreign')

foreign_fishing_NWIO[['flag', 'owner_flag', 'geartype']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_NWIO)

# ## Argentina EEZ

foreign_fishing_ARG = get_vessels_fishing_in_eezs(['ARG'], EEZ_INFO_TABLE, VERSION, OWNER_TABLE, OWNERSHIP_BY_MMSI_TABLE, f"{PROJECT_PUBLIC}.{PUBLIC_FISHING_EFFORT_TABLE}", 'is_foreign')

foreign_fishing_ARG[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_ARG)

# ## Falkland/Malvinas Islands EEZ (FLK)

foreign_fishing_FLK = get_vessels_fishing_in_eezs(['FLK'], EEZ_INFO_TABLE, VERSION, OWNER_TABLE, OWNERSHIP_BY_MMSI_TABLE, f"{PROJECT_PUBLIC}.{PUBLIC_FISHING_EFFORT_TABLE}", 'is_foreign')

foreign_fishing_FLK[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_FLK)

# + [markdown] tags=[]
# ## West Africa EEZs
# -

WA_eez = ['SEN', 'GMB', 'GNB', 'SLE', 'LIB', 'CIV', 'GHA', 'TGO', 'BEN', 'NGA', 'CMR', 'GNQ', 'GAB', 'STP', 'COG', 'COD', 'AGO']
foreign_fishing_WA_EEZ = get_vessels_fishing_in_eezs(WA_eez, EEZ_INFO_TABLE, VERSION, OWNER_TABLE, OWNERSHIP_BY_MMSI_TABLE, f"{PROJECT_PUBLIC}.{PUBLIC_FISHING_EFFORT_TABLE}", 'is_foreign')


foreign_fishing_WA_EEZ.owner_flag.value_counts()

foreign_fishing_WA_EEZ[foreign_fishing_WA_EEZ.owner_flag == 'ESP'][['flag', 'owner_flag', 'geartype']].value_counts()

foreign_fishing_WA_EEZ[foreign_fishing_WA_EEZ.owner_flag == 'ESP'][['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_WA_EEZ)

# # Supplementary Materials

print(f"About two-thirds of the identities in our dataset have at least one owner with a known flag state.")
print(df_ownership_by_mmsi[~df_ownership_by_mmsi.is_unknown].shape[0]/df_ownership_by_mmsi.shape[0])

unknown_fishing_hours = df_public_fishing_effort_unknown.fishing_hours.sum()
total_fishing_hours = df_public_fishing_effort_total.total_fishing_in_cell.sum()
known_fishing_hours = total_fishing_hours - unknown_fishing_hours
print(f"These vessels account for {known_fishing_hours/total_fishing_hours*100:0.2f}% of total fishing activity since 2012.")


# +
# Open prop_single_owner_flag_identities.sql.j2 file
with open('queries/util/prop_single_owner_flag_identities.sql.j2') as f:
    sql_template = Template(f.read())
    
# Format the query according to the desired features
q = sql_template.render(
    PROJECT=PROJECT,
    VERSION=VERSION,
    EEZ_INFO_TABLE=EEZ_INFO_TABLE,
    IDENTITY_TABLE=IDENTITY_TABLE,
    OWNER_TABLE=OWNER_TABLE
)

# Uncomment `print(q)` to get the query being run for QA purposes
# print(q)
df_prop_single_owner_flag = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
print(f"Identities can be associated with multiple owners due to discrepancies between registries, but this is extremely limited with the majority of the identities ({df_prop_single_owner_flag.iloc[0].prop_single_owner_flag*100:0.2f}%) were still associated with only one owner flag.")

# -

print(f"Only {df_ownership_by_mmsi[df_ownership_by_mmsi.is_foreign_and_domestic].shape[0]} identities had a combination of foreign and domestic owners, and were classified as foreign as the non-domestic aspect of the ownership is the variable of interest.")


# + [markdown] tags=[]
# ## Push rendered notebook to `rendered` repo


# +
# import rendered
# rendered.publish_to_github(f'./identity_paper_fishing_effort.ipynb', 
#                            f'vessel-identity-paper-v{VERSION}/', action='push')
# -


