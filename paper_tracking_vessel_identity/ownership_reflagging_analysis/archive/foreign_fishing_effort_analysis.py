# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.3
#   kernelspec:
#     display_name: gfw-new
#     language: python
#     name: gfw-new
# ---

# +
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import matplotlib.gridspec as gridspec
from operator import add

import pyseas
pyseas._reload()
import pyseas.maps as psm
from pyseas.maps import bivariate

# Set plot parameters and styling
psm.use(psm.styles.chart_style)
plt.rcParams['figure.figsize'] = (10, 6)

# Show all dataframe rows
pd.set_option("max_rows", 20)
# -

# Parameters used for visualizations
color_dark_pink = '#d73b68'
color_gray = '#b2b2b2'
color_dark_blue = '#204280'
color_purple = '#742980'
color_light_pink = '#d73b68'
color_orange = '#ee6256'
color_light_orange = '#f8ba47'
color_yellow = '#ebe55d'

# ## Table version definitions

ownership_by_mmsi_table = 'world-fishing-827.scratch_jenn.ownership_by_mmsi_2012_2020_v20210601'
public_fishing_effort_table = 'global-fishing-watch.gfw_public_data.fishing_effort_byvessel_v2'
eez_info_table = 'world-fishing-827.gfw_research.eez_info'
vi_table = 'world-fishing-827.gfw_research.vi_ssvid_v20210301'
owner_table = 'world-fishing-827.vessel_identity.identity_owner_v20210601'


# # Fishing Effort Maps

# ### Fishing Effort by Foreign-Owned Vessels
#
# Fishing my EU owned and flagged vessels with EU EEZs are filtered out and considered domestic.
#
# Current list of EU EEZs: [ 'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE', 'GBR' ]

# +
# q = f'''
# WITH 

# ## Get the fishing vessels of interest (i.e. foreign owned, domestic owned, etc)
# ## Remove MMSI that have identities with overlapping time ranges
# fishing_identities_of_interest AS (
#     SELECT 
#     mmsi,
#     first_timestamp,
#     last_timestamp,
#     FROM `{ownership_by_mmsi_table}`
#     WHERE is_fishing
#     AND is_foreign
#     AND NOT timestamp_overlap
# ),

# ## Get the fishing activity for each mmsi and their time ranges
# ## The DISTINCT is crucial as it prevent duplication of fishing activity
# ## When multiple identities are attached to one MMSI and have
# ## overlapping time ranges.
# fishing_of_interest AS (
#     SELECT DISTINCT
#     b.*
#     FROM fishing_identities_of_interest a
#     LEFT JOIN `{public_fishing_effort_table}` b
#     ON a.mmsi = b.mmsi
#     AND b.date BETWEEN DATE(a.first_timestamp) AND DATE(a.last_timestamp)
#     WHERE b.fishing_hours IS NOT NULL
# )

# ## Group by grid cells
# SELECT
# cell_ll_lat * 10 as cell_ll_lat,
# cell_ll_lon * 10 as cell_ll_lon,
# SUM(fishing_hours) AS fishing_hours,
# FROM fishing_of_interest
# GROUP BY cell_ll_lat, cell_ll_lon
# '''

# df_public_fishing_effort_foreign = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
q = f'''CREATE TEMP FUNCTION flag_to_eu(flag ANY TYPE) AS ((
    SELECT IF(flag IN UNNEST([ 'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE', 'GBR' ]), 'EU', flag)
));

CREATE TEMP FUNCTION eez_to_eu(eez ANY TYPE) AS ((
    SELECT IF(eez IN UNNEST([ 'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE', 'GBR' ]), 'EU', eez)
));

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
        SELECT DISTINCT
        mmsi,
        a.vessel_record_id,
        a.n_shipname,
        a.n_callsign,
        a.flag,
        flag_to_eu(a.flag) as flag_eu,
        b.owner_flag,
        flag_to_eu(b.owner_flag) as owner_flag_eu,
        geartype,
        first_timestamp,
        last_timestamp,
        FROM `{ownership_by_mmsi_table}` a
        JOIN `{owner_table}` b
        ON a.mmsi = b.ssvid
        AND a.vessel_record_id = b.vessel_record_id
        AND a.n_shipname = b.n_shipname
        AND a.n_callsign = b.n_callsign
        AND a.flag = b.flag
        WHERE is_fishing
        AND is_foreign
        AND NOT timestamp_overlap
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
          eez_to_eu(udfs.eez_id_to_iso3 (eez)) as eez_iso3_eu,
          eez_id_to_name(eez) AS eez_name,
          IF((eez_to_eu(udfs.eez_id_to_iso3 (eez)) = 'EU') AND (flag_eu = 'EU') AND (owner_flag_eu = 'EU'), TRUE, FALSE) as is_inter_eu          
        FROM fishing_of_interest_by_mmsi 
        INNER JOIN (
          SELECT gridcode, eez
          FROM `pipe_static.regions`
          LEFT JOIN UNNEST(regions.eez) AS eez )
        USING (gridcode)
    ),

    fishing_of_interest_filter_inter_eu AS (
        SELECT *,
        FROM fishing_of_interest_by_mmsi_with_eez
        WHERE is_inter_eu = FALSE
    )

    ## Group by grid cells
    SELECT
    cell_ll_lat * 10 as cell_ll_lat,
    cell_ll_lon * 10 as cell_ll_lon,
    SUM(fishing_hours) AS fishing_hours,
    FROM fishing_of_interest_filter_inter_eu
    GROUP BY cell_ll_lat, cell_ll_lon
    '''

df_public_fishing_effort_foreign = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

# ### Fishing Effort by All Vessels with Unknown Ownership
#
# This includes matched vessels without known ownership flag AND all fishing effort in the public data set not attributed to a matched vessel.

# +
q = f'''
WITH 

    ## Get the fishing vessels of interest (i.e. foreign owned, domestic owned, etc)
    ## Remove MMSI that have identities with overlapping time ranges
    fishing_identities_to_remove AS (
        SELECT 
        mmsi,
        first_timestamp,
        last_timestamp,
        FROM `{ownership_by_mmsi_table}`
        WHERE is_fishing
        AND NOT is_unknown
        AND NOT timestamp_overlap
    ),

    ## Get the fishing activity for each mmsi and their time ranges
    ## The DISTINCT is crucial as it prevent duplication of fishing activity
    ## When multiple identities are attached to one MMSI and have
    ## overlapping time ranges.
    fishing_joined AS (
        SELECT DISTINCT
        a.*, b.*
        FROM `{public_fishing_effort_table}` a
        LEFT JOIN fishing_identities_to_remove b
        ON a.mmsi = b.mmsi
        AND a.date BETWEEN DATE(b.first_timestamp) AND DATE(b.last_timestamp)
        WHERE a.fishing_hours IS NOT NULL
    )

## Group by grid cells, excluding all cells that match a vessel with a
## known owner using `WHERE first_timestamp IS NULL`
SELECT
cell_ll_lat * 10 as cell_ll_lat,
cell_ll_lon * 10 as cell_ll_lon,
SUM(fishing_hours) AS fishing_hours,
FROM fishing_joined
WHERE first_timestamp IS NULL
GROUP BY cell_ll_lat, cell_ll_lon
'''

df_public_fishing_effort_unknown = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

# ### Total Fishing Effort in Public Data

# +
q = f'''
SELECT
cell_ll_lat * 10 as cell_ll_lat,
cell_ll_lon * 10 as cell_ll_lon,
SUM(fishing_hours) AS total_fishing_in_cell,
COUNT(mmsi) as num_mmsi,
FROM `{public_fishing_effort_table}`
GROUP BY cell_ll_lat, cell_ll_lon
'''

df_public_fishing_effort_total = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -



# ## Rasterize and calculate ratios

# +
grid_foreign = pyseas.maps.rasters.df2raster(df_public_fishing_effort_foreign, 'cell_ll_lon', 'cell_ll_lat', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_unknown = pyseas.maps.rasters.df2raster(df_public_fishing_effort_unknown, 'cell_ll_lon', 'cell_ll_lat', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_total = pyseas.maps.rasters.df2raster(df_public_fishing_effort_total, 'cell_ll_lon', 'cell_ll_lat', 
                                     'total_fishing_in_cell', xyscale=10, per_km2=True)

grid_foreign_ratio = np.divide(grid_foreign, grid_total, out=np.zeros_like(grid_foreign), where=grid_total!=0)
grid_unknown_ratio = np.divide(grid_unknown, grid_total, out=np.zeros_like(grid_unknown), where=grid_total!=0)
# -


cmap = bivariate.TransparencyBivariateColormap(pyseas.cm.misc.blue_orange)
with psm.context(psm.styles.dark):
    fig, (ax0, ax1) = psm.create_maps(2, 1, figsize=(15, 15), dpi=300, facecolor='white')

    norm1 = mpcolors.LogNorm(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.1, vmax=10, clip=True)
    
    ## Plot fishing by foreign-owned vessels
    bivariate.add_bivariate_raster(grid_foreign_ratio, grid_total, cmap, norm1, norm2, ax=ax0)
    psm.add_land(ax0)
    psm.add_eezs(ax0)
    cb_ax = bivariate.add_bivariate_colorbox(cmap, norm1, norm2,
                                     xlabel='hours fished by foreign-owned vessel\n(as fraction of total fishing hours)',
                                     ylabel='total hours fished\n(per $\mathregular{km^2}$)', fontsize=8,
                                     loc = (0.6, -0.17), aspect_ratio=3.0,
                                     xformat='{x:.2f}', yformat='{x:.2f}', fig=fig, ax=ax0)
    
    ## Plot fishing by vessels with unknown ownership
    bivariate.add_bivariate_raster(grid_unknown_ratio, grid_total, cmap, norm1, norm2, ax=ax1)
    psm.add_land(ax1)
    psm.add_eezs(ax1)
    cb_ax = bivariate.add_bivariate_colorbox(cmap, norm1, norm2,
                                     xlabel='hours fished by vessels with unknown ownership\n(as fraction of total fishing hours)',
                                     ylabel='total hours fished\n(per $\mathregular{km^2}$)', fontsize=8,
                                     loc = (0.6, -0.17), aspect_ratio=3.0,
                                     xformat='{x:.2f}', yformat='{x:.2f}', fig=fig, ax=ax1)
    ax0.set_title("Fishing Effort by Foreign-Owned Fishing Vessels (2012-2020)", 
                  fontsize=16)
    ax1.set_title("Fishing Effort by Fishing Vessels with Unknown Ownership (2012-2020)", 
                  fontsize=16)
    plt.subplots_adjust(hspace=.05)
    plt.tight_layout()
    plt.savefig('./figures/fishing_effort_foreign_unknown_prop.png', dpi=300, bbox_inches='tight')
    plt.show()



# # Exploring hotspots of fishing by foreign-owned vessels

def get_vessels_fishing_in_eezs(eez_list, ownership_type=None, fishing_min=24):

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
        FROM `{ownership_by_mmsi_table}`
        WHERE is_fishing
        AND is_foreign
        AND TRUE
        AND NOT timestamp_overlap
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
    LEFT JOIN `{owner_table}`
    USING(ssvid, vessel_record_id, n_shipname, n_callsign, flag)
    WHERE eez_iso3 IN UNNEST({eez_list})
    AND fishing_hours > {fishing_min}
    ORDER BY fishing_hours DESC
    '''

    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


def get_vessels_fishing_in_bbox(lon_start, lat_start, lon_end, lat_end, ownership_type=None, fishing_min=24):

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
    FROM `{owner_table}` a
    JOIN mmsi_in_bbox b
    USING (ssvid)
    LEFT JOIN `{ownership_by_mmsi_table}` c
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


# ## Indian Ocean

# ### Kenya EEZ (KEN)

foreign_fishing_KEN = get_vessels_fishing_in_eezs(['KEN'], 'is_foreign')

foreign_fishing_KEN[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_KEN)

# ### Seychelles EEZ (SYC)

foreign_fishing_SYC = get_vessels_fishing_in_eezs(['SYC'], 'is_foreign')

foreign_fishing_SYC[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_SYC)



# ### Northwest Indian Ocean Bbox
#
# *TODO: modify query to only pull high seas fishing within the bbox using `pipe_static.regions`?*

foreign_fishing_NWIO = get_vessels_fishing_in_bbox(38.2, -14.8, 81.7, 9.8, 'is_foreign')

foreign_fishing_NWIO[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_NWIO)

# ### Southwest Indian Ocean Bbox
#
# *TODO: modify query to only pull high seas fishing within the bbox using `pipe_static.regions`?*

foreign_fishing_SWIO = get_vessels_fishing_in_bbox(20.01, -45.0, 80, -21.79, 'is_foreign')

foreign_fishing_SWIO[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_SWIO)

# ## Pacific Ocean

# ### South Pacific
#
# *TODO: modify query to only pull high seas fishing within the bbox using `pipe_static.regions`?*

foreign_fishing_SPacific = get_vessels_fishing_in_bbox(124.9, -30.4, -81.2, 13.8, 'is_foreign')

foreign_fishing_SPacific[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_SPacific)

# ### North Pacific
#
# *TODO: modify query to only pull high seas fishing within the bbox using `pipe_static.regions`?*

foreign_fishing_NPacific = get_vessels_fishing_in_bbox(129.4, 24.7, -126.7, 45.1, 'is_foreign')

foreign_fishing_NPacific[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_NPacific)

# ## Argentina Area

# ### Argentina EEZ (ARG)

foreign_fishing_ARG = get_vessels_fishing_in_eezs(['ARG'], 'is_foreign')

foreign_fishing_ARG[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_ARG)

# ### Falkland/Malvinas Islands EEZ (FLK)

foreign_fishing_FLK = get_vessels_fishing_in_eezs(['FLK'], 'is_foreign')

foreign_fishing_FLK[0:20]

foreign_fishing_FLK[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_FLK)

# ### South Georgia/Sandwich Islands EEZ (SGS)

foreign_fishing_SGS = get_vessels_fishing_in_eezs(['SGS'], 'is_foreign')

foreign_fishing_SGS[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_SGS)


# ## West Africa

## Gets gridded fishing effort for non-overlapping MMSI with the specified ownership type and geartype.
## Default values are used to allow the user to not specify one or both of 
## the parameters, in which case the query will will not parse out effort by that attribute.
def get_fishing_effort_by_eez(ownership_type=None, geartype=None):
    
    if not ownership_type:
        ownership_check = 'TRUE'
    else:
        ownership_check = ownership_type

    if not geartype:
        geartype_check = 'TRUE'
    elif isinstance(geartype, str):
        geartype_check = f'geartype = "{geartype}"'
    elif isinstance(geartype, list):
        geartype_check = f'geartype IN UNNEST({geartype})'
    else:
        print("Invalid geartype")
        sys.exit()
    
    
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
        first_timestamp,
        last_timestamp,
        FROM `{ownership_by_mmsi_table}`
        WHERE is_fishing
        AND {ownership_check}
        AND {geartype_check}
        AND NOT timestamp_overlap
    ),

    ## Get the fishing activity for each mmsi and their time ranges
    ## The DISTINCT is crucial as it prevent duplication of fishing activity
    ## When multiple identities are attached to one MMSI and have
    ## overlapping time ranges.
    fishing_of_interest_by_mmsi AS (
        SELECT DISTINCT
        b.*,
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

    ## Group fishing effort for all EEZs
    fishing_effort_by_eez AS (
        SELECT 
        eez, eez_iso3, eez_name,
        SUM(fishing_hours) AS fishing_hours
        FROM fishing_of_interest_by_mmsi_with_eez
        GROUP BY eez, eez_iso3, eez_name
    )

    SELECT *
    FROM fishing_effort_by_eez
    ORDER BY fishing_hours DESC
    '''

    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

df_foreign_fishing_by_eez = get_fishing_effort_by_eez('is_foreign')
df_domestic_fishing_by_eez = get_fishing_effort_by_eez('is_domestic')

# Note: the column where eez is NULL is the high seas

df_fishing_by_eez = df_foreign_fishing_by_eez.merge(df_domestic_fishing_by_eez, on=['eez', 'eez_iso3'], suffixes=['_foreign', '_domestic'])
df_fishing_by_eez['prop_foreign_to_domestic'] = np.where(df_fishing_by_eez.fishing_hours_domestic == 0, 0, df_fishing_by_eez.fishing_hours_foreign/df_fishing_by_eez.fishing_hours_domestic)
df_fishing_by_eez


west_african_eezs = ['8369', '8371', '8370', '8471', '8472', '8390', '8391', '8473', '8400', \
                     '8392', '8393', '8474', '21797', '8398', '8475', '8397', '8398', '8476', \
                     '8394', '8478', '8477']
df_fishing_eez_wa = df_fishing_by_eez[df_foreign_fishing_by_eez.eez.isin(west_african_eezs)].reset_index(drop=True).sort_values('fishing_hours_foreign', ascending=False)

df_fishing_eez_wa.sort_values('fishing_hours_foreign', ascending=False)

df_fishing_eez_wa.sort_values('prop_foreign_to_domestic', ascending=False)

# ### Gabon EEZ (GAB)
#
# Third highest foreign fishing hours and second highest proportion of foreign to domestic fishing.

foreign_fishing_GAB = get_vessels_fishing_in_eezs(['GAB'], 'is_foreign')

foreign_fishing_GAB[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_GAB)





# ## All EEZs



df_fishing_by_eez[0:20].sort_values('fishing_hours_foreign', ascending=False)

df_fishing_by_eez[0:20].sort_values('prop_foreign_to_domestic', ascending=False)



# ## Northest Atlantic (Canada and Greeland)

foreign_fishing_NWA = get_vessels_fishing_in_bbox(-71.7,44.8,-20.6,72.8, 'is_foreign')

foreign_fishing_NWA[['flag', 'owner_flag']].value_counts()

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_NWA)


