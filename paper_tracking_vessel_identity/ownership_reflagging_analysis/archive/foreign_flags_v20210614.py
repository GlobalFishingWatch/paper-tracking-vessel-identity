# -*- coding: utf-8 -*-
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
#     display_name: Python 3
#     language: python
#     name: python3
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

# # Visualization code

# Parameters used for visualizations
color_dark_pink = '#d73b68'
color_gray = '#b2b2b2'
color_dark_blue = '#204280'
color_purple = '#742980'
color_light_pink = '#d73b68'
color_orange = '#ee6256'
color_light_orange = '#f8ba47'
color_yellow = '#ebe55d'

# # Get the data

# ## Vessel flag and owner flag
#
# This query counts distinct combinations of identity (minus SSVID) and owner flag.
# Identities are deduplicated across SSVID because the same identity, owner, and owner flag combo is often associated with multiple SSVID across time which add unmeaningful duplicates.
#
# Should we consider collapsing this down further to the hull level?
# Then it would be unique combinations of vessel_record_id, flag, and owner_flag...
# It's hard to know how to count things for ownership.
#
# Ownership metrics are calculated based on if an identity is flagged to the same flag state (domestic), a different flag state (foreign), or if the owners flags state is not known (unknown). If an identity has both foreign and domestic owners, it is labeled `foreign_and_domestic` but is then considered as foreign in `ids_foreign_combined` and `prop_foreign_combined` which are used for analysis. This is because there are only 14 remaining identities as `foreign_and_domestic` after an extensive manual ownership review of those originally labeled as such.
#
# #### NOTE: There are 17 rows removed by `WHERE flag != 'null'` in `identities_joined`. Do we want to include then in some way?

identity_table = 'world-fishing-827.vessel_identity.identity_core_v20210601'
owner_table = 'world-fishing-827.vessel_identity.identity_owner_v20210601'
eez_info_table = 'world-fishing-827.gfw_research.eez_info'


# +
### This query counts distinct combinations of identity (minus SSVID), owner, and owner flag.
### Identities are deduplicated across SSVID because the same identity, owner, and owner flag combo
### is often associated with multiple SSVID across time which add unmeaningful duplicates.
q = f'''
WITH 

territory_flag_mapping AS (
    SELECT DISTINCT
    territory1_iso3, sovereign1_iso3
    FROM `{eez_info_table}`
    WHERE eez_type = '200NM'
    AND territory1_iso3 != sovereign1_iso3
),

all_identities AS (
    SELECT 
    vessel_record_id,
    ssvid,
    IFNULL(n_shipname, 'null') as n_shipname,
    IFNULL(n_callsign, 'null') as n_callsign,
    IFNULL(flag, 'null') as flag,
    FROM `{identity_table}`
),

identities_with_ownership AS (
    SELECT DISTINCT
    vessel_record_id,
    ssvid,
    IFNULL(n_shipname, 'null') as n_shipname,
    IFNULL(n_callsign, 'null') as n_callsign,
    IFNULL(flag, 'null') as flag,
    owner_flag
    FROM `{owner_table}`
),

identities_joined AS (
    SELECT
    vessel_record_id,
    ssvid,
    n_shipname,
    n_callsign,
    flag,
    owner_flag,
    IFNULL(sovereign1_iso3, owner_flag) as owner_flag_sovereign,
    FROM all_identities 
    LEFT JOIN identities_with_ownership
    USING(vessel_record_id, ssvid, n_shipname, n_callsign, flag)
    LEFT JOIN territory_flag_mapping
    ON owner_flag = territory1_iso3
    WHERE flag != 'null'
    ORDER BY n_shipname, n_callsign, ssvid, flag, owner_flag
),

identities_final AS (
    SELECT DISTINCT
    vessel_record_id, n_shipname, n_callsign, flag, 
    owner_flag, owner_flag_sovereign,
    IF(owner_flag_sovereign IS NOT NULL AND flag NOT IN (owner_flag_sovereign, owner_flag), 1, 0) as is_foreign,
    IF(owner_flag_sovereign IS NOT NULL AND flag IN (owner_flag_sovereign, owner_flag), 1, 0) as is_domestic,
    IF(owner_flag_sovereign IS NULL, 1, 0) as is_unknown,
    FROM identities_joined
),

identities_classified AS (
SELECT 
vessel_record_id, n_shipname, n_callsign, flag,
# Ignore unknown identities when vessels have other known ownerships
IF(SUM(is_foreign) > 0 AND SUM(is_domestic) = 0, TRUE, FALSE) as is_foreign,
IF(SUM(is_foreign) = 0 AND SUM(is_domestic) > 0, TRUE, FALSE) as is_domestic,
IF(SUM(is_foreign) > 0 AND SUM(is_domestic) > 0, TRUE, FALSE) as is_foreign_and_domestic,
IF(SUM(is_foreign) = 0 AND SUM(is_domestic) = 0 AND SUM(is_unknown) > 0, TRUE, FALSE) as is_unknown,
COUNT(*) AS num_owners
FROM identities_final
GROUP BY vessel_record_id, n_shipname, n_callsign, flag
)

SELECT 
flag,
SUM(IF(is_foreign, 1, 0)) as ids_foreign,
SUM(IF(is_domestic, 1, 0)) as ids_domestic,
SUM(IF(is_foreign_and_domestic, 1, 0)) as ids_foreign_and_domestic,
SUM(IF(is_unknown, 1, 0)) as ids_unknown,
COUNT(*) AS ids_total,
SUM(IF(is_foreign, 1, 0))/COUNT(*) as prop_foreign,
SUM(IF(is_domestic, 1, 0))/COUNT(*) as prop_domestic,
SUM(IF(is_foreign_and_domestic, 1, 0))/COUNT(*) as prop_foreign_and_domestic,
SUM(IF(is_unknown, 1, 0))/COUNT(*) as prop_unknown,
SUM(IF(is_foreign OR is_foreign_and_domestic, 1, 0))/COUNT(*) as prop_foreign_combined,
FROM identities_classified 
GROUP BY flag
ORDER BY ids_total DESC
'''

df_ownership_by_flag = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -
df_ownership_by_flag

# ## Reflagging flags
#
# For each flag, this query counts the number of hulls that were involved in at least one reflagging with that flag (either in or out, doesn't matter). It then counts the number of hulls that are flagged under that flag and never reflagged. Both of those counts are then normalized by the total number of hulls, which is the sum of those two counts. This number is validated by counting the total distinct hulls from the identity table for comparison. There are currently two flags whose totals don't match because there is one reflagging event involving KHM that is wrongly attributed to CHN for some reason.
#
#
# This query only considers flag_in because the table was built to ONLY include vessels that reflagged at least once and it include the terminal flag (the flag that the vessel is last known to have been flying) with flag_in as that flag and flag_out as NULL. Therefore, flag_out never needs to be considered. There are also a few hundred rows where flag_in == flag_out because a different identity change happened that didn't involve the flag. These rows have been removed for now after discussion with Jaeyoon.

reflagging_table = 'world-fishing-827.scratch_jaeyoon.reflagging_flag_in_out_v20210422'


# +
q = f'''
WITH 

reflag_count AS (
SELECT 
flag_in as flag,
count(DISTINCT vessel_record_id) as hulls_reflagged,
FROM `{reflagging_table}`
WHERE flag_in IS NOT NULL
AND (flag_in != flag_out OR flag_out IS NULL)
GROUP BY flag_in
),

no_reflag_count AS (
SELECT
flag,
count(DISTINCT vessel_record_id) AS hulls_not_reflagged
FROM `{identity_table}`
WHERE first_timestamp <=  TIMESTAMP("2020-12-31 23:59:59 UTC")
AND vessel_record_id NOT IN (SELECT DISTINCT vessel_record_id FROM `{reflagging_table}`)
GROUP BY flag
),

total_count AS (
SELECT
flag,
count(DISTINCT vessel_record_id) AS distinct_hulls
FROM `{identity_table}`
WHERE first_timestamp <=  TIMESTAMP("2020-12-31 23:59:59 UTC")
GROUP BY flag    
)

## Check that total_hulls == distinct_hulls
## Currently there one flagging misattributed from KHM to CHN
## causing totals to not match for those two flags
SELECT 
flag,
IFNULL(hulls_reflagged, 0) AS hulls_reflagged,
IFNULL(hulls_not_reflagged, 0) AS hulls_not_reflagged,
distinct_hulls,
IFNULL(hulls_reflagged, 0) + IFNULL(hulls_not_reflagged, 0) AS hulls_total,
IFNULL(hulls_reflagged, 0)/(IFNULL(hulls_reflagged, 0) + IFNULL(hulls_not_reflagged, 0)) AS norm_reflag,
IFNULL(hulls_not_reflagged, 0)/(IFNULL(hulls_reflagged, 0) + IFNULL(hulls_not_reflagged, 0)) AS norm_not_reflag
FROM reflag_count 
LEFT JOIN no_reflag_count USING(flag)
LEFT JOIN total_count USING(flag)
# WHERE distinct_hulls != (IFNULL(hulls_reflagged, 0) + IFNULL(hulls_not_reflagged, 0))
ORDER BY norm_reflag DESC
'''

df_reflagging_flags = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -
df_reflagging_flags

# ## Length of flag stays
#
# Average length that an identity is active while flying a given flag.

# +
q = f'''
WITH 

identities_with_length AS (
    SELECT 
    *,
    TIMESTAMP_DIFF(last_timestamp, first_timestamp, DAY) as length_days
    FROM `{identity_table}`
    WHERE NOT timestamp_overlap
    AND flag IS NOT NULL
),

median_length_by_flag AS (
    SELECT
    DISTINCT flag,
    median_flag_stay
    FROM (
    SELECT
        flag,
        PERCENTILE_CONT(length_days, 0.5) OVER(PARTITION BY flag) 
            AS median_flag_stay
    FROM identities_with_length)
),

stats_by_flag AS (
    SELECT 
    flag,
    COUNT(*) as num_identities,
    COUNT(DISTINCT vessel_record_id) as num_hulls,
    AVG(length_days) as avg_flag_stay,
    SUM(length_days) as total_flag_stay
    FROM identities_with_length
    GROUP BY flag
    ORDER BY avg_flag_stay
)

SELECT 
flag,
num_identities,
num_hulls,
avg_flag_stay,
median_flag_stay,
total_flag_stay
FROM stats_by_flag
JOIN median_length_by_flag USING (flag)

'''

df_flag_stays = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -
df_flag_stays



# ## Join tables together to get all the data for each flag and add information on if the flag is a suspected Flag of Convenience (FOC)
#
# Fill in the reflagging with zeros where a flag doesn't have any.

# +
# From Galaz et al, 2018
# These are flags from the ITF list that are also tax havens so it is a subset of focs_itf
focs_galaz = ['ATG', 'BHS', 'BRB', 'BLZ','BMU', 'CYM', 'CYP', 'GIB', 'LBN', 'LBR', \
              'MLT', 'MHL', 'MUS', 'ANT', 'PAN', 'VCT', 'TON', 'VUT']

# Full ITF list
# https://www.itfseafarers.org/en/focs/current-registries-listed-as-focs
focs_itf = ['ATG', 'BHS', 'BRB', 'BLZ', 'BMU', 'KHM', 'CYM', 'COM', 'CYP', 'GNQ', 'FRO', 'GEO', \
            'GIB', 'HND', 'JAM', 'LBN', 'LBR', 'MLT', 'PMD', 'MHL', 'MUS', 'MDA', 'MNG', 'MMR', \
            'ANT', 'PRK', 'PAN', 'STP', 'VCT', 'LKA', 'TON', 'VUT']

# From Ford and Wilcox, 2019
# Could be a conflict here since they use an "ownership ratio"
# that is "a measure of the ratio of the fleet nationally flagged to nationally owned"
# as one of the factors to identify flags of convenience.
# 
# I've taken their ranking and chosen the top 20
focs_fordwilcox = ['CYM', 'ATG', 'LBR', 'VCT', 'VUT', 'BRB', 'SLE', 'MHS', 'KHM', 'KNA', \
                   'BOL', 'BHS', 'PAN', 'MLT', 'TZA', 'HND', 'BRU', 'KIR']

focs = list(set(focs_galaz + focs_itf + focs_fordwilcox))
# -

df_flag_data = df_ownership_by_flag.merge(df_reflagging_flags, on='flag', how='outer').fillna(0)
df_flag_data = df_flag_data.merge(df_flag_stays, on='flag', how='outer').fillna(0)
df_flag_data['is_foc'] = np.where(df_flag_data.flag.isin(focs), True, False)
df_flag_data

# # Analysis

# ## Figure 1: Do the Top 20 reflagging countries tend to have more foreign ownership than less frequently reflagged countries?

# This analysis was originally done using the top 20 flags by the raw number of reflagging events for each flag. However, this was heavily weighted towards major flag states with a lot of vessels so I normalized by the number of identities flagged to each flag state.
#
# Once sorted by normalized reflagging numbers, there are a lot of flags with very few reflagging instances and overall small fleet sizes that are not high impact to the larger system. In order to focus on larger fleets where insight and management changes would have the most impact, I filtered the flags down to only those in the top 50% (hulls_total >= 24) by number of unique hulls registered to a flag at some point during 2012-2020. I then use this set of vessels throughout the final analysis.
#
# *Does this make sense? Does it seem too finicky or cherry picked?*

df_flag_data.hulls_total.describe()

df_flag_data.hulls_total.hist(bins=100)

# ### Filter down to flag states that were in the top 50% by number of identities using their flag at some point in time. 

df_flag_data_filtered = df_flag_data[df_flag_data.hulls_total >= 26].sort_values('norm_reflag', ascending=False).reset_index(drop=True)
print(df_flag_data_filtered.shape[0])
df_flag_data_filtered

# ### The top 20 by normalized reflagging are as follows:

df_top20_flags = df_flag_data_filtered[0:20]
df_other_flags = df_flag_data_filtered[20:]
df_top20_flags

# ### The top 20 by foreign ownership ratio are as follows:
#
# This uses `prop_foreign_combined`, which is calculated using all identities that are marked as `foreign` OR `foreign_and_domestic`.

df_top20_foreign_ownership = df_flag_data_filtered.sort_values("prop_foreign", ascending=False)[0:20]
df_other_foreign_ownership = df_flag_data_filtered.sort_values("prop_foreign", ascending=False)[20:]
df_top20_foreign_ownership

# ## Plot the average ownership types for the Top 20 flag states by normalized reflagging versus all other flags AND the normalized reflagging for the Top 20 flags by foreign ownership vs. all other flags (both in the top 50% by size)

# +
df_other_flags = df_flag_data_filtered[20:]

prop_domestic_owner = [df_top20_flags.prop_domestic.mean(),
                        df_other_flags.prop_domestic.mean()]
prop_foreign_domestic_owner = [df_top20_flags.prop_foreign_and_domestic.mean(),
                       df_other_flags.prop_foreign_and_domestic.mean()]
prop_foreign_owner = [df_top20_flags.prop_foreign.mean(),
                       df_other_flags.prop_foreign.mean()]
prop_unknown_owner = [df_top20_flags.prop_unknown.mean(),
                       df_other_flags.prop_unknown.mean()]

fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(12, 8))

### Top 20 flags by normalized reflagging
x_labels = ['Top 20\nReflagged Flags', 'All Other Flags']
# fig, ax = plt.subplots(figsize=(6, 8))
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax0.bar(indices, prop_domestic_owner, width, label="Vessel registered to\nsame country as owner", color=color_dark_blue)
p2 = ax0.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
p3 = ax0.bar(indices, prop_foreign_domestic_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="Vessel registered both\ndomestic and foreign", color=color_yellow)
p4 = ax0.bar(indices, prop_unknown_owner, width, bottom=list(map(add, list(map(add, prop_domestic_owner, prop_foreign_domestic_owner)), prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax0.set_xticks(indices)
ax0.set_xticklabels(x_labels, fontsize=16)
ax0.set_ylabel("Mean ownership ratio", fontsize=16)
ax0.set_title("Ownership type for top 20 most\nreflagged flags vs. other flags", fontsize=18)
ax0.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)


### Top 20 flags by foreign ownership ratio
norm_reflagging = [df_top20_foreign_ownership.norm_reflag.mean(),
                   df_other_foreign_ownership.norm_reflag.mean()]
norm_not_reflagging = [df_top20_foreign_ownership.norm_not_reflag.mean(),
                       df_other_foreign_ownership.norm_not_reflag.mean()]

x_labels = ['Top 20 by \nForeign Ownership', 'All Other Flags']
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax1.bar(indices, norm_reflagging, width, label="Hulls that were reflagged at least once", color=color_purple)
p1 = ax1.bar(indices, norm_not_reflagging, width, bottom=norm_reflagging, label="Hulls that were never reflagged", color=color_orange)
# p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
# p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax1.set_xticks(indices)
ax1.set_xticklabels(x_labels, fontsize=16)
ax1.set_ylabel("Mean normalized reflagging", fontsize=16)
ax1.set_title("Reflagging activity for top 20 flags with\nmost foreign ownership vs. other flags", fontsize=18)
ax1.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=1)

### Global layout settings
plt.yticks(fontsize=14)
plt.tight_layout()
plt.savefig('./figures/top20_versus_others.png')
plt.show()
# -
print("Domestic:          ", prop_domestic_owner)
print("Foreign:           ", prop_foreign_owner)
print("Foreign & Domestic:", prop_foreign_domestic_owner)
print("Unknown:           ", prop_unknown_owner)

# ##### Stats for the identity paper

# #### TODO: add foreign+domestic to the foreign stats

print(f"There are {prop_foreign_owner[0]/prop_domestic_owner[0]} times more foreign owned vessels than domestic ones")
print(f"The Top 20 most reflagged flags have a foreign ownership ratio that is {prop_foreign_owner[0]/prop_foreign_owner[1]} times more than the group of all other flags, ({prop_foreign_owner[0]*100:0.1f}% vs {prop_foreign_owner[1]*100:0.1f}%). It's greater than foreign and unknown ownership combined for all other states ({(prop_foreign_owner[1]+prop_unknown_owner[1])*100:0.1f}%).")


# ### Supplementary Materials for Figure 1

# #### What are the top 20 countries by normalized reflagging?
#
# TODO: Expand to include full name, number of reflaggings, number of total flagged vessels, and normalized reflaggings
#
# Medium article with how to style table: https://towardsdatascience.com/simple-little-tables-with-matplotlib-9780ef5d0bc4

# +
title_text = 'Top 20 Flags\nby Normalized Reflagging'
fig_background_color = 'white'

# Put the data into matrix form
data = [[flag] for flag in df_top20_flags.flag.to_list()]

# Create the figure. Setting a small pad on tight_layout
# seems to better regulate white space. Sometimes experimenting
# with an explicit figsize here can produce better outcome.
plt.figure(linewidth=2,
           edgecolor=None,
           facecolor=fig_background_color,
           tight_layout={'pad':1},
           figsize=(5,8)
          )
# Add a table at the bottom of the axes
the_table = plt.table(cellText=data,
                      rowLabels=None,
                      rowColours=None,
                      rowLoc='center',
                      colColours=None,
                      colLabels=None,
                      colWidths=[0.5],
                      cellLoc='center',
                      loc='center',
                     )

# Scaling is the only influence we have over top and bottom cell padding.
# Make the rows taller (i.e., make cell y scale larger).
the_table.scale(1, 2.5)

# Set font size
the_table.set_fontsize(14)

# Hide axes
ax = plt.gca()
ax.get_xaxis().set_visible(False)
ax.get_yaxis().set_visible(False)
# Hide axes border
plt.box(on=None)
# Add title
plt.suptitle(title_text)
# Add footer
# Force the figure to update, so backends center objects correctly within the figure.
# Without plt.draw() here, the title will center on the axes and not the figure.
plt.draw()
# Create image. plt.savefig ignores figure edge and face colors, so map them.
fig = plt.gcf()
plt.savefig('figures/top_20_reflagged_flags_list.png',
            #bbox='tight',
            edgecolor=fig.get_edgecolor(),
            facecolor=fig.get_facecolor(),
            dpi=150
            )
# -

# #### What are the top 20 countries by foreign ownership?
#
# TODO: Expand to include full name, number of reflaggings, number of total flagged vessels, and normalized reflaggings
#
# Medium article with how to style table: https://towardsdatascience.com/simple-little-tables-with-matplotlib-9780ef5d0bc4

# +
title_text = 'Top 20 Flags\nby Foreign Ownership Ratio'
fig_background_color = 'white'

# Put the data into matrix form
data = [[flag] for flag in df_top20_foreign_ownership.flag.to_list()]

# Create the figure. Setting a small pad on tight_layout
# seems to better regulate white space. Sometimes experimenting
# with an explicit figsize here can produce better outcome.
plt.figure(linewidth=2,
           edgecolor=None,
           facecolor=fig_background_color,
           tight_layout={'pad':1},
           figsize=(5,8)
          )
# Add a table at the bottom of the axes
the_table = plt.table(cellText=data,
                      rowLabels=None,
                      rowColours=None,
                      rowLoc='center',
                      colColours=None,
                      colLabels=None,
                      colWidths=[0.5],
                      cellLoc='center',
                      loc='center',
                     )

# Scaling is the only influence we have over top and bottom cell padding.
# Make the rows taller (i.e., make cell y scale larger).
the_table.scale(1, 2.5)

# Set font size
the_table.set_fontsize(14)

# Hide axes
ax = plt.gca()
ax.get_xaxis().set_visible(False)
ax.get_yaxis().set_visible(False)
# Hide axes border
plt.box(on=None)
# Add title
plt.suptitle(title_text)
# Add footer
# Force the figure to update, so backends center objects correctly within the figure.
# Without plt.draw() here, the title will center on the axes and not the figure.
plt.draw()
# Create image. plt.savefig ignores figure edge and face colors, so map them.
fig = plt.gcf()
plt.savefig('figures/top_20_foreign_ownership_flags_list.png',
            #bbox='tight',
            edgecolor=fig.get_edgecolor(),
            facecolor=fig.get_facecolor(),
            dpi=150
            )
# -

# #### What is the breakdown for each individual country in the Top 20?

temp = df_top20_flags.copy()
flags = temp.flag.to_list()
fig, ax = plt.subplots(figsize=(8,7))
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.prop_domestic, width, label="Vessel registered to\nsame country as owner", color=color_dark_blue)
p2 = ax.bar(indices, temp.prop_foreign, width, bottom=temp.prop_domestic, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
p3 = ax.bar(indices, temp.prop_foreign_and_domestic, width, bottom=temp.prop_domestic+temp.prop_foreign, label="Vessel registered both\ndomestic and foreign", color=color_yellow)
p4 = ax.bar(indices, temp.prop_unknown, width, bottom=temp.prop_domestic+temp.prop_foreign+temp.prop_foreign_and_domestic, label="No information\navailable on owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90, fontsize=14)
plt.yticks(fontsize=14)
ax.set_ylabel("Proportion of vessels", fontsize=16)
plt.title("Vessel ownership type\nfor top 20 most reflagged flags")
plt.legend(loc='upper left', bbox_to_anchor=[0.08,-0.18], ncol=2)
plt.tight_layout()
plt.savefig('./figures/ownership_top20_reflagged_flags.png')
plt.show()

# ### Flag Stay

# +
avg_flag_stay_reflag = [df_top20_flags.avg_flag_stay.mean(),
                        df_other_flags.avg_flag_stay.mean()]

fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(12, 8))

### Top 20 flags by normalized reflagging
x_labels = ['Top 20\nReflagged Flags', 'All Other Flags']
# fig, ax = plt.subplots(figsize=(6, 8))
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax0.bar(indices, avg_flag_stay_reflag, width, label="Average length that an identity\nstays with a flag", color=color_dark_blue)
ax0.set_xticks(indices)
ax0.set_xticklabels(x_labels, fontsize=16)
ax0.set_ylabel("Average length of flag stay", fontsize=16)
ax0.set_title("Average length of flag stay for top 20 most\nreflagged flags vs. other flags", fontsize=18)
ax0.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)


### Top 20 flags by foreign ownership ratio
avg_flag_stay_foreign = [df_top20_foreign_ownership.avg_flag_stay.mean(),
                         df_other_foreign_ownership.avg_flag_stay.mean()]


x_labels = ['Top 20 by \nForeign Ownership', 'All Other Flags']
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax1.bar(indices, avg_flag_stay_foreign, width, color=color_dark_blue)
ax1.set_xticks(indices)
ax1.set_xticklabels(x_labels, fontsize=16)
ax1.set_ylabel("Average length of flag stay", fontsize=16)
ax1.set_title("Average length of flag stay for top 20 flags with\nmost foreign ownership vs. other flags", fontsize=18)

### Global layout settings
plt.yticks(fontsize=14)
plt.tight_layout()
plt.savefig('./figures/top20_versus_others_flag_stay.png')
plt.show()
print(avg_flag_stay_reflag)
# +
avg_flag_stay_reflag = [df_top20_flags.median_flag_stay.mean(),
                        df_other_flags.median_flag_stay.mean()]

fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(12, 8))

### Top 20 flags by normalized reflagging
x_labels = ['Top 20\nReflagged Flags', 'All Other Flags']
# fig, ax = plt.subplots(figsize=(6, 8))
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax0.bar(indices, avg_flag_stay_reflag, width, label="Average median length that an identity\nstays with a flag", color=color_dark_blue)
ax0.set_xticks(indices)
ax0.set_xticklabels(x_labels, fontsize=16)
ax0.set_ylabel("Average median length of flag stay", fontsize=16)
ax0.set_title("Average median length of flag stay for top 20 most\nreflagged flags vs. other flags", fontsize=18)
ax0.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)


### Top 20 flags by foreign ownership ratio
avg_flag_stay_foreign = [df_top20_foreign_ownership.median_flag_stay.mean(),
                         df_other_foreign_ownership.median_flag_stay.mean()]


x_labels = ['Top 20 by \nForeign Ownership', 'All Other Flags']
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax1.bar(indices, avg_flag_stay_foreign, width, color=color_dark_blue)
ax1.set_xticks(indices)
ax1.set_xticklabels(x_labels, fontsize=16)
ax1.set_ylabel("Average median length of flag stay", fontsize=16)
ax1.set_title("Average median length of flag stay for top 20 flags with\nmost foreign ownership vs. other flags", fontsize=18)

### Global layout settings
plt.yticks(fontsize=14)
plt.tight_layout()
plt.savefig('./figures/top20_versus_others_median_flag_stay.png')
plt.show()
print(avg_flag_stay_reflag)
print(avg_flag_stay_foreign)
# -
temp = df_top20_flags.copy()
flags = temp.flag.to_list()
fig, ax = plt.subplots(figsize=(8,7))
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.avg_flag_stay, width, label="Average length that an identity\nstays with a flag", color=color_dark_blue)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90, fontsize=14)
plt.yticks(fontsize=14)
ax.set_ylabel("Average length of flag stay", fontsize=16)
plt.title("Average length of flag stay\nfor top 20 most reflagged flags")
plt.legend(loc='upper left', bbox_to_anchor=[0.08,-0.18], ncol=2)
plt.tight_layout()
plt.savefig('./figures/flag_stay_top20_reflagged_flags.png')
plt.show()

temp = df_top20_foreign_ownership.copy()
flags = temp.flag.to_list()
fig, ax = plt.subplots(figsize=(8,7))
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.avg_flag_stay, width, label="Average length that an identity\nstays with a flag", color=color_dark_blue)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90, fontsize=14)
plt.yticks(fontsize=14)
ax.set_ylabel("Average length of flag stay", fontsize=16)
plt.title("Average length of flag stay\nfor top 20 flags with highest proprotion of foreign vessels")
plt.legend(loc='upper left', bbox_to_anchor=[0.08,-0.18], ncol=2)
plt.tight_layout()
plt.savefig('./figures/flag_stay_top20_foreign_flags.png')
plt.show()

# +
ax = df_flag_data_filtered[df_flag_data_filtered.is_foc].plot.scatter('avg_flag_stay', 'prop_foreign_combined', c=color_dark_pink, s=40, figsize=(10, 6), label='Suspected FOC', zorder=2)
df_flag_data_filtered[~df_flag_data_filtered.is_foc].plot.scatter('avg_flag_stay', 'prop_foreign_combined', c=color_dark_blue, s=20, ax=ax, label='All other flags', zorder=2)
for i, row in df_flag_data_filtered.iterrows():
    ax.annotate(row.flag, xy=(row.avg_flag_stay, row.prop_foreign_combined), zorder=2)

# Plot regression line
# x_reg = list(np.arange(0, 1.1, 0.1))
# y_reg = [b_for + x0*m_for for x0 in x_reg]
# ax.plot(x_reg, y_reg, color='black', zorder=1)

# Bounding box with prop_foreign >= 0.15 and norm_reflag >= 0.3
# ax.axvline(0.3, color=color_gray, linestyle='--')
# ax.axhline(0.15, color=color_gray, linestyle='--')

plt.title(f"Proportion foreign ownership vs. average flag stay")#\nR-squared = {r_sq_for:.2f}")
plt.xlabel("Average length of flag stay")
plt.ylabel("Proportion foreign ownership")
plt.tight_layout()
# plt.savefig('./figures/regression_ownership_flag_stay_foreign.png')
plt.show()

# +
ax = df_flag_data_filtered[df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'median_flag_stay', c=color_dark_pink, s=40, figsize=(10, 6), label='Suspected FOC', zorder=2)
df_flag_data_filtered[~df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'median_flag_stay', c=color_dark_blue, s=20, ax=ax, label='All other flags', zorder=2)
for i, row in df_flag_data_filtered.iterrows():
    ax.annotate(row.flag, xy=(row.norm_reflag, row.median_flag_stay), zorder=2)

# Plot regression line
# x_reg = list(np.arange(0, 1.1, 0.1))
# y_reg = [b_for + x0*m_for for x0 in x_reg]
# ax.plot(x_reg, y_reg, color='black', zorder=1)

# Bounding box with prop_foreign >= 0.15 and norm_reflag >= 0.3
# ax.axvline(0.3, color=color_gray, linestyle='--')
# ax.axhline(0.15, color=color_gray, linestyle='--')

plt.title(f"Normalized reflagging vs. median flag stay")#\nR-squared = {r_sq_for:.2f}")
plt.xlabel("Normalized incidents of reflagging")
plt.ylabel("Median length of flag stay")
plt.tight_layout()
# plt.savefig('./figures/regression_ownership_median_flag_stay_foreign.png')
plt.show()
# -

# ## Figure 2: Do the Top 20 countries having the most foreign owned vessels registered to other countries tend to reflag more than other countries?
#
# NOTE: Using `prop_foreign_combined` for this which combines `prop_foreign` and `prop_foreign_and_domestic`

# ### Linear regression analysis and FOCs

# ##### Fit a linear regression model

# +
from sklearn.linear_model import LinearRegression
x_for = np.asarray(df_flag_data_filtered.norm_reflag.to_list()).reshape((-1,1))
y_for = np.array(df_flag_data_filtered.prop_foreign_combined.to_list())

model_for = LinearRegression().fit(x_for, y_for)
r_sq_for = model_for.score(x_for, y_for)
m_for = model_for.coef_[0]
b_for = model_for.intercept_
print(r_sq_for)
# -

num_foc = df_flag_data_filtered[df_flag_data_filtered.is_foc].shape[0]
num_bounding = df_flag_data_filtered[(df_flag_data_filtered.prop_foreign_combined >= 0.15) & (df_flag_data_filtered.norm_reflag >= 0.30)].shape[0]
num_bounding_foc = df_flag_data_filtered[(df_flag_data_filtered.prop_foreign_combined >= 0.15) & (df_flag_data_filtered.norm_reflag >= 0.30) & (df_flag_data_filtered.is_foc)].shape[0]
num_bounding_top20 = df_flag_data_filtered[(df_flag_data_filtered.prop_foreign_combined >= 0.15) & (df_flag_data_filtered.norm_reflag >= 0.30) & (df_flag_data_filtered.flag.isin(df_top20_flags.flag.to_list()))].shape[0]
print(f"Flags that are suspected FOC: {num_foc}")
print(f"Flags with >=15% foreign ownership and >=30% of fleet reflagging: {num_bounding} ({num_bounding/df_flag_data_filtered.shape[0]*100:0.2f}% of top 50% of fleet)")
print(f"Flags with >=15% foreign ownership and >=30% of fleet reflagging AND top 20 flag: {num_bounding_top20} ({num_bounding_top20/num_bounding*100:0.2f}% of flags above these thresholds, {num_bounding_top20/20*100:0.2f}% of all top 20 flags)")
print(f"Flags with >=15% foreign ownership and >=30% of fleet reflagging AND suspected FOC: {num_bounding_foc} ({num_bounding_foc/num_bounding*100:0.2f}% of flags above these thresholds, {num_bounding_foc/num_foc*100:0.2f}% of all FOC)")









# #### TODO:
# * Mark Top 20 groups somehow?
# * Rethink list of FOCs altogether (need to do full lit review for better sources)

# +
# ax = df_flag_data_filtered[df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'prop_foreign_combined', c=color_dark_pink, s=40, figsize=(10, 6), label='Suspected FOC', zorder=2)
ax = df_flag_data_filtered.plot.scatter('norm_reflag', 'prop_foreign_combined', c=color_dark_blue, s=20, zorder=2)
for i, row in df_flag_data_filtered.iterrows():
    ax.annotate(row.flag, xy=(row.norm_reflag, row.prop_foreign_combined), zorder=2)

# Plot regression line
x_reg = list(np.arange(0, 1.1, 0.1))
y_reg = [b_for + x0*m_for for x0 in x_reg]
ax.plot(x_reg, y_reg, color='black', zorder=1)

# Bounding box with prop_foreign >= 0.15 and norm_reflag >= 0.3
ax.axvline(0.3, color=color_gray, linestyle='--')
ax.axhline(0.15, color=color_gray, linestyle='--')

plt.title(f"Proportion foreign ownership vs. normalized reflagging")#\nR-squared = {r_sq_for:.2f}")
plt.xlabel("Normalized incidents of reflagging")
plt.ylabel("Proportion foreign ownership")
plt.tight_layout()
plt.savefig('./figures/regression_ownership_reflagging_foreign.png')
plt.show()
# -

# #### Average median length that an identity stays with the flags in the upper left quadrant vs. lower right quadrant

# +
flags_high = df_flag_data_filtered[(df_flag_data_filtered.norm_reflag >= 0.3) & (df_flag_data_filtered.prop_foreign_combined >= 0.15)]
flags_low = df_flag_data_filtered[(df_flag_data_filtered.norm_reflag < 0.3) & (df_flag_data_filtered.prop_foreign_combined < 0.15)]

print(f"The average median length of stay for the flags above both thresholds is {flags_high.median_flag_stay.mean()} days")
print(f"The average median length of stay for the flags below both thresholds is {flags_low.median_flag_stay.mean()} days")
# -

# #### Old image with FOCs from ITF marked

# +
ax = df_flag_data_filtered[df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'prop_foreign_combined', c=color_dark_pink, s=40, figsize=(10, 6), label='Suspected FOC', zorder=2)
df_flag_data_filtered[~df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'prop_foreign_combined', c=color_dark_blue, s=20, ax=ax, label='All other flags', zorder=2)
for i, row in df_flag_data_filtered.iterrows():
    ax.annotate(row.flag, xy=(row.norm_reflag, row.prop_foreign_combined), zorder=2)

# Plot regression line
x_reg = list(np.arange(0, 1.1, 0.1))
y_reg = [b_for + x0*m_for for x0 in x_reg]
ax.plot(x_reg, y_reg, color='black', zorder=1)

# Bounding box with prop_foreign >= 0.15 and norm_reflag >= 0.3
ax.axvline(0.3, color=color_gray, linestyle='--')
ax.axhline(0.15, color=color_gray, linestyle='--')

plt.title(f"Proportion foreign ownership vs. normalized reflagging\nR-squared = {r_sq_for:.2f}")
plt.xlabel("Normalized incidents of reflagging")
plt.ylabel("Proportion foreign ownership")
plt.tight_layout()
plt.savefig('./figures/regression_ownership_reflagging_foreign_foc.png')
plt.show()

# +
from sklearn.linear_model import LinearRegression
x_dom = np.asarray(df_flag_data_filtered.norm_reflag.to_list()).reshape((-1,1))
y_dom = np.array(df_flag_data_filtered.prop_domestic.to_list())

model_dom = LinearRegression().fit(x_dom, y_dom)
r_sq_dom = model_dom.score(x_dom, y_dom)
m_dom = model_dom.coef_[0]
b_dom = model_dom.intercept_
print(r_sq_dom)

# +
ax = df_flag_data_filtered[df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'prop_domestic', c=color_dark_pink, s=40, figsize=(10, 6), label='Suspected FOC')
df_flag_data_filtered[~df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'prop_domestic', c=color_dark_blue, s=20, ax=ax, label='All other flags')
for i, row in df_flag_data_filtered.iterrows():
    ax.annotate(row.flag, xy=(row.norm_reflag, row.prop_domestic))

# Plot regression line
x_reg = list(np.arange(0, 1.1, 0.1))
y_reg = [b_dom + x0*m_dom for x0 in x_reg]
ax.plot(x_reg, y_reg)

plt.title(f"Proportion domestic ownership vs. normalized reflagging\nR-squared = {r_sq_dom:.2f}")
plt.xlabel("Normalized incidents of reflagging")
plt.ylabel("Proportion domestic ownership")
plt.tight_layout()
plt.savefig('./figures/regression_ownership_reflagging_domestic.png')
plt.show()
# -








# ---
# ---
# # MAPPING FISHING EFFORT BY OWNERSHIP

# ### Transform ownership information to be able to be referenced "by MMSI"

ownership_by_mmsi_table = 'world-fishing-827.scratch_jenn.ownership_by_mmsi_2012_2020_v20210601'

# +
q = f'''
-------------------------------------------------------------------------------
-- Check if there are overlapping timestamp ranges in the given array
-- of timestamp ranges (each consisting of first timestamp and last timestamp).
-- If at least one timestamp range does overlap with another time range
-- the function returns TRUE. If no time ranges overlaps, it then returns FALSE
-------------------------------------------------------------------------------
CREATE TEMP FUNCTION check_timestamp_overlap (
    arr ARRAY<STRUCT<first_timestamp TIMESTAMP, last_timestamp TIMESTAMP>>) AS ((
  WITH
    ------------------------------------
    -- Flatten the given array of struct
    ------------------------------------
    ts AS (
      SELECT first_timestamp, last_timestamp
      FROM UNNEST (arr)
    ),

    -----------------------------------------------
    -- Cross join all time ranges except themselves
    -- to determine there is an overlap of time
    -----------------------------------------------
    compare_ts AS (
      SELECT
        a.first_timestamp < b.last_timestamp
        AND a.last_timestamp > b.first_timestamp AS overlap
      FROM ts AS a
      CROSS JOIN ts AS b
      WHERE NOT (a.first_timestamp = b.first_timestamp
        AND a.last_timestamp = b.last_timestamp)
    )

  -------------------------------------------------------------------
  -- If only one time range per vessel is given, there is no overlap.
  -- Otherwise, determine by Logical OR if there is an overlap
  -------------------------------------------------------------------
  SELECT
    IF (COUNT (*) <= 1,
      FALSE,
      IF (LOGICAL_OR (overlap) IS NULL,
        TRUE, LOGICAL_OR (overlap) ) )
  FROM compare_ts
));

--------------------------
-- Sort ARRAY by timestamp
--------------------------
CREATE TEMP FUNCTION sort_array(arr ANY TYPE) AS ((
  SELECT
    ARRAY_AGG(a ORDER BY a.first_timestamp ASC, a.last_timestamp ASC)
  FROM (
    SELECT a
    FROM UNNEST(arr) a ) activity
));

WITH 

## Get mapping of territory flags
## to the flag of the sovereign country.
## Used to better characterize foreign ownership.
territory_flag_mapping AS (
    SELECT DISTINCT
    territory1_iso3, sovereign1_iso3
    FROM `{eez_info_table}`
    WHERE eez_type = '200NM'
    AND territory1_iso3 != sovereign1_iso3
),

## Get all identities from the sample database release
## with key attributes such as geartype and time ranges
all_identities AS (
    SELECT 
    vessel_record_id,
    ssvid,
    IFNULL(n_shipname, 'null') as n_shipname,
    IFNULL(n_callsign, 'null') as n_callsign,
    IFNULL(flag, 'null') as flag,
    geartype,
    first_timestamp,
    last_timestamp,
    is_fishing,
    is_carrier,
    is_bunker
    FROM `{identity_table}`
    WHERE DATE(first_timestamp) <= '2020-12-31'
),

## Get all ownership information from the sample database release
all_ownership AS (
    SELECT 
    vessel_record_id,
    ssvid,
    IFNULL(n_shipname, 'null') as n_shipname,
    IFNULL(n_callsign, 'null') as n_callsign,
    IFNULL(flag, 'null') as flag,
    owner,
    owner_flag
    FROM `{owner_table}`
),

## Join identities with ownership information using
## vessel_record_id, ssvid, n_shipname, n_callsign, and flag
## Remove the few times were the identity has a null flag
identities_with_ownership AS (
    SELECT
    vessel_record_id,
    ssvid,
    n_shipname,
    n_callsign,
    flag,
    IFNULL((SELECT sovereign1_iso3 FROM territory_flag_mapping WHERE territory1_iso3 = flag), flag) as flag_sovereign,
    owner,
    owner_flag,
    IFNULL((SELECT sovereign1_iso3 FROM territory_flag_mapping WHERE territory1_iso3 = owner_flag), owner_flag) as owner_flag_sovereign,
    FROM all_identities 
    LEFT JOIN all_ownership 
    USING(vessel_record_id, ssvid, n_shipname, n_callsign, flag)
    WHERE flag != 'null'
    ORDER BY n_shipname, n_callsign, ssvid, flag, owner, owner_flag
),

## Gather all of the flags for non-null owners for each identity
## This is necessary to properly handle flags coming from null owners
## so that only flags not already represented by non-null owners
## are included in `identities_final`.
identities_flags_for_nonnull_owners AS (
    SELECT
    vessel_record_id, n_shipname, n_callsign, flag,
    ARRAY_AGG(DISTINCT owner_flag_sovereign) AS flags_for_nonnull_owners
    FROM identities_with_ownership
    WHERE owner IS NOT NULL
    GROUP BY vessel_record_id, n_shipname, n_callsign, flag
),

## Gather all of the SSVIDs used by an identity where identity
## is defined as above but minus `ssvid`
ssvid_for_all_owners AS (
    SELECT
    vessel_record_id, n_shipname, n_callsign, flag,
    ARRAY_AGG(DISTINCT ssvid) AS ssvids_used
    FROM identities_with_ownership
    GROUP BY vessel_record_id, n_shipname, n_callsign, flag 
),

## For each unique combination of identity and ownership information,
## classify it as foreign, domestic, or unknown. Null owners are included
## only if their flag is not already specified for an identity
## by a non-null owner to avoid duplication from varying quality of registries.
identities_with_ownership_classified AS (
    SELECT DISTINCT
    vessel_record_id, n_shipname, n_callsign, flag, flag_sovereign,
    owner, owner_flag, owner_flag_sovereign,
    IF(owner_flag_sovereign IS NOT NULL AND flag_sovereign NOT IN (owner_flag_sovereign, owner_flag), 1, 0) as is_foreign,
    IF(owner_flag_sovereign IS NOT NULL AND flag_sovereign IN (owner_flag_sovereign, owner_flag), 1, 0) as is_domestic,
    IF(owner_flag_sovereign IS NULL, 1, 0) as is_unknown,
    FROM identities_with_ownership
    LEFT JOIN identities_flags_for_nonnull_owners USING(vessel_record_id, n_shipname, n_callsign, flag)
    WHERE (owner IS NOT NULL OR (owner_flag_sovereign IS NULL OR owner_flag_sovereign NOT IN UNNEST(flags_for_nonnull_owners)))
),

## Collapse ownership information by identity (vessel_record_id, n_shipname, n_callsign, flag)
## and count how many of the owners were foreign, domestic or unknown.
## Ignore unknown identities when vessels have other known ownerships
## If all known ownership is foreign or domestic, classify the identity as is_foreign or is_domestic, respectively.
## If all ownerships are unknown, classify the identity as is_unknown.
## If there is a mixture of known foreign and domestic owners, classify as is_foreign_and_domestic.
identities_classified AS (
SELECT 
vessel_record_id, n_shipname, n_callsign, flag,
IF(SUM(is_foreign) > 0 AND SUM(is_domestic) = 0, TRUE, FALSE) as is_foreign,
IF(SUM(is_foreign) = 0 AND SUM(is_domestic) > 0, TRUE, FALSE) as is_domestic,
IF(SUM(is_foreign) > 0 AND SUM(is_domestic) > 0, TRUE, FALSE) as is_foreign_and_domestic,
IF(SUM(is_foreign) = 0 AND SUM(is_domestic) = 0 AND SUM(is_unknown) > 0, TRUE, FALSE) as is_unknown,
COUNT(*) AS num_owners
FROM identities_with_ownership_classified
GROUP BY vessel_record_id, n_shipname, n_callsign, flag
),

## Join the final classified identities with the list of SSVID
## they are associated with.
identities_classified_with_ssvid AS (
SELECT 
*
FROM identities_classified
LEFT JOIN ssvid_for_all_owners USING(vessel_record_id, n_shipname, n_callsign, flag)
ORDER BY num_owners DESC
),

## Unnest SSVIDs for each identity so that there is a row for each SSVID in an identity.
## Join with `all_identities` to get the additional attributes for that identity
## such as geartype and the time range for which the identity is valid.
identities_by_ssvid AS (
SELECT
    ssvid AS mmsi, vessel_record_id, 
    IF(n_shipname = 'null', NULL, n_shipname) AS n_shipname, 
    IF(n_callsign = 'null', NULL, n_callsign) AS n_callsign, 
    IF(flag = 'null', NULL, flag) AS flag, 
    is_domestic, is_foreign, is_foreign_and_domestic, is_unknown,
    geartype, first_timestamp, last_timestamp, is_fishing, is_carrier, is_bunker
    FROM identities_classified_with_ssvid, UNNEST(ssvids_used) ssvid
    LEFT JOIN all_identities USING(ssvid, vessel_record_id, n_shipname, n_callsign, flag)
)

## Check which SSVID have identities that overlap in time
## and join that information to the final table.
SELECT 
*
FROM identities_by_ssvid
JOIN (SELECT 
      mmsi,
      check_timestamp_overlap(ARRAY_AGG(STRUCT(first_timestamp as first_timestamp, last_timestamp as last_timestamp))) AS timestamp_overlap
      FROM identities_by_ssvid
      GROUP BY mmsi)
USING (mmsi)
'''

df_ownership_by_mmsi = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


### Save the table to `scratch_jenn` in BigQuery to be used by the fishing effort queries
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()
job_config = bigquery.QueryJobConfig(destination=ownership_by_mmsi_table)

# Start the query, passing in the extra configuration.
query_job = client.query(q, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(ownership_by_mmsi_table))
# -
df_ownership_by_mmsi

# ### How many MMSI missing from public fishing effort data?drop_duplicates

public_fishing_effort_table = 'global-fishing-watch.gfw_public_data.fishing_effort_byvessel_v2'

# +
q = f'''
WITH 

mmsi_public_fishing_effort AS (
    SELECT DISTINCT 
    mmsi
    FROM `{public_fishing_effort_table}`
)

SELECT DISTINCT
mmsi 
FROM `{ownership_by_mmsi_table}`
WHERE is_fishing
AND DATE(first_timestamp) <= '2020-12-31'
AND mmsi NOT IN (SELECT mmsi FROM mmsi_public_fishing_effort )
'''

df_mmsi_missing_public_fishing = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

print(f'There are {df_ownership_by_mmsi[df_ownership_by_mmsi.is_fishing].mmsi.unique().shape[0]} distinct fishing MMSI')
print(f'There are {df_mmsi_missing_public_fishing.shape[0]} ({df_mmsi_missing_public_fishing.shape[0]/df_ownership_by_mmsi[df_ownership_by_mmsi.is_fishing].mmsi.unique().shape[0]*100:0.2f}%) fishing MMSI not in the public fishing effort data')

vi_table = 'world-fishing-827.gfw_research.vi_ssvid_v20210301'

# +
q = f'''
WITH 

  likely_gear AS (
  SELECT
    ssvid
  FROM `{vi_table}`
  WHERE
    best.best_vessel_class = 'gear'),

mmsi_public_fishing_effort AS (
    SELECT DISTINCT 
    mmsi
    FROM `global-fishing-watch.gfw_public_data.fishing_effort_byvessel_v2`
),

mmsi_missing AS (
    SELECT DISTINCT
    *
    FROM `{ownership_by_mmsi_table}`
    WHERE is_fishing
    AND DATE(first_timestamp) <= '2020-12-31'
    AND mmsi NOT IN (SELECT mmsi FROM mmsi_public_fishing_effort )
)

SELECT 
a.mmsi,
year,
a.n_shipname, 
a.n_callsign,
a.flag,
a.first_timestamp,
a.last_timestamp,
b.activity.active_hours,
b.activity.fishing_hours,
b.activity.overlap_hours_multinames,
b.activity.offsetting,
(SELECT COUNT(value) FROM UNNEST(b.ais_identity.n_shipname) WHERE count >= 10) as num_shipnames,
b.on_fishing_list_best,
b.best.best_vessel_class,
CAST(a.mmsi AS int64) IN (
        SELECT ssvid
        FROM gfw_research.bad_mmsi, UNNEST(ssvid) as ssvid
        ) AS is_bad_mmsi,
mmsi IN (SELECT ssvid FROM likely_gear) AS likely_gear,
a.vessel_record_id,
FROM mmsi_missing a
LEFT JOIN `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210301` b
ON a.mmsi = b.ssvid
AND ((a.n_shipname IS NULL AND b.registry_info.best_known_shipname IS NULL) OR (a.n_shipname = b.registry_info.best_known_shipname))
AND ((a.n_callsign IS NULL AND b.registry_info.best_known_callsign IS NULL) OR (a.n_callsign = b.registry_info.best_known_callsign))
AND ((a.flag IS NULL AND b.registry_info.best_known_flag IS NULL) OR (a.flag = b.registry_info.best_known_flag))
AND (b.year >= EXTRACT(year FROM a.first_timestamp) AND (b.year <= EXTRACT(year FROM a.last_timestamp)))
AND year <= 2020
# WHERE b.activity.fishing_hours IS NULL

'''

df_mmsi_missing_vi = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_mmsi_missing_vi


# +
df_mmsi_missing_vi_matched = df_mmsi_missing_vi[df_mmsi_missing_vi.year.notna()].copy().reset_index(drop=True)
df_mmsi_missing_vi_unmatched = df_mmsi_missing_vi[df_mmsi_missing_vi.year.isna()].copy().reset_index(drop=True)

print(f'{df_mmsi_missing_vi.mmsi.unique().shape[0]} distinct missing MMSI')
print(f'{df_mmsi_missing_vi_matched.mmsi.unique().shape[0]} distinct MMSI have at least one identity WITH a match in the vessel info table')
print(f'{df_mmsi_missing_vi_unmatched.mmsi.unique().shape[0]} distinct MMSI have at least one identity WITHOUT a match in the vessel info table')
print(f'{df_mmsi_missing_vi[(df_mmsi_missing_vi.mmsi.isin(df_mmsi_missing_vi_matched.mmsi.to_list())) & (df_mmsi_missing_vi.mmsi.isin(df_mmsi_missing_vi_unmatched.mmsi.to_list()))].mmsi.unique().shape[0]} distinct MMSI have at least one identity WITH a match and at least one idenity WIHTOUT a match in the vessel info table')


# -

# #### Missing MMSI with no match in the vessel info tables
# These unmatched MMSI are not fully explored. Manual exploration of a few identities shows that their n_shipname, n_callsign, and flag combination exists in the vessel info table under other MMSI but not after the one given in the vessel database. Could be that they use MMSI that are different than the ones that they are registered under.

df_mmsi_missing_vi_unmatched.head(10)

# #### Missing MMSI WITH a match in the vessel info table
#
# For these MMSI and their identities, I've pulled the yearly information used to filter MMSI for the `fishing_vessels_v` table that is used to make the public fishing effort dataset. Through this, we can see that the majority of the identities fail pass filters in every year. Only 118 of the 8117 matched identity-year pairs pass the filters. It's unsure why this is the case as the filters are quite complicated and there may be nuances that are missed here, but the vast majority fail so I think we can confidently say that these have been excluded from the pulic fishing effort for expected reasons.

df_mmsi_missing_vi_matched

# Filter down to just those that pass the (roughly approximated) filters used in the `vi_fishing_vessels.sql.j2` query
# https://github.com/GlobalFishingWatch/vessel-info/blob/develop/vessel_info_pipe/vi_fishing_vessels.sql.j2
df_mmsi_missing_vi_matched[(df_mmsi_missing_vi_matched.on_fishing_list_best)
                         & ((df_mmsi_missing_vi_matched.overlap_hours_multinames < 24) | (df_mmsi_missing_vi_matched.overlap_hours_multinames.isna())) 
                         & (df_mmsi_missing_vi_matched.offsetting == False)
                         & (df_mmsi_missing_vi_matched.num_shipnames <= 5)
                         & (df_mmsi_missing_vi_matched.fishing_hours > 24)
                         & (df_mmsi_missing_vi_matched.active_hours > 24*5)
                         & (df_mmsi_missing_vi_matched.is_bad_mmsi == False)
                         & (df_mmsi_missing_vi_matched.likely_gear == False)
                        ]

# #### How much fishing effort are we missing?
#
# The hours lost due to missing MMSI only account for an estimated 2% of the total fishing effort for vessels that are in the public data despite constituting 15% of the distinct MMSI. So these vessels are generally low activity vessels. Given that the filters for the fishing vessel list were designed to be conservative, that may be a good option to move forward with as these are the MMSI for which we have the highest confidence in the accuracy of their fishing activity and identity.

# +
q = f'''
WITH 

vessels_of_interest AS (
    SELECT 
    mmsi,
    first_timestamp,
    last_timestamp,
    FROM `{ownership_by_mmsi_table}`
),

## Get the fishing activity for each mmsi and their time ranges
## The DISTINCT is crucial as it prevent duplication of fishing activity
## When multiple identities are attached to one MMSI and have
## overlapping time ranges.
fishing AS (
    SELECT DISTINCT
    b.*
    FROM vessels_of_interest a
    LEFT JOIN `global-fishing-watch.gfw_public_data.fishing_effort_byvessel_v2` b
    ON a.mmsi = b.mmsi
    AND b.date BETWEEN DATE(a.first_timestamp) AND DATE(a.last_timestamp)
)

SELECT
mmsi,
SUM(hours) AS hours,
SUM(fishing_hours) AS fishing_hours,
min(date) as min_date,
max(date) as max_date
FROM fishing 
GROUP BY mmsi
'''

df_public_fishing_effort_by_mmsi = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_public_fishing_effort_by_mmsi

print(f'Hours from MMSI in public fishing effort data: {round(df_public_fishing_effort_by_mmsi.fishing_hours.sum()):,d}')
print(f'Hours from matched missing MMSI:               {round(df_mmsi_missing_vi_matched.fishing_hours.sum()):,d} ({df_mmsi_missing_vi_matched.fishing_hours.sum()/df_public_fishing_effort_by_mmsi.fishing_hours.sum()*100:0.2f}%)')




# ## Compare fishing hours for public dataset vs. internal pipeline

## Gets gridded fishing effort for non-overlapping MMSI with the specified ownership type and geartype.
## Default values are used to allow the user to not specify one or both of 
## the parameters, in which case the query will will not parse out effort by that attribute.
def get_gridded_fishing_effort_pipe(ownership_type=None, geartype=None):
    
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
    ##########################################################
    # EXAMPLE QUERY: FISHING EFFORT
    #
    # DESCRIPTION:
    # This query demonstrates how to extract valid positions
    # and calculate fishing hours. The key features of the
    # query include the following:

    -- 1) Identify good segments and active non-noise fishing vessels
    -- 2) Get all fishing activity for a defined period of time. Use gfw_research.pipe_vYYYYMMDD_fishing for smaller query and filter to good segments
    -- 2) Identify "fishing" positions and calculate hours using night loitering for squid jiggers, and neural net score for all other vessels. 
    -- 4) Filter positions to only the list of fishing vessels
    -- 5) Calculate fishing hours using night loitering for squid jiggers, neural net score for all other vessels
    -- 5) Bin the data to desired resolution

    WITH

      ########################################
      # This subquery identifies good segments
      good_segments AS (
      SELECT
        seg_id
      FROM
        `gfw_research.pipe_v20201001_segs`
      WHERE
        good_seg
        AND positions > 10
        AND NOT overlapping_and_short),

      ####################################################################
        ## Get the fishing vessels of interest (i.e. foreign owned, domestic owned, etc)
        ## Remove MMSI that have identities with overlapping time ranges
      fishing_vessels AS (
        SELECT 
        mmsi,
        geartype, # this geartype comes from the vessel database rather than vessel info
        first_timestamp,
        last_timestamp,
        FROM `{ownership_by_mmsi_table}`
        WHERE is_fishing
        AND {ownership_check}
        AND {geartype_check}
        AND NOT timestamp_overlap
      ),

      #####################################################################
      # This subquery fishing query gets all fishing on November 20th, 2018
      # It queries the pipe_vYYYYMMDD_fishing table, which includes only likely
      # fishing vessels. However, we are not fully confident in all vessels on
      # this list and the table also includes noisy vessels. Thus, analyses
      # often filter the pipe_vYYYYMMDD_fishing table to a refined set of vessels
      fishing AS (
      SELECT
        ssvid,
        /*
        Assign lat/lon bins at desired resolution (here 10th degree)
        FLOOR takes the smallest integer after converting to units of
        0.1 degree - e.g. 37.42 becomes 374 10th degree units
        */
        FLOOR(lat * 10) as lat_bin,
        FLOOR(lon * 10) as lon_bin,
        lat, lon,
        EXTRACT(date FROM _partitiontime) as date,
        EXTRACT(year FROM _partitiontime) as year,
        hours,
        nnet_score,
        night_loitering
      /*
      Query the pipe_vYYYYMMDD_fishing table to reduce query
      size since we are only interested in fishing vessels
      */
      FROM
        `gfw_research.pipe_v20201001_fishing`
      # Restrict query to single date
      # WHERE _partitiontime between "2020-01-01" and '2020-02-01'
      # Use good_segments subquery to only include positions from good segments
      WHERE seg_id IN (
        SELECT
          seg_id
        FROM
          good_segments)),

      ########################################################################
      # Filter fishing to just the activity between first_timestamp and last_timestamp
      # for each SSVID
      fishing_filtered AS (
      SELECT *
      FROM fishing a
      JOIN fishing_vessels b
      # Only keep positions for fishing vessels active that year
      ON a.ssvid = b.mmsi
      AND a.date BETWEEN DATE(b.first_timestamp) AND DATE(b.last_timestamp)
      ),

      ########################################################################
      # Create fishing_hours attribute. Use night_loitering instead of nnet_score as indicator of fishing for squid jiggers
      fishing_hours_filtered AS (
      SELECT *,
        CASE
          WHEN geartype = 'squid_jigger' and night_loitering = 1 THEN hours
          WHEN geartype != 'squid_jigger' and nnet_score > 0.5 THEN hours
          ELSE 0
        END
        AS fishing_hours
      FROM fishing_filtered
      ),

      #####################################################################
      # This subquery sums fishing hours over all time but keeps lat_bin
      # and lon_bin in *10 variety for pyseas visualization
      fishing_binned AS (
      SELECT
        lat_bin as lat_bin,
        lon_bin as lon_bin,
        SUM(hours) as hours,
        SUM(fishing_hours) as fishing_hours
      FROM fishing_hours_filtered
      GROUP BY lat_bin, lon_bin
      )

    #####################
    # Return fishing data
    SELECT *
    FROM fishing_binned
    '''

    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


## Gets gridded fishing effort for non-overlapping MMSI with the specified ownership type and geartype.
## Default values are used to allow the user to not specify one or both of 
## the parameters, in which case the query will will not parse out effort by that attribute.
def get_gridded_fishing_effort(ownership_type=None, geartype=None):
    
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
    fishing_of_interest AS (
        SELECT DISTINCT
        b.*
        FROM fishing_identities_of_interest a
        LEFT JOIN `{public_fishing_effort_table}` b
        ON a.mmsi = b.mmsi
        AND b.date BETWEEN DATE(a.first_timestamp) AND DATE(a.last_timestamp)
        WHERE b.fishing_hours IS NOT NULL
    ),

    ## Group by grid cells
    fishing_of_interest_gridded AS (
        SELECT
        cell_ll_lat * 10 as cell_ll_lat,
        cell_ll_lon * 10 as cell_ll_lon,
        SUM(fishing_hours) AS fishing_hours,
        FROM fishing_of_interest
        GROUP BY cell_ll_lat, cell_ll_lon
    ),

    fishing_total_gridded AS (
        SELECT
        cell_ll_lat * 10 as cell_ll_lat,
        cell_ll_lon * 10 as cell_ll_lon,
        SUM(fishing_hours) AS total_fishing_in_cell,
        SUM(fishing_hours)/(SELECT SUM(fishing_hours) FROM `{public_fishing_effort_table}`) as effort_weight,
        COUNT(mmsi) as num_mmsi,
        FROM `{public_fishing_effort_table}`
        GROUP BY cell_ll_lat, cell_ll_lon
    )

    SELECT
    cell_ll_lat,
    cell_ll_lon,
    num_mmsi,
    fishing_hours,
    total_fishing_in_cell,
    IFNULL(SAFE_DIVIDE(fishing_hours, total_fishing_in_cell), 0) AS fishing_hours_prop,
    IFNULL(SAFE_DIVIDE(fishing_hours, total_fishing_in_cell), 0) * effort_weight AS fishing_hours_prop_weighted,
    FROM fishing_of_interest_gridded 
    JOIN fishing_total_gridded USING (cell_ll_lat, cell_ll_lon) 
    WHERE cell_ll_lat IS NOT NULL and cell_ll_lon IS NOT NULL
    '''

    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# ##### Foreign

df_public_fishing_effort_foreign_pipe = get_gridded_fishing_effort_pipe('is_foreign')

df_public_fishing_effort_foreign = get_gridded_fishing_effort('is_foreign')

# ##### Domestic

df_public_fishing_effort_domestic_pipe = get_gridded_fishing_effort_pipe('is_domestic')

df_public_fishing_effort_domestic = get_gridded_fishing_effort('is_domestic')

# ##### Foreign and Domestic

df_public_fishing_effort_foreign_domestic_pipe = get_gridded_fishing_effort_pipe('is_foreign_and_domestic')

df_public_fishing_effort_foreign_domestic = get_gridded_fishing_effort('is_foreign_and_domestic')

# ##### Unknown

df_public_fishing_effort_unknown_pipe = get_gridded_fishing_effort_pipe('is_unknown')

df_public_fishing_effort_unknown = get_gridded_fishing_effort('is_unknown')

# #### Total

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

# ### Rasterize and calculate ratios

# +
grid_foreign_pipe = pyseas.maps.rasters.df2raster(df_public_fishing_effort_foreign_pipe, 'lon_bin', 'lat_bin', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_foreign = pyseas.maps.rasters.df2raster(df_public_fishing_effort_foreign, 'cell_ll_lon', 'cell_ll_lat', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_domestic_pipe = pyseas.maps.rasters.df2raster(df_public_fishing_effort_domestic_pipe, 'lon_bin', 'lat_bin', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_domestic = pyseas.maps.rasters.df2raster(df_public_fishing_effort_domestic, 'cell_ll_lon', 'cell_ll_lat', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_foreign_domestic_pipe = pyseas.maps.rasters.df2raster(df_public_fishing_effort_foreign_domestic_pipe, 'lon_bin', 'lat_bin', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_foreign_domestic = pyseas.maps.rasters.df2raster(df_public_fishing_effort_foreign_domestic, 'cell_ll_lon', 'cell_ll_lat', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_unknown_pipe = pyseas.maps.rasters.df2raster(df_public_fishing_effort_unknown_pipe, 'lon_bin', 'lat_bin', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_unknown = pyseas.maps.rasters.df2raster(df_public_fishing_effort_unknown, 'cell_ll_lon', 'cell_ll_lat', 
                                     'fishing_hours', xyscale=10, per_km2=True)
grid_total = pyseas.maps.rasters.df2raster(df_public_fishing_effort_total, 'cell_ll_lon', 'cell_ll_lat', 
                                     'total_fishing_in_cell', xyscale=10, per_km2=True)

grid_foreign_pipe_ratio = np.divide(grid_foreign_pipe, grid_total, out=np.zeros_like(grid_foreign_pipe), where=grid_total!=0)
grid_foreign_ratio = np.divide(grid_foreign, grid_total, out=np.zeros_like(grid_foreign), where=grid_total!=0)
grid_domestic_pipe_ratio = np.divide(grid_domestic_pipe, grid_total, out=np.zeros_like(grid_domestic_pipe), where=grid_total!=0)
grid_domestic_ratio = np.divide(grid_domestic, grid_total, out=np.zeros_like(grid_domestic), where=grid_total!=0)
grid_foreign_domestic_pipe_ratio = np.divide(grid_foreign_domestic_pipe, grid_total, out=np.zeros_like(grid_foreign_domestic_pipe), where=grid_total!=0)
grid_foreign_domestic_ratio = np.divide(grid_foreign_domestic, grid_total, out=np.zeros_like(grid_foreign_domestic), where=grid_total!=0)
grid_unknown_pipe_ratio = np.divide(grid_unknown_pipe, grid_total, out=np.zeros_like(grid_unknown_pipe), where=grid_total!=0)
grid_unknown_ratio = np.divide(grid_unknown, grid_total, out=np.zeros_like(grid_unknown), where=grid_total!=0)


# +
## All fishing effort with unknown ownerships (combines vessels with DB without ownership
## with all fishing effort done by MMSI not in DB or outside of active time range in DB)
grid_unknown_all = grid_total - grid_foreign - grid_domestic - grid_foreign_domestic
grid_unknown_all = np.where(grid_unknown_all < 0, 0, grid_unknown_all)
grid_unknown_all_ratio = np.divide(grid_unknown_all, grid_total, out=np.zeros_like(grid_unknown_all), where=grid_total!=0)

grid_unknown_all_pipe = grid_total - grid_foreign_pipe - grid_domestic_pipe - grid_foreign_domestic_pipe
grid_unknown_all_pipe = np.where(grid_unknown_all_pipe < 0, 0, grid_unknown_all_pipe)
grid_unknown_all_pipe_ratio = np.divide(grid_unknown_all_pipe, grid_total, out=np.zeros_like(grid_unknown_all_pipe), where=grid_total!=0)


# -
pyseas._reload()
cmap = bivariate.TransparencyBivariateColormap(pyseas.cm.misc.blue_orange)
with psm.context(psm.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    ax = psm.create_map()
    psm.add_land(ax)
    norm1 = mpcolors.LogNorm(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.01, vmax=25.2, clip=True)
    bivariate.add_bivariate_raster(grid_unknown_all_ratio, grid_total, cmap, norm1, norm2)
    cb_ax = bivariate.add_bivariate_colorbox(cmap, norm1, norm2,
                                     xlabel='fishing hours by vessels with unknown ownership\n(as fraction of total fishing hours)',
                                     ylabel='total fishing hours', fontsize=8,
                                     height=0.4, loc = (0.8, -0.45),
                                     xformat='{x:.2f}', yformat='{x:.2f}')
    plt.title("Fishing Effort by Fishing Vessels with Unknown Ownership (2012-2020)", 
              fontsize=16)
    plt.show()

cmap = bivariate.TransparencyBivariateColormap(pyseas.cm.misc.blue_orange)
with psm.context(psm.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    ax = psm.create_map()
    psm.add_land(ax)
    norm1 = mpcolors.LogNorm(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.01, vmax=25.2, clip=True)
    bivariate.add_bivariate_raster(grid_unknown_ratio, grid_total, cmap, norm1, norm2)
    cb_ax = bivariate.add_bivariate_colorbox(cmap, norm1, norm2,
                                     xlabel='fishing hours by vessels with unknown ownership\n(as fraction of total fishing hours)',
                                     ylabel='total fishing hours', fontsize=8,
                                     height=0.4, loc = (0.8, -0.45),
                                     xformat='{x:.2f}', yformat='{x:.2f}')
    plt.title("Fishing Effort by Fishing Vessels with Unknown Ownership (2012-2020)", 
              fontsize=16)
    plt.show()










# ### Comparing fishing effort between pipeline and public data

print(df_public_fishing_effort_foreign_pipe.fishing_hours.sum())
print(df_public_fishing_effort_foreign.fishing_hours.sum())
print()
print(df_public_fishing_effort_domestic_pipe.fishing_hours.sum())
print(df_public_fishing_effort_domestic.fishing_hours.sum())
print()
print(df_public_fishing_effort_foreign_domestic_pipe.fishing_hours.sum())
print(df_public_fishing_effort_foreign_domestic.fishing_hours.sum())
print()
print(df_public_fishing_effort_unknown_pipe.fishing_hours.sum())
print(df_public_fishing_effort_unknown.fishing_hours.sum())
print()

# #### Foreign

# +
# Need to cap at one because of the differences between pipeline and public data 
# where sometimes there's more fishing effort for the vessels of interest than
# the total public data effort in that cell leading to a ratio > 1
grid_foreign_pipe_ratio_capped = np.where(grid_foreign_pipe_ratio <= 1, grid_foreign_pipe_ratio, 1)
fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_foreign_pipe_ratio_capped, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners - Pipe')
    psm.add_logo(loc='lower left')
    
plt.show()


fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_foreign_ratio, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners - Public')
    psm.add_logo(loc='lower left')
    
plt.show()


# +
# Need to cap at one because of the differences between pipeline and public data 
# where sometimes there's more fishing effort for the vessels of interest than
# the total public data effort in that cell leading to a ratio > 1
grid_domestic_pipe_ratio_capped = np.where(grid_domestic_pipe_ratio <= 1, grid_domestic_pipe_ratio, 1)
fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_domestic_pipe_ratio_capped, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners - Pipe')
    psm.add_logo(loc='lower left')
    
plt.show()


fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_domestic_ratio, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners - Public')
    psm.add_logo(loc='lower left')
    
plt.show()


# +
# Need to cap at one because of the differences between pipeline and public data 
# where sometimes there's more fishing effort for the vessels of interest than
# the total public data effort in that cell leading to a ratio > 1
grid_foreign_domestic_pipe_ratio_capped = np.where(grid_foreign_domestic_pipe_ratio <= 1, grid_foreign_domestic_pipe_ratio, 1)
fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_foreign_domestic_pipe_ratio_capped, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign and Domestic Owners - Pipe')
    psm.add_logo(loc='lower left')
    
plt.show()


fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_foreign_domestic_ratio, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign and Domestic Owners - Public')
    psm.add_logo(loc='lower left')
    
plt.show()

# +
# Need to cap at one because of the differences between pipeline and public data 
# where sometimes there's more fishing effort for the vessels of interest than
# the total public data effort in that cell leading to a ratio > 1
grid_unknown_all_pipe_ratio_capped = np.where(grid_unknown_all_pipe_ratio <= 1, grid_unknown_all_pipe_ratio, 1)
fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_unknown_all_pipe_ratio_capped, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Unknown Owners - Pipe')
    psm.add_logo(loc='lower left')
    
plt.show()


fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_unknown_all_ratio, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Unknown Owners - Public')
    psm.add_logo(loc='lower left')
    
plt.show()
# -

# #### CONCLUSION: The pipe and public effort is similar enough to move forward with public
#
# Missing 15% of MMSI but only around 2% of the fishing effort
#
# We want to also use public fishing effort because it has a clear denominator. Getting the total fishing from the pipeline is difficult because each MMSI needs to be properly matched to a vessel in the vessel info table so that we know if it's a squid jigger or not. But not all MMSI can be matched cleanly and this leaves in noisy and spoofing vessels. The example query narros down the pipeline to those vessels in `fishing_vessels_ssvid_v`, but that table does not contain every MMSI in this analysis used to pull the subsets of fishing effort, so this doesn't capture the full denominator, leading to ratios far over 1.
#
# *Note: the only category with noticable spatial change when using the pipeline is the unknown_all. This is because switching to the pipeline adds in the most MMSI and activity for this category. Is this enough of a reason to figure out how to switch?*



# ## Exploring mapping of gridded domestic and foreign fishing vs. total fishing
#
# ##### Comparing different visualizations for each category
#
# 1. Raw fishing effort
# 2. Proportional fishing effort
# 3. Bivariate fishing effort (with log only on total fishing effort)
# 3. Bivariate fishing effort (with log on both axes)
#
# ##### Findings
# * Bivariate with a log scale looks to be the best for showing fishing effort by foreign-owned and unknown ownership vessels. 
# * Fishing effort by domestic-owned vessels is not log distributed, but may not be included in the paper. 
# * All vessels marked and foreign AND domestic should be reviewed to see if they can be cleaned up and classified as one or the other.

# #### Fishing Effort Distributions

# ##### FOREIGN

temp = pd.DataFrame(grid_foreign_ratio.flatten(), columns=['fishing_ratio'])
temp[temp.fishing_ratio > 0.01].fishing_ratio.hist(bins=100)
plt.show()
temp[temp.fishing_ratio > 0.01].fishing_ratio.apply(np.log10).hist(bins=100)

# ##### UNKNOWN

temp = pd.DataFrame(grid_unknown_all_ratio.flatten(), columns=['fishing_ratio'])
temp[temp.fishing_ratio > 0.01].fishing_ratio.hist(bins=100)
plt.show()
temp[temp.fishing_ratio > 0.01].fishing_ratio.apply(np.log).hist(bins=100)

# ##### DOMESTIC

temp = pd.DataFrame(grid_domestic_ratio.flatten(), columns=['fishing_ratio'])
temp[temp.fishing_ratio > 0].fishing_ratio.hist(bins=100)
plt.show()
temp[temp.fishing_ratio > 0].fishing_ratio.apply(np.log).hist(bins=100)

# ##### FOREIGN AND DOMESTIC

temp = pd.DataFrame(grid_foreign_domestic_ratio.flatten(), columns=['fishing_ratio'])
temp[temp.fishing_ratio > 0.01].fishing_ratio.hist(bins=100)
plt.show()
temp[temp.fishing_ratio > 0.01].fishing_ratio.apply(np.log).hist(bins=100)

# ##### FOREIGN

# +
fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_foreign, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners')
    psm.add_logo(loc='lower left')
    
plt.show()

fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_foreign_ratio, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners')
    psm.add_logo(loc='lower left')
    
plt.show()


pyseas._reload()
with pyseas.context(pyseas.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    #
    # Adjust the minimum/maximum cap to better display the contrast of fishing effot and ratio.
    vmin = grid_total[grid_total > 0].min() * 1000
    vmax = grid_total.max()/100
    ax, im, cb = pyseas.maps.core.plot_raster_w_colorbar_bv(
        grid_total, # total fishing effort  
        grid_foreign_ratio, # foreign fishing effort
        vmin=vmin, vmax=vmax,
        label="Ratio of fishing hours by foreign-owned vessels\n"
            "to those of all vessels broadcasting AIS\n"
            "with respect to total fishing hours\n"
            "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells",   
        projection = 'global.default')
    pyseas.maps.add_eezs(ax)
    cb.set_xlabel('Ratio of Fishing Hours by Foreign-Owned Vessels to Those by All')
    cb.set_ylabel('Total Fishing Hours')
    plt.title("Fishing Effort by Foreign-Owned Vessels", fontsize=12)

    plt.show()
    
    
pyseas._reload()
with pyseas.context(pyseas.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    #
    # Adjust the minimum/maximum cap to better display the contrast of fishing effot and ratio.
    vmin = grid_total[grid_total > 0].min() * 1000
    vmax = grid_total.max()/100
    ax, im, cb = pyseas.maps.core.plot_raster_w_colorbar_bv_log(
        grid_total, # total fishing effort  
        grid_foreign_ratio, # foreign fishing effort
        vmin=vmin, vmax=vmax,
        label="Ratio of fishing hours by foreign-owned vessels\n"
            "to those of all vessels broadcasting AIS\n"
            "with respect to total fishing hours\n"
            "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells",   
        projection = 'global.default')
    pyseas.maps.add_eezs(ax)
    cb.set_xlabel('Ratio of Fishing Hours by Foreign-Owned Vessels to Those by All')
    cb.set_ylabel('Total Fishing Hours')
    plt.title("Fishing Effort by Foreign-Owned Vessels", fontsize=12)

    plt.show()
# -

# ##### UNKNOWN

# +
fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_unknown_all, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Unknown Owners')
    psm.add_logo(loc='lower left')
    
plt.show()

fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_unknown_all_ratio, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Unknown Owners')
    psm.add_logo(loc='lower left')
    
plt.show()


pyseas._reload()
with pyseas.context(pyseas.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    #
    # Adjust the minimum/maximum cap to better display the contrast of fishing effot and ratio.
    vmin = grid_total[grid_total > 0].min() * 1000
    vmax = grid_total.max()/100
    ax, im, cb = pyseas.maps.core.plot_raster_w_colorbar_bv(
        grid_total, # total fishing effort  
        grid_unknown_all_ratio, # foreign fishing effort
        vmin=vmin, vmax=vmax,
        label="Ratio of fishing hours by unknown-owned vessels\n"
            "to those of all vessels broadcasting AIS\n"
            "with respect to total fishing hours\n"
            "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells",   
        projection = 'global.default')
    pyseas.maps.add_eezs(ax)
    cb.set_xlabel('Ratio of Fishing Hours by Unknown-Owned Vessels to Those by All')
    cb.set_ylabel('Total Fishing Hours')
    plt.title("Fishing Effort by Unknown-Owned Vessels", fontsize=12)

    plt.show()
    
    
pyseas._reload()
with pyseas.context(pyseas.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    #
    # Adjust the minimum/maximum cap to better display the contrast of fishing effot and ratio.
    vmin = grid_total[grid_total > 0].min() * 1000
    vmax = grid_total.max()/100
    ax, im, cb = pyseas.maps.core.plot_raster_w_colorbar_bv_log(
        grid_total, # total fishing effort  
        grid_unknown_all_ratio, # foreign fishing effort
        vmin=vmin, vmax=vmax,
        label="Ratio of fishing hours by unknown-owned vessels\n"
            "to those of all vessels broadcasting AIS\n"
            "with respect to total fishing hours\n"
            "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells",   
        projection = 'global.default')
    pyseas.maps.add_eezs(ax)
    cb.set_xlabel('Ratio of Fishing Hours by Unknown-Owned Vessels to Those by All')
    cb.set_ylabel('Total Fishing Hours')
    plt.title("Fishing Effort by Unknown-Owned Vessels", fontsize=12)

    plt.show()
# -





# ##### DOMESTIC

# +
fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_domestic, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners')
    psm.add_logo(loc='lower left')
    
plt.show()

fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_domestic_ratio, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners')
    psm.add_logo(loc='lower left')
    
plt.show()


pyseas._reload()
with pyseas.context(pyseas.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    #
    # Adjust the minimum/maximum cap to better display the contrast of fishing effot and ratio.
    vmin = grid_total[grid_total > 0].min() * 1000
    vmax = grid_total.max()/100
    ax, im, cb = pyseas.maps.core.plot_raster_w_colorbar_bv(
        grid_total, # total fishing effort  
        grid_domestic_ratio, # foreign fishing effort
        vmin=vmin, vmax=vmax,
        label="Ratio of fishing hours by domestic-owned vessels\n"
            "to those of all vessels broadcasting AIS\n"
            "with respect to total fishing hours\n"
            "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells",   
        projection = 'global.default')
    pyseas.maps.add_eezs(ax)
    cb.set_xlabel('Ratio of Fishing Hours by Domestic-Owned Vessels to Those by All')
    cb.set_ylabel('Total Fishing Hours')
    plt.title("Fishing Effort by Domestic-Owned Vessels", fontsize=12)

    plt.show()
    
    
pyseas._reload()
with pyseas.context(pyseas.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    #
    # Adjust the minimum/maximum cap to better display the contrast of fishing effot and ratio.
    vmin = grid_total[grid_total > 0].min() * 1000
    vmax = grid_total.max()/100
    ax, im, cb = pyseas.maps.core.plot_raster_w_colorbar_bv_log(
        grid_total, # total fishing effort  
        grid_domestic_ratio, # foreign fishing effort
        vmin=vmin, vmax=vmax,
        label="Ratio of fishing hours by domestic-owned vessels\n"
            "to those of all vessels broadcasting AIS\n"
            "with respect to total fishing hours\n"
            "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells",   
        projection = 'global.default')
    pyseas.maps.add_eezs(ax)
    cb.set_xlabel('Ratio of Fishing Hours by Domestic-Owned Vessels to Those by All')
    cb.set_ylabel('Total Fishing Hours')
    plt.title("Fishing Effort by Domestic-Owned Vessels", fontsize=12)

    plt.show()
# -

# ##### FOREIGN AND DOMESTIC

# +
fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_foreign_domestic, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Both Foreign and Domestic Owners')
    psm.add_logo(loc='lower left')
    
plt.show()

fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(grid_foreign_domestic_ratio, 
                                             r"ratio of total fishing hours",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Both Foreign and Domestic Owners')
    psm.add_logo(loc='lower left')
    
plt.show()


pyseas._reload()
with pyseas.context(pyseas.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    #
    # Adjust the minimum/maximum cap to better display the contrast of fishing effot and ratio.
    vmin = grid_total[grid_total > 0].min() * 1000
    vmax = grid_total.max()/100
    ax, im, cb = pyseas.maps.core.plot_raster_w_colorbar_bv(
        grid_total, # total fishing effort  
        grid_foreign_domestic_ratio, # foreign fishing effort
        vmin=vmin, vmax=vmax,
        label="Ratio of fishing hours by vessels with both foreign and domestic owners\n"
            "to those of all vessels broadcasting AIS\n"
            "with respect to total fishing hours\n"
            "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells",   
        projection = 'global.default')
    pyseas.maps.add_eezs(ax)
    cb.set_xlabel('Ratio of Fishing Hours by Vessels with Both Foreign and Domestic Owners to Those by All')
    cb.set_ylabel('Total Fishing Hours')
    plt.title("Fishing Effort by Vessels with Both Foreign and Domestic Owners", fontsize=12)

    plt.show()
    
    
pyseas._reload()
with pyseas.context(pyseas.styles.dark):
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='white')
    #
    # Adjust the minimum/maximum cap to better display the contrast of fishing effot and ratio.
    vmin = grid_total[grid_total > 0].min() * 1000
    vmax = grid_total.max()/100
    ax, im, cb = pyseas.maps.core.plot_raster_w_colorbar_bv_log(
        grid_total, # total fishing effort  
        grid_foreign_domestic_ratio, # foreign fishing effort
        vmin=vmin, vmax=vmax,
        label="Ratio of fishing hours by vessels with both foreign and domestic owners\n"
            "to those of all vessels broadcasting AIS\n"
            "with respect to total fishing hours\n"
            "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells",   
        projection = 'global.default')
    pyseas.maps.add_eezs(ax)
    cb.set_xlabel('Ratio of Fishing Hours by Vessels with Both Foreign and Domestic Owners to Those by All')
    cb.set_ylabel('Total Fishing Hours')
    plt.title("Fishing Effort by Vessels with Both Foreign and Domestic Owners", fontsize=12)

    plt.show()
# -




# ## Exploring hotspots of fishing by foreign-owned vessels





def get_vessels_fishing_in_eezs(eez_list, ownership_type=None, fishing_min=24):

    if not isinstance(eez_list, list):
        print("ERROR: must give eezs as a list")
        return

    if not ownership_type:
        ownership_check = 'TRUE'
    else:
        ownership_check = ownership_type

    q = f'''
    WITH

      ############################################
      # Get EEZ ID and info
      eez_names AS (
      SELECT
        CAST(eez_id AS STRING) AS eez,
        reporting_name,
        territory1_iso3 AS eez_iso3
      FROM
        `{eez_info_table}`),

      #########################################
      # Extract EEZ fishing summary from
      # activity.eez array in latest vessel info table
      eez_fishing AS (
      SELECT
        ssvid,
        best.best_flag,
        best.best_vessel_class,
        value AS eez,
        # rename the activit.eez.value field to eez
        fishing_hours
      FROM
        `{vi_table}`
      # Unnest the activity.eez array
      CROSS JOIN
        UNNEST(activity.eez)
      # Only ssvid on the best fishing list
      WHERE on_fishing_list_best),

      ############################################
      # Join country name to EEZ id
      eez_fishing_labeled AS (
      SELECT
        *
      FROM
        eez_fishing
      JOIN
        eez_names
      USING
        (eez)),

      ############################################
      # Filter to fishing in EEZs of interest using ISO3
      vessels_fishing_in_eezs AS (
      SELECT
        *
      FROM
        eez_fishing_labeled
      WHERE
        eez_iso3 IN UNNEST({eez_list})
        AND fishing_hours > {fishing_min}
      ORDER BY fishing_hours
        )

    SELECT a.*, b.fishing_hours, c.* EXCEPT(vessel_record_id, mmsi, n_shipname, n_callsign, flag)
    FROM `{owner_table}` a
    JOIN vessels_fishing_in_eezs b
    USING (ssvid)
    LEFT JOIN `{ownership_by_mmsi_table}` c
    ON a.ssvid = c.mmsi
    AND a.vessel_record_id = c.vessel_record_id
    AND a.n_shipname = c.n_shipname
    AND a.n_callsign = c.n_callsign
    AND a.flag = c.flag
    WHERE {ownership_check}
    AND (is_fishing OR is_fishing IS NULL)
    ORDER BY fishing_hours DESC
    '''

    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


def get_vessels_fishing_in_bbox(lon_start, lat_start, lon_end, lat_end, ownership_type=None, fishing_min=24):

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
        AND cell_ll_lon BETWEEN {lon_start} AND {lon_end}
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
    ORDER BY fishing_hours DESC
    '''

    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


def plot_stats_for_vessels(df):
    
    fig, (ax0, ax1, ax2) = plt.subplots(1, 3, figsize=(15, 5))

    df.flag.value_counts().plot.bar(ax=ax0)
    ax0.set_title("Registered flags")

    df.owner_flag.value_counts().plot.bar(ax=ax1)
    ax1.set_title("Owner flags")
    
    df.geartype.value_counts().plot.bar(ax=ax2)
    ax2.set_title("Geartype")

    plt.show()
    return(fig, (ax0, ax1, ax2))


# ##### South Georgia and Sandwich Islands

foreign_fishing_SGS = get_vessels_fishing_in_eezs(['SGS'], 'is_foreign')

foreign_fishing_SGS

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_SGS)

# ##### Seychelles

foreign_fishing_SYC = get_vessels_fishing_in_eezs(['SYC'], 'is_foreign')

foreign_fishing_SYC

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_SYC)

# ##### Falkland/Malvinas

foreign_fishing_FLK = get_vessels_fishing_in_eezs(['FLK'], 'is_foreign')

foreign_fishing_FLK

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_FLK)

# ##### Argentina

foreign_fishing_ARG = get_vessels_fishing_in_eezs(['ARG'], 'is_foreign')

foreign_fishing_ARG

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_ARG)

# ##### Kenya

foreign_fishing_KEN = get_vessels_fishing_in_eezs(['KEN'], 'is_foreign')

foreign_fishing_KEN

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_KEN)

# ##### Gabon

foreign_fishing_GAB = get_vessels_fishing_in_eezs(['GAB'], 'is_foreign')

foreign_fishing_GAB

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_GAB)



# + active=""
# ##### West Africa
# -

foreign_fishing_WA = get_vessels_fishing_in_bbox(-26.07, -17.92, 14.61, 9.48, 'is_foreign')

foreign_fishing_WA

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_WA)

# ##### Samoa, Tokelau, Tonga, Niue

foreign_fishing_WSM = get_vessels_fishing_in_eezs(['WSM', 'TKL', 'TON', 'NIU'], 'is_foreign')

foreign_fishing_WSM

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_WSM)



# ## Fishing Effort by EEZ

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


df_fishing_by_eez[0:20].sort_values('prop_foreign_to_domestic', ascending=False)

west_african_eezs = ['8369', '8371', '8370', '8471', '8472', '8390', '8391', '8473', '8400', \
                     '8392', '8393', '8474', '21797', '8398', '8475', '8397', '8398', '8476', \
                     '8394', '8478', '8477']
df_fishing_eez_wa = df_fishing_by_eez[df_foreign_fishing_by_eez.eez.isin(west_african_eezs)].reset_index(drop=True).sort_values('fishing_hours_foreign', ascending=False)
df_fishing_eez_wa

df_fishing_eez_wa.sort_values('prop_foreign_to_domestic', ascending=False)

# #### Mauritania (MRT)

foreign_fishing_MRT = get_vessels_fishing_in_eezs(['MRT'], 'is_foreign')

foreign_fishing_MRT

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_MRT)

# #### Guinea-Bissau (GNB)

foreign_fishing_GNB = get_vessels_fishing_in_eezs(['GNB'], 'is_foreign')

foreign_fishing_GNB

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_GNB)

# #### Gabon (GAB)

foreign_fishing_GAB = get_vessels_fishing_in_eezs(['GAB'], 'is_foreign')

foreign_fishing_GAB

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_GAB)

# #### Ivory Coast (CIV)

foreign_fishing_CIV = get_vessels_fishing_in_eezs(['CIV'], 'is_foreign')

foreign_fishing_CIV

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_CIV)

# #### Liberia (LBR)

foreign_fishing_LBR = get_vessels_fishing_in_eezs(['LBR'], 'is_foreign')

foreign_fishing_LBR

fig, (ax0, ax1, ax2) = plot_stats_for_vessels(foreign_fishing_LBR)















# ## Which geartypes are driving fishing by different ownership types?
#
# This is REALLY tricky to get for all of the unknown fishing activity as that requires matching properly to vessel info which then means talking about the vessel info tables, so I'm only going to focus on comparing foreign-owned with domestic-owned for right now.


df_ownership_by_mmsi[(df_ownership_by_mmsi.is_foreign) & (df_ownership_by_mmsi.is_fishing)].geartype.value_counts()[0:20].plot.bar()
plt.title("Geartypes of foreign-owned fishing vessels: Top 20")

# +
geartype_all = df_ownership_by_mmsi[(df_ownership_by_mmsi.is_fishing)].geartype.value_counts()
geartype_foreign = df_ownership_by_mmsi[(df_ownership_by_mmsi.is_foreign) & (df_ownership_by_mmsi.is_fishing)].geartype.value_counts()
geartype_domestic = df_ownership_by_mmsi[(df_ownership_by_mmsi.is_domestic) & (df_ownership_by_mmsi.is_fishing)].geartype.value_counts()
geartype_foreign_domestic = df_ownership_by_mmsi[(df_ownership_by_mmsi.is_foreign_and_domestic) & (df_ownership_by_mmsi.is_fishing)].geartype.value_counts()
geartype_unknown = df_ownership_by_mmsi[(df_ownership_by_mmsi.is_unknown) & (df_ownership_by_mmsi.is_fishing)].geartype.value_counts()

geartype_all.rename('count_all', inplace=True)
geartype_foreign.rename('count_foreign', inplace=True)
geartype_domestic.rename('count_domestic', inplace=True)
geartype_foreign_domestic.rename('count_foreign_domestic', inplace=True)
geartype_unknown.rename('count_unknown', inplace=True)
df_geartype_fishing = pd.concat([geartype_all, geartype_foreign, geartype_domestic, geartype_foreign_domestic, geartype_unknown], axis=1)

## Add proportions for foreign, domestic, and foreign_and_domestic
df_geartype_fishing['prop_foreign'] = df_geartype_fishing.count_foreign/df_geartype_fishing.count_all
df_geartype_fishing['prop_domestic'] = df_geartype_fishing.count_domestic/df_geartype_fishing.count_all
df_geartype_fishing['prop_foreign_domestic'] = df_geartype_fishing.count_foreign_domestic/df_geartype_fishing.count_all
df_geartype_fishing['prop_unknown'] = df_geartype_fishing.count_unknown/df_geartype_fishing.count_all

## Fill in nan with 0 (works for both count and proportion)
df_geartype_fishing.fillna(0, inplace=True)
# -

df_geartype_fishing

df_ownership_by_mmsi[(df_ownership_by_mmsi.is_foreign) & (df_ownership_by_mmsi.is_fishing)].geartype.value_counts()[0:20].plot.bar()
plt.title("Geartypes of foreign-owned fishing vessels: Top 20")

## By count
df_temp = df_geartype_fishing.sort_values('count_all', ascending=False)[['prop_foreign', 'prop_foreign_domestic', 'prop_domestic', 'prop_unknown']]
df_temp[0:20].plot(kind='bar', stacked=True)
# plt.title("Top ten flags by fishing hours")
# plt.ylabel("fishing hours")

## By count
df_temp = df_geartype_fishing.sort_values('count_all', ascending=False)[['count_foreign', 'count_foreign_domestic', 'count_domestic', 'count_unknown']]
df_temp[0:20].plot(kind='bar', stacked=True)
# plt.title("Top ten flags by fishing hours")
# plt.ylabel("fishing hours")












# # OLD CODE, NOT UPDATED FOR NEW DATASET AND BIVARIATE MAPPING
# ---
# ---
# ---







# ## Mapping fishing effort for each ownership type by geartype

# ##### Foreign Vessels by top geartypes (by number of identities attributed to the geartype)
#
# * trawlers
# * drifting_longlines and drifting_longlines|set_longlines (and set_longlines?)
# * purse_seines and tuna_purse_seines
# * squid_jigger
#

df_public_fishing_effort_foreign_trawlers = get_gridded_fishing_effort(ownership_type='is_foreign', geartype='trawlers')

df_public_fishing_effort_foreign_longlines = get_gridded_fishing_effort(ownership_type='is_foreign', geartype=['drifting_longlines', 'drifting_longlines|set_longlines', 'set_longlines'])


df_public_fishing_effort_foreign_purse_seines = get_gridded_fishing_effort(ownership_type='is_foreign', geartype=['purse_seines', 'tuna_purse_seines'])


df_public_fishing_effort_foreign_squid_jiggers = get_gridded_fishing_effort(ownership_type='is_foreign', geartype='squid_jigger')

# ##### Domestic Vessels by geartype
#
# fishing, trawlers, tuna_purse_seines, purse_seines, longlines (combined?), squid_jigger

df_public_fishing_effort_domestic_trawlers = get_gridded_fishing_effort(ownership_type='is_domestic', geartype='trawlers')

df_public_fishing_effort_domestic_longlines = get_gridded_fishing_effort(ownership_type='is_domestic', geartype=['drifting_longlines', 'drifting_longlines|set_longlines', 'set_longlines'])


df_public_fishing_effort_domestic_purse_seines = get_gridded_fishing_effort(ownership_type='is_domestic', geartype=['purse_seines', 'tuna_purse_seines'])


df_public_fishing_effort_domestic_squid_jiggers = get_gridded_fishing_effort(ownership_type='is_domestic', geartype='squid_jigger')

# #### Purse Seines

# +
raster_foreign_purse_prop= psm.rasters.df2raster(df_public_fishing_effort_foreign_purse_seines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_foreign_purse_prop[raster_foreign_purse_prop == 0] = np.nan


raster_domestic_purse_prop = psm.rasters.df2raster(df_public_fishing_effort_domestic_purse_seines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_domestic_purse_prop[raster_domestic_purse_prop == 0] = np.nan


## PLOT FOREIGN AND DOMESTIC
fig = plt.figure(figsize=(14, 14))
gs = gridspec.GridSpec(2, 1, hspace=0.3, wspace=0.02)
# with plt.rc_context(psm.styles.dark):
#     for i in range(2):
#         for j in range(2):
#             ax, im = psm.plot_raster(seismic_raster, 
#                                      subplot=gs[i, j],
#                                       projection='country.indonesia',
#                                       cmap='presence',
#                                       norm=norm,
#                                       origin='lower')
#     psm.add_colorbar(im, ax=ax, label=r"hours per $\mathregular{km^2}$", 
#              width=1.7, height=0.035, wspace=0.0025, valign=0.2)


with plt.rc_context(psm.styles.dark):
    
    ### Foreign
    ax, im = psm.plot_raster(raster_foreign_purse_prop, 
                             subplot=gs[0],
                             cmap=pyseas.cm.dark.fishing,
                             origin='lower',
                            )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nAs a Proportion of Total Fishing Effort:\nPurse Seines')
    psm.add_logo(loc='lower left')

    
    ### Domestic
    ax, im = psm.plot_raster(raster_domestic_purse_prop, 
                             subplot=gs[1],
                             cmap=pyseas.cm.dark.fishing,
                             origin='lower',
                            )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nAs a Proportion of Total Fishing Effort:\nPurse Seines')
    psm.add_logo(loc='lower left')
    
    ## Plot colorbar
    psm.add_colorbar(im, ax=ax, label=r"proportion of total fishing", 
             width=0.5, height=0.035, wspace=0.01, valign=0.5, right_edge=0.8)

# plt.savefig('./figures/fishing_effort_proportional_purse_seines.png', dpi=200, bbox_inches='tight')
plt.show()

# +
## PLOT FOREIGN AND DOMESTIC
fig = plt.figure(figsize=(14, 14))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=1)

raster_foreign_purse_prop= psm.rasters.df2raster(df_public_fishing_effort_foreign_purse_seines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_foreign_purse_prop[raster_foreign_purse_prop == 0] = np.nan


raster_domestic_purse_prop = psm.rasters.df2raster(df_public_fishing_effort_domestic_purse_seines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_domestic_purse_prop[raster_domestic_purse_prop == 0] = np.nan

with plt.rc_context(psm.styles.dark):
    ax, im = psm.plot_raster(raster_foreign_purse_prop, 
#                                  r"proportion of total fishing effort",
#                                              projection='country.indonesia',
                                 subplot=(2,1,1),
                                 cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
#                                  cbformat='%.2f',
                                 origin='lower',
#                                  loc='bottom'
                                )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nAs a Proportion of Total Fishing Effort:\nPurse Seines')
    psm.add_logo(loc='lower left')


# norm = mpcolors.LogNorm(vmin=0.1, vmax=1)

    ax, im = psm.plot_raster(raster_domestic_purse_prop, 
#                                  r"proportion of total fishing effort",
#                                              projection='country.indonesia',
                                 subplot=(2,1,2),
                                 cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
#                                  cbformat='%.1f',
                                 origin='lower',
#                                  loc='bottom'
                                )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nAs a Proportion of Total Fishing Effort:\nPurse Seines')
    psm.add_logo(loc='lower left')
    
    ## Plot colorbar
    cbar = fig.colorbar(im, 
                        ax=ax,
                        orientation='horizontal',
                        fraction=0.02,
                        aspect=60,
                        pad=0.05,
                     )

    cbar.set_label("proportion of total fishing by all MMSI (hours)")

plt.tight_layout(pad=3)
plt.savefig('./figures/fishing_effort_proportional_purse_seines.png', dpi=200, bbox_inches='tight')
plt.show()

# +

fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_foreign_purse = psm.rasters.df2raster(df_public_fishing_effort_foreign_purse_seines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours', 
                                              xyscale=10, origin='lower', per_km2=True)
raster_foreign_purse[raster_foreign_purse == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_foreign_purse, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nPurse Seines')
    psm.add_logo(loc='lower left')
plt.show()


fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_domestic_purse = psm.rasters.df2raster(df_public_fishing_effort_domestic_purse_seines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours', 
                                              xyscale=10, origin='lower', per_km2=True)
raster_domestic_purse[raster_domestic_purse == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_domestic_purse, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nPurse Seines')
    psm.add_logo(loc='lower left')
# -



# #### Drifting Longlines

# +
## PLOT FOREIGN AND DOMESTIC
fig = plt.figure(figsize=(14, 14))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=1)

raster_foreign_longlines_prop = psm.rasters.df2raster(df_public_fishing_effort_foreign_longlines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_foreign_longlines_prop[raster_foreign_longlines_prop == 0] = np.nan


raster_domestic_longlines_prop = psm.rasters.df2raster(df_public_fishing_effort_domestic_longlines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_domestic_longlines_prop[raster_domestic_longlines_prop == 0] = np.nan

with plt.rc_context(psm.styles.dark):
    ax, im = psm.plot_raster(raster_foreign_longlines_prop, 
#                                  r"proportion of total fishing effort",
#                                              projection='country.indonesia',
                                 subplot=(2,1,1),
                                 cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
#                                  cbformat='%.2f',
                                 origin='lower',
#                                  loc='bottom'
                                )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nAs a Proportion of Total Fishing Effort:\nLonglines')
    psm.add_logo(loc='lower left')


# norm = mpcolors.LogNorm(vmin=0.1, vmax=1)

    ax, im = psm.plot_raster(raster_domestic_longlines_prop, 
#                                  r"proportion of total fishing effort",
#                                              projection='country.indonesia',
                                 subplot=(2,1,2),
                                 cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
#                                  cbformat='%.1f',
                                 origin='lower',
#                                  loc='bottom'
                                )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nAs a Proportion of Total Fishing Effort:\nLonglines')
    psm.add_logo(loc='lower left')
    
    ## Plot colorbar
    cbar = fig.colorbar(im, 
                        ax=ax,
                        orientation='horizontal',
                        fraction=0.02,
                        aspect=60,
                        pad=0.05,
                     )

    cbar.set_label("proportion of total fishing by all MMSI (hours)")

plt.tight_layout(pad=3)
plt.savefig('./figures/fishing_effort_proportional_longlines.png', dpi=200, bbox_inches='tight')
plt.show()

# +

fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_foreign_longlines = psm.rasters.df2raster(df_public_fishing_effort_foreign_longlines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours', 
                                              xyscale=10, origin='lower', per_km2=True)
raster_foreign_longlines[raster_foreign_longlines == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_foreign_longlines, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nLonglines')
    psm.add_logo(loc='lower left')
plt.show()


fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_domestic_longlines = psm.rasters.df2raster(df_public_fishing_effort_domestic_longlines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours', 
                                              xyscale=10, origin='lower', per_km2=True)
raster_domestic_longlines[raster_domestic_longlines == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_domestic_longlines, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nLonglines')
    psm.add_logo(loc='lower left')
# -



# #### Squid Jiggers

# +
## PLOT FOREIGN AND DOMESTIC
fig = plt.figure(figsize=(14, 14))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=1)

raster_foreign_squid_prop = psm.rasters.df2raster(df_public_fishing_effort_foreign_squid_jiggers, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_foreign_squid_prop[raster_foreign_squid_prop == 0] = np.nan


raster_domestic_squid_prop = psm.rasters.df2raster(df_public_fishing_effort_domestic_squid_jiggers, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_domestic_squid_prop[raster_domestic_squid_prop == 0] = np.nan

with plt.rc_context(psm.styles.dark):
    ax, im = psm.plot_raster(raster_foreign_squid_prop, 
#                                  r"proportion of total fishing effort",
#                                              projection='country.indonesia',
                                 subplot=(2,1,1),
                                 cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
#                                  cbformat='%.2f',
                                 origin='lower',
#                                  loc='bottom'
                                )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nAs a Proportion of Total Fishing Effort:\nSquid Jiggers')
    psm.add_logo(loc='lower left')


# norm = mpcolors.LogNorm(vmin=0.1, vmax=1)

    ax, im = psm.plot_raster(raster_domestic_squid_prop, 
#                                  r"proportion of total fishing effort",
#                                              projection='country.indonesia',
                                 subplot=(2,1,2),
                                 cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
#                                  cbformat='%.1f',
                                 origin='lower',
#                                  loc='bottom'
                                )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nAs a Proportion of Total Fishing Effort:\nSquid Jiggers')
    psm.add_logo(loc='lower left')
    
    ## Plot colorbar
    cbar = fig.colorbar(im, 
                        ax=ax,
                        orientation='horizontal',
                        fraction=0.02,
                        aspect=60,
                        pad=0.05,
                     )

    cbar.set_label("proportion of total fishing by all MMSI (hours)")

plt.tight_layout(pad=3)
plt.savefig('./figures/fishing_effort_proportional_squid_jiggers.png', dpi=200, bbox_inches='tight')
plt.show()

# +

fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_foreign_longlines = psm.rasters.df2raster(df_public_fishing_effort_foreign_longlines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours', 
                                              xyscale=10, origin='lower', per_km2=True)
raster_foreign_longlines[raster_foreign_longlines == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_foreign_longlines, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nDrifting Longlines')
    psm.add_logo(loc='lower left')
plt.show()


fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_domestic_longlines = psm.rasters.df2raster(df_public_fishing_effort_domestic_longlines, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours', 
                                              xyscale=10, origin='lower', per_km2=True)
raster_domestic_longlines[raster_domestic_longlines == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_domestic_longlines, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nDrifting Longlines')
    psm.add_logo(loc='lower left')
# -



# #### Trawlers

# +

fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_foreign_trawlers = psm.rasters.df2raster(df_public_fishing_effort_foreign_trawlers, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_foreign_trawlers[raster_foreign_trawlers == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_foreign_trawlers, 
                                             r"proportion of total fishing effort",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nAs a Proportion of Total Fishing Effort:\nTrawlers')
    psm.add_logo(loc='lower left')
plt.show()


fig = plt.figure(figsize=(14, 7))
# norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_domestic_trawlers = psm.rasters.df2raster(df_public_fishing_effort_foreign_trawlers, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours_prop', 
                                              xyscale=10, origin='lower', per_km2=False)
raster_domestic_trawlers[raster_domestic_trawlers == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_domestic_trawlers, 
                                             r"proportion of total fishing effort",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
#                                              norm=norm,
                                             cbformat='%.1ddf',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nAs a Proportion of Total Fishing Effort:\nTrawlers')
    psm.add_logo(loc='lower left')

# +

fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_foreign_trawlers = psm.rasters.df2raster(df_public_fishing_effort_foreign_trawlers, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours', 
                                              xyscale=10, origin='lower', per_km2=True)
raster_foreign_trawlers[raster_foreign_trawlers == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_foreign_trawlers, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Foreign Owners\nTrawlers')
    psm.add_logo(loc='lower left')
plt.show()


fig = plt.figure(figsize=(14, 7))
norm = mpcolors.LogNorm(vmin=0.1, vmax=10)
raster_domestic_trawlers = psm.rasters.df2raster(df_public_fishing_effort_foreign_trawlers, 'cell_ll_lon', 'cell_ll_lat', 'fishing_hours', 
                                              xyscale=10, origin='lower', per_km2=True)
raster_domestic_trawlers[raster_domestic_trawlers == 0] = np.nan
with plt.rc_context(psm.styles.dark):
    ax, im, cb = psm.plot_raster_w_colorbar(raster_domestic_trawlers, 
                                             r"fishing hours per $\mathregular{km^2}$ ",
#                                              projection='country.indonesia',
                                             cmap=pyseas.cm.dark.fishing,
                                             norm=norm,
                                             cbformat='%.0f',
                                             origin='lower',
                                             loc='bottom')
    psm.add_countries()
    psm.add_eezs()
    ax.set_title('Fishing Effort by Vessels Registered with Domestic Owners\nTrawlers')
    psm.add_logo(loc='lower left')
# -



