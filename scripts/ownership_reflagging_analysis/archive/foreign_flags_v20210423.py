# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.5.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from operator import add

import pyseas
pyseas._reload()
import pyseas.maps as psm

# Set plot parameters and styling
psm.use(psm.styles.chart_style)
plt.rcParams['figure.figsize'] = (10, 6)

# Show all dataframe rows
pd.set_option("max_rows", 20)
# -

# # Other Questions:
#
# Why do these vessels not collapse so that we have ownership for the match? And why don't these just already match to both identities? 
#
# `SELECT *
# FROM world-fishing-827.vessel_database.all_vessels_v20210401
# WHERE identity.ssvid = '367535260'`
#
# It actually looks like there are a ton of USA vessels like the above without ownership information because of how they match with USA and USA2 lists
#
# `SELECT *
# FROM world-fishing-827.scratch_jaeyoon.vdatabase_release_sample_identity_v20210411
# WHERE vessel_record_id NOT IN (SELECT vessel_record_id FROM world-fishing-827.scratch_jaeyoon.vdatabase_release_sample_owner_v20210411)`
#







# # Visualization code

# Parameters used for visualizations
color_dark_pink = '#d73b68'
color_gray = '#b2b2b2'
color_dark_blue = '#204280'

# # Get the data

# ## Vessel flag and owner flag
#
# #### We then calculate metrics for ownership based on if an identity is flagged to the same flag state (domestic), a different flag state (foreign), or if the owners flags state is not known (unknown).
#
# Note: There are likely some instances of territories being counted as foreign flags.
#
# FUTURE WORK?: rerun the ownership table to include all identities regardless of if they have ownership information and just set those fields to NULL. Makes it a lot easier to make sure that unowned identities are being properly accounted for.
#
# #### NOTE: There are 14 rows removed by `WHERE flag != 'null'`. Do we want to include then in some way?
#
# *Are these being joined correctly? Are they being deduplicated correctly? The DISTINCT in the final query was designed so that a combination of identity (as defined by the combination of vessel_record_id, ssvid, n_shipname, n_callsign, imo, and flag) and ownership flag can only happen once. (See output of identities_joined to QA this)*

identity_table = 'world-fishing-827.scratch_jaeyoon.vdatabase_release_sample_identity_v20210411'
owner_table = 'world-fishing-827.scratch_jaeyoon.vdatabase_release_sample_owner_v20210411'


# +
### ORIGINAL QUERY
### Only counts distinct combinations of vessel_record_id, flag, owner_flag 
# q = f'''
# WITH 
# all_identities AS (
#     SELECT 
#     vessel_record_id,
#     ssvid,
#     IFNULL(n_shipname, 'null') as n_shipname,
#     IFNULL(n_callsign, 'null') as n_callsign,
#     IFNULL(imo, 'null') as imo,
#     IFNULL(flag, 'null') as flag,
#     FROM `{identity_table}`
# ),

# identities_with_ownership AS (
#     SELECT 
#     vessel_record_id,
#     ssvid,
#     IFNULL(n_shipname, 'null') as n_shipname,
#     IFNULL(n_callsign, 'null') as n_callsign,
#     IFNULL(imo, 'null') as imo,
#     IFNULL(flag, 'null') as flag,
#     owner_flag
#     FROM `{owner_table}`
# ),

# identities_joined AS (
#     SELECT DISTINCT
#     vessel_record_id,
#     flag,
#     owner_flag
#     FROM all_identities 
#     LEFT JOIN identities_with_ownership 
#     USING(vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag)
#     WHERE flag != 'null'
# ),

# identities_count AS (
#     SELECT
#     flag,
#     SUM(IF((owner_flag IS NOT NULL) AND (flag = owner_flag), 1, 0)) as count_domestic,
#     SUM(IF((owner_flag IS NOT NULL) AND (flag != owner_flag), 1, 0)) as count_foreign,
#     SUM(IF(owner_flag IS NULL, 1, 0)) as count_unknown,
#     COUNT(*) as count_total
#     FROM identities_joined 
#     GROUP BY flag
# )

# SELECT
# *,
# count_domestic/count_total AS prop_domestic,
# count_foreign/count_total AS prop_foreign,
# count_unknown/count_total AS prop_unknown,
# FROM identities_count
# ORDER BY count_total DESC
# '''

# df_ownership_by_flag = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# +
### This query counts distinct combinations of identity (minus SSVID), owner, and owner flag.
### Identities are deduplicated across SSVID because the same identity, owner, and owner flag combo
### is often associated with multiple SSVID across time which add unmeaningful duplicates.
q = f'''
WITH 
all_identities AS (
    SELECT 
    vessel_record_id,
    ssvid,
    IFNULL(n_shipname, 'null') as n_shipname,
    IFNULL(n_callsign, 'null') as n_callsign,
    IFNULL(imo, 'null') as imo,
    IFNULL(flag, 'null') as flag,
    FROM `{identity_table}`
),

identities_with_ownership AS (
    SELECT 
    vessel_record_id,
    ssvid,
    IFNULL(n_shipname, 'null') as n_shipname,
    IFNULL(n_callsign, 'null') as n_callsign,
    IFNULL(imo, 'null') as imo,
    IFNULL(owner, 'null') as owner,
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
    imo,
    owner,
    flag,
    owner_flag
    FROM all_identities 
    LEFT JOIN identities_with_ownership 
    USING(vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag)
    WHERE flag != 'null'
    ORDER BY n_shipname, n_callsign, imo, ssvid, owner, flag, owner_flag
),

identities_deduped AS (
    SELECT DISTINCT
    vessel_record_id, n_shipname, n_callsign, imo, owner, flag, owner_flag,
    FROM identities_joined
),

identities_count AS (
    SELECT
    flag,
    SUM(IF((owner_flag IS NOT NULL) AND (flag = owner_flag), 1, 0)) as count_domestic,
    SUM(IF((owner_flag IS NOT NULL) AND (flag != owner_flag), 1, 0)) as count_foreign,
    SUM(IF(owner_flag IS NULL, 1, 0)) as count_unknown,
    COUNT(*) as count_total
    FROM identities_deduped 
    GROUP BY flag
)

SELECT
*,
count_domestic/count_total AS prop_domestic,
count_foreign/count_total AS prop_foreign,
count_unknown/count_total AS prop_unknown,
FROM identities_count
ORDER BY count_total DESC
'''

df_ownership_by_flag = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -
df_ownership_by_flag

# ## Reflagging flags
#
# Creates a table that maps each flag to the number of times that a flag was involved in a reflagging. This query only considers flag_in because the table was built to ONLY include vessels that reflagged at least once and it include the terminal flag (the flag that the vessel is last known to have been flying) with flag_in as that flag and flag_out as NULL. Therefore, flag_out never needs to be considered.
#
# Also, there are a few hundred rows where flag_in == flag_out because a different identity change happened that didn't involve the flag. These rows have been removed for now after discussion with Jaeyoon.
#
# These values are then normalized by the number of unique SSVID using the number of identities associated with a flag.
#
# *Does this metric make sense to normalize the number of reflaggings? Should we instead count the number of unique MMSI or unique hulls?*

reflagging_table = 'world-fishing-827.scratch_jaeyoon.reflagging_flag_in_out_v20210422'


q = f'''
WITH 

reflagging_by_flag AS (
SELECT 
flag_in as flag,
count(*) as num_reflag
FROM `{reflagging_table}`
WHERE flag_in IS NOT NULL
AND (flag_in != flag_out OR flag_out IS NULL)
GROUP BY flag_in
),

identities_by_flag AS (
SELECT
flag,
COUNT(*) as num_identities
FROM `{identity_table}`
GROUP BY flag
)

SELECT 
*,
num_reflag/num_identities AS norm_reflag
FROM reflagging_by_flag 
LEFT JOIN identities_by_flag
USING(flag)
ORDER BY norm_reflag DESC
'''
print(q)
# df_reflagging_flags = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
df_reflagging_flags[df_reflagging_flags.flag == 'SLE']

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
df_flag_data['is_foc'] = np.where(df_flag_data.flag.isin(focs), True, False)
df_flag_data

# # Analysis

# ## Figure 1: Do the Top 20 reflagging countries tend to have more foreign ownership than less frequently reflagged countries?

# This analysis was originally done using the top 20 flags by the raw number of reflagging events for each flag. However, this was heavily weighted towards major flag states with a lot of vessels so I normalized by the number of identities flagged to each flag state.
#
# Once sorted by normalized reflagging numbers, there are a lot of flags with very few reflagging instances and overall small fleet sizes that are not high impact. In order to focus on larger fleets where insight and management changes would have the most impact, I filtered the flags down to only those in the top 50% (num_identities >= 40) by number of unique identities registered to a flag from 2012-2020. I then use this set of vessels throughout the final analysis.
#
# *Does this make sense? Does it seem too finicky or cherry picked?*

# Filtering out flags with no reflagging
df_flag_data[df_flag_data.num_reflag > 0].num_identities.describe()

df_flag_data[df_flag_data.num_reflag > 0].num_identities.hist(bins=100)

# ### Filter down to flag states that were in the top 50% by number of identities using their flag at some point in time. 

df_flag_data_filtered = df_flag_data[df_flag_data.num_identities >= 40].sort_values('norm_reflag', ascending=False).reset_index(drop=True)
print(df_flag_data_filtered.shape[0])
df_flag_data_filtered

# ### The top 20 by normalized reflagging are as follows:

df_top20_flags = df_flag_data_filtered[0:20]
df_other_flags = df_flag_data_filtered[20:]
df_top20_flags

# ### The top 20 by foreign ownership ratio are as follows:

df_top20_foreign_ownership = df_flag_data_filtered.sort_values("prop_foreign", ascending=False)[0:20]
df_other_foreign_ownership = df_flag_data_filtered.sort_values("prop_foreign", ascending=False)[20:]
df_top20_foreign_ownership

# ## Plot the average ownership types for the Top 20 flag states by normalized reflagging versus all other flags AND the normalized reflagging for the Top 20 flags by foreign ownership vs. all other flags (both in the top 50% by size)

# +
df_other_flags = df_flag_data_filtered[20:]

prop_domestic_owner = [df_top20_flags.prop_domestic.mean(),
                        df_other_flags.prop_domestic.mean()]
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
p3 = ax0.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax0.set_xticks(indices)
ax0.set_xticklabels(x_labels, fontsize=16)
ax0.set_ylabel("Mean ownership ratio", fontsize=16)
ax0.set_title("Mean ownership ratio by ownership type\nof top 20 most reflagged flags vs other flags\n(for top 50% of flags by number of identities)")
ax0.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)


### Top 20 flags by foreign ownership ratio
norm_reflagging = [df_top20_foreign_ownership.norm_reflag.mean(),
                   df_other_foreign_ownership.norm_reflag.mean()]

x_labels = ['Top 20 by \nForeign Ownership', 'All Other Flags']
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax1.bar(indices, norm_reflagging, width, color=color_dark_blue)
# p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
# p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax1.set_xticks(indices)
ax1.set_xticklabels(x_labels, fontsize=16)
ax1.set_ylabel("Mean normalized reflagging", fontsize=16)
ax1.set_title("Mean normalized reflagging by top 20 flags\nwith most foreign ownership vs other flags\n(for top 50% of flags by number of identities)")
# ax1.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)

### Global layout settings
plt.yticks(fontsize=14)
plt.tight_layout()
# plt.savefig('./figures/top20_versus_others.png')
plt.show()
# +
df_other_flags = df_flag_data_filtered[20:]

prop_domestic_owner = [df_top20_flags.prop_domestic.mean(),
                        df_other_flags.prop_domestic.mean()]
prop_foreign_owner = [df_top20_flags.prop_foreign.mean(),
                       df_other_flags.prop_foreign.mean()]
prop_unknown_owner = [df_top20_flags.prop_unknown.mean(),
                       df_other_flags.prop_unknown.mean()]

x_labels = ['Top 20\nReflagged Flags', 'All Other Flags']
fig, ax = plt.subplots(figsize=(6, 8))
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax.bar(indices, prop_domestic_owner, width, label="Vessel registered to\nsame country as owner", color=color_dark_blue)
p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(x_labels, fontsize=16)
ax.set_ylabel("Mean ownership ratio", fontsize=16)
plt.yticks(fontsize=14)
plt.title("Mean ownership ratio by ownership type\nof top 20 most reflagged flags vs other flags\n(for top 50% of flags by number of identities)")
plt.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)
plt.tight_layout()
plt.savefig('./figures/ownership_top20_reflagged_vs_others.png')
plt.show()
# +
norm_reflagging = [df_top20_foreign_ownership.norm_reflag.mean(),
                   df_other_foreign_ownership.norm_reflag.mean()]

x_labels = ['Top 20 by \nForeign Ownership', 'All Other Flags']
fig, ax = plt.subplots(figsize=(6, 8))
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax.bar(indices, norm_reflagging, width, color=color_dark_blue)
# p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
# p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(x_labels, fontsize=16)
ax.set_ylabel("Mean normalized reflagging", fontsize=16)
plt.yticks(fontsize=14)
plt.title("Mean normalized reflagging by top 20 flags\nwith most foreign ownership vs other flags\n(for top 50% of flags by number of identities)")
# plt.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)
plt.tight_layout()
plt.savefig('./figures/ownership_top20_foreign_vs_others.png')
plt.show()
# -
print("Domestic:", prop_domestic_owner)
print("Foreign:", prop_foreign_owner)
print("Unknown:", prop_unknown_owner)

print("Domestic:", prop_domestic_owner)
print("Foreign:", prop_foreign_owner)
print("Unknown:", prop_unknown_owner)

# ##### Stats for the identity paper

print(f"There are {prop_foreign_owner[0]/prop_domestic_owner[0]} times more foreign owned vessels than domestic ones")
print(f"The Top 20 most reflagged flags have a foreign ownership ratio that is {prop_foreign_owner[0]/prop_foreign_owner[1]} times more than the group of all other flags, ({prop_foreign_owner[0]*100:0.1f}% vs {prop_foreign_owner[1]*100:0.1f}%). It's greater than foreign and unknown ownership combined for all other states ({(prop_foreign_owner[1]+prop_unknown_owner[1])*100:0.1f}%).")


# ### Supplementary Materials for Figure 1

# #### What are the top 20 countries by normalized reflagging?
#
# TODO: Expand to include full name, number of reflaggings, number of total flagged vessels, and normalized reflaggings
#
# Medium article with how to style table: https://towardsdatascience.com/simple-little-tables-with-matplotlib-9780ef5d0bc4

# +
title_text = 'Top 20 Most Reflagged Flags'
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
title_text = 'Top 20 Flags by Foreign Ownership Ratio'
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
p2 = ax.bar(indices, temp.prop_unknown, width, bottom=temp.prop_domestic+temp.prop_foreign, label="No information\navailable on owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90, fontsize=14)
plt.yticks(fontsize=14)
ax.set_ylabel("Proportion of vessels", fontsize=16)
plt.title("Domestic vs. foreign ownership flags\nfor top 20 most reflagged flags\n(for top 50% of flags by number of identities)")
plt.legend(loc='upper left', bbox_to_anchor=[0.08,-0.18], ncol=2)
plt.tight_layout()
plt.savefig('./figures/ownership_top20_reflagged_flags.png')
plt.show()

# ## Figure 2: Do the Top 20 countries having the most foreign owned vessels registered to other countries tend to reflag more than other countries?

# ### Linear regression analysis and FOCs

# ##### Fit a linear regression model

# +
from sklearn.linear_model import LinearRegression
x_for = np.asarray(df_flag_data_filtered.norm_reflag.to_list()).reshape((-1,1))
y_for = np.array(df_flag_data_filtered.prop_foreign.to_list())

model_for = LinearRegression().fit(x_for, y_for)
r_sq_for = model_for.score(x_for, y_for)
m_for = model_for.coef_[0]
b_for = model_for.intercept_
print(r_sq_for)
# -

num_foc = df_flag_data_filtered[df_flag_data_filtered.is_foc].shape[0]
num_bounding = df_flag_data_filtered[(df_flag_data_filtered.prop_foreign >= 0.15) & (df_flag_data_filtered.norm_reflag >= 0.30)].shape[0]
num_bounding_foc = df_flag_data_filtered[(df_flag_data_filtered.prop_foreign >= 0.15) & (df_flag_data_filtered.norm_reflag >= 0.30) & (df_flag_data_filtered.is_foc)].shape[0]
num_bounding_top20 = df_flag_data_filtered[(df_flag_data_filtered.prop_foreign >= 0.15) & (df_flag_data_filtered.norm_reflag >= 0.30) & (df_flag_data_filtered.flag.isin(df_top20_flags.flag.to_list()))].shape[0]
print(f"Flags with >=15% foreign ownership and >=30% of fleet reflagging: {num_bounding} ({num_bounding/df_flag_data_filtered.shape[0]*100:0.2f}% of top 50% of fleet)")
print(f"Flags with >=15% foreign ownership and >=30% of fleet reflagging AND suspected FOC: {num_bounding_foc} ({num_bounding_foc/num_bounding*100:0.2f}% of flags above these thresholds, {num_bounding_foc/num_foc*100:0.2f}% of all FOC)")
print(f"Flags with >=15% foreign ownership and >=30% of fleet reflagging AND top 20 flag: {num_bounding_top20} ({num_bounding_top20/num_bounding*100:0.2f}% of flags above these thresholds, {num_bounding_top20/20*100:0.2f}% of all top 20 flags)")
print(f"Flags that are suspected FOC: {num_foc}")


# #### TODO:
# * Mark Top 20 somehow
# * Separate out Ford and Wilcox with different color

# +
ax = df_flag_data_filtered[df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'prop_foreign', c=color_dark_pink, s=40, figsize=(10, 6), label='Suspected FOC', zorder=2)
df_flag_data_filtered[~df_flag_data_filtered.is_foc].plot.scatter('norm_reflag', 'prop_foreign', c=color_dark_blue, s=20, ax=ax, label='All other flags', zorder=2)
for i, row in df_flag_data_filtered.iterrows():
    ax.annotate(row.flag, xy=(row.norm_reflag, row.prop_foreign), zorder=2)

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
plt.savefig('./figures/regression_ownership_reflagging_foreign.png')
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
# # ORIGINAL ANALYSIS BELOW
# Note: some of this may be broken if you try to rerun
#
# ---

# # Answer the following questions:
#
# * Are Top X reflagging countries tend to have their owners’ flag different from the vessels’ flag more than the rest of countries?
# * Likewise, Top X countries having the most vessels whose owners are registered to other countries tend to reflag more than the rest of countries?

df_top20_flags_raw = df_reflagging_flags[0:20]
df_top20_flags_raw

flags = df_vessels.flag.unique().tolist()
count_domestic_owner = [df_vessels[(df_vessels.flag == flag) & (df_vessels.owner_flag.notna()) & (df_vessels.flag == df_vessels.owner_flag)].shape[0] for flag in flags]
count_foreign_owner = [df_vessels[(df_vessels.flag == flag) & (df_vessels.owner_flag.notna()) & (df_vessels.flag != df_vessels.owner_flag)].shape[0] for flag in flags]
count_unknown_owner = [df_vessels[(df_vessels.flag == flag) & (df_vessels.owner_flag.isna())].shape[0] for flag in flags]
df_ownership_by_flag = pd.DataFrame({'flag': flags, 'count_domestic': count_domestic_owner, 'count_foreign': count_foreign_owner, 'count_unknown': count_unknown_owner})
df_ownership_by_flag['prop_domestic'] = df_ownership_by_flag.count_domestic/(df_ownership_by_flag.count_domestic+df_ownership_by_flag.count_foreign+df_ownership_by_flag.count_unknown)
df_ownership_by_flag['prop_foreign'] = df_ownership_by_flag.count_foreign/(df_ownership_by_flag.count_domestic+df_ownership_by_flag.count_foreign+df_ownership_by_flag.count_unknown)
df_ownership_by_flag['prop_unknown'] = df_ownership_by_flag.count_unknown/(df_ownership_by_flag.count_domestic+df_ownership_by_flag.count_foreign+df_ownership_by_flag.count_unknown)
df_ownership_by_flag['total'] = df_ownership_by_flag.count_domestic+df_ownership_by_flag.count_foreign+df_ownership_by_flag.count_unknown
df_ownership_by_flag = df_ownership_by_flag.sort_values('prop_foreign', ascending=False).set_index('flag')
df_ownership_by_flag


# +
ownership_by_top20 = df_ownership_by_flag.loc[df_ownership_by_flag.index.intersection(df_top20_flags_raw.flag.to_list())]
ownership_by_all_others = df_ownership_by_flag.drop(df_ownership_by_flag.index.intersection(df_top20_flags_raw.flag.to_list()))

prop_domestic_owner = [ownership_by_top20.count_domestic.sum()/ownership_by_top20.total.sum(),
                        ownership_by_all_others.count_domestic.sum()/ownership_by_all_others.total.sum()]
prop_foreign_owner = [ownership_by_top20.count_foreign.sum()/ownership_by_top20.total.sum(),
                       ownership_by_all_others.count_foreign.sum()/ownership_by_all_others.total.sum()]
prop_unknown_owner = [ownership_by_top20.count_unknown.sum()/ownership_by_top20.total.sum(),
                       ownership_by_all_others.count_unknown.sum()/ownership_by_all_others.total.sum()]

x_labels = ['Top 20 Reflagged', 'All Other Flags']
fig, ax = plt.subplots()
indices = np.arange(len(x_labels))
width = 0.5
p1 = ax.bar(indices, prop_domestic_owner, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Foreign owner", color=color_dark_pink)
p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(x_labels)
ax.set_ylabel("Proportion of vessels")
plt.title("Ownership for top 20 most reflagged flags and all other flags")
plt.legend()
plt.show()
# -

# ### Remove a few key major flags from the top 20 like Russia

flags_to_remove = ['RUS', 'NOR', 'ISL', 'GBR', 'DNK', 'ESP', 'KOR', 'JPN', 'CHN']
temp = df_reflagging_flags[~df_reflagging_flags.flag.isin(flags_to_remove)]
df_ownership_by_flag_temp = df_ownership_by_flag[~df_ownership_by_flag.index.isin(flags_to_remove)]
df_top20_flags_temp = temp[0:20]
df_top20_flags_temp

# +
ownership_by_top20 = df_ownership_by_flag_temp.loc[df_ownership_by_flag_temp.index.intersection(df_top20_flags_temp.flag.to_list())]
ownership_by_all_others = df_ownership_by_flag_temp.drop(df_ownership_by_flag_temp.index.intersection(df_top20_flags_temp.flag.to_list()))

prop_domestic_owner = [ownership_by_top20.count_domestic.sum()/ownership_by_top20.total.sum(),
                        ownership_by_all_others.count_domestic.sum()/ownership_by_all_others.total.sum()]
prop_foreign_owner = [ownership_by_top20.count_foreign.sum()/ownership_by_top20.total.sum(),
                       ownership_by_all_others.count_foreign.sum()/ownership_by_all_others.total.sum()]
prop_unknown_owner = [ownership_by_top20.count_unknown.sum()/ownership_by_top20.total.sum(),
                       ownership_by_all_others.count_unknown.sum()/ownership_by_all_others.total.sum()]


x_labels = ['Top 20 Reflagged', 'All Other Flags']
fig, ax = plt.subplots()
indices = np.arange(len(x_labels))
width = 0.5
p1 = ax.bar(indices, prop_domestic_owner, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Foreign owner", color=color_dark_pink)
p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(x_labels)
ax.set_ylabel("Proportion of vessels")
plt.title("Ownership for top 20 most reflagged flags and all other flags")
plt.legend()
plt.show()
# -



# #### Divide out by the FOCs listed in this article https://www.itfglobal.org/en/sector/seafarers/flags-of-convenience
#
# Notably absent: KNA, SLE, KIR

focs = ['ATG', 'BHS', 'BRB', 'BLZ', 'BMU', 'KHM', 'CYM', 'COM', 'CYP', 'GNQ', 'FRO', 'GEO', \
        'GIB', 'HND', 'JAM', 'LBN', 'LBR', 'MLT', 'PMD', 'MHL', 'MUS', 'MDA', 'MNG', 'MMR', \
        'ANT', 'PRK', 'PAN', 'STP', 'VCT', 'LKA', 'TON', 'VUT']

# +
ownership_by_foc = df_ownership_by_flag.loc[df_ownership_by_flag.index.intersection(focs)]
ownership_by_nonfoc = df_ownership_by_flag.drop(df_ownership_by_flag.index.intersection(focs))

prop_domestic_owner = [ownership_by_foc.count_domestic.sum()/ownership_by_foc.total.sum(),
                       ownership_by_nonfoc.count_domestic.sum()/ownership_by_nonfoc.total.sum()]
prop_foreign_owner = [ownership_by_foc.count_foreign.sum()/ownership_by_foc.total.sum(),
                      ownership_by_nonfoc.count_foreign.sum()/ownership_by_nonfoc.total.sum()]
prop_unknown_owner = [ownership_by_foc.count_unknown.sum()/ownership_by_foc.total.sum(),
                      ownership_by_nonfoc.count_unknown.sum()/ownership_by_nonfoc.total.sum()]


x_labels = ['Top 20 Reflagged', 'All Other Flags']
fig, ax = plt.subplots()
indices = np.arange(len(x_labels))
width = 0.5
p1 = ax.bar(indices, prop_domestic_owner, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Foreign owner", color=color_dark_pink)
p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(x_labels)
ax.set_ylabel("Proportion of vessels")
plt.title("Ownership for FOCs (based on ITF list) and all other flags")
plt.legend()
plt.show()
# -



# ### Normalize the reflagging by the number of unique SSVID that have ever used the flag and THEN plot top 20

df_top20_flags = df_reflagging_flags[0:20]
df_top20_flags

# +
ownership_by_top20 = df_ownership_by_flag.loc[df_ownership_by_flag.index.intersection(df_top20_flags.flag.to_list())]
ownership_by_all_others = df_ownership_by_flag.drop(df_ownership_by_flag.index.intersection(df_top20_flags.flag.to_list()))

prop_domestic_owner = [ownership_by_top20.count_domestic.sum()/ownership_by_top20.total.sum(),
                        ownership_by_all_others.count_domestic.sum()/ownership_by_all_others.total.sum()]
prop_foreign_owner = [ownership_by_top20.count_foreign.sum()/ownership_by_top20.total.sum(),
                       ownership_by_all_others.count_foreign.sum()/ownership_by_all_others.total.sum()]
prop_unknown_owner = [ownership_by_top20.count_unknown.sum()/ownership_by_top20.total.sum(),
                       ownership_by_all_others.count_unknown.sum()/ownership_by_all_others.total.sum()]

x_labels = ['Top 20 Reflagged', 'All Other Flags']
fig, ax = plt.subplots()
indices = np.arange(len(x_labels))
width = 0.5
p1 = ax.bar(indices, prop_domestic_owner, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Foreign owner", color=color_dark_pink)
p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(x_labels)
ax.set_ylabel("Proportion of vessels")
plt.title("Ownership for top 20 most reflagged flags and all other flags\nby normalized reflaggings (unique SSVID > 5)")
plt.legend()
plt.show()
# -



# # Trying other ways of investigating completely

temp = df_ownership_by_flag.loc[df_top20_flags.flag.to_list()]
flags = temp.index.to_list()
fig, ax = plt.subplots()
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.count_domestic, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, temp.count_foreign, width, bottom=temp.count_domestic, label="Foreign owner", color=color_dark_pink)
p2 = ax.bar(indices, temp.count_unknown, width, bottom=temp.count_domestic+temp.count_foreign, label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
ax.set_ylabel("Number of vessels")
plt.title("Domestic vs. foreign ownership flags\nfor top 20 most reflagged flags")
plt.legend()
plt.show()

temp = df_ownership_by_flag.loc[df_top20_flags.flag.to_list()]
flags = temp.index.to_list()
fig, ax = plt.subplots()
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.prop_domestic, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, temp.prop_foreign, width, bottom=temp.prop_domestic, label="Foreign owner", color=color_dark_pink)
p2 = ax.bar(indices, temp.prop_unknown, width, bottom=temp.prop_domestic+temp.prop_foreign, label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
ax.set_ylabel("Proportion of vessels")
plt.title("Domestic vs. foreign ownership flags\nfor top 20 most reflagged flags")
plt.legend()
plt.show()




# ## Which flags have the most foreign flagging?

temp = df_ownership_by_flag[0:40]
flags = temp.index.to_list()
fig, ax = plt.subplots()
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.count_domestic, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, temp.count_foreign, width, bottom=temp.count_domestic, label="Foreign owner", color=color_dark_pink)
p2 = ax.bar(indices, temp.count_unknown, width, bottom=temp.count_domestic+temp.count_foreign, label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
plt.title("Domestic vs. foreign ownership flags\nfor flags with highest proportion of foreign ownership")
ax.set_ylabel("Number of vessels")
plt.legend()
plt.show()

temp = df_ownership_by_flag[0:40]
flags = temp.index.to_list()
fig, ax = plt.subplots()
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.prop_domestic, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, temp.prop_foreign, width, bottom=temp.prop_domestic, label="Foreign owner", color=color_dark_pink)
p2 = ax.bar(indices, temp.prop_unknown, width, bottom=temp.prop_domestic+temp.prop_foreign, label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
ax.set_ylabel("Proportion of vessels")
plt.title("Domestic vs. foreign ownership flags\nfor flags with highest proportion of foreign ownership")
plt.legend()
plt.show()

temp = df_ownership_by_flag.sort_values('count_foreign', ascending=False)[0:40]
flags = temp.index.to_list()
fig, ax = plt.subplots()
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.count_domestic, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, temp.count_foreign, width, bottom=temp.count_domestic, label="Foreign owner", color=color_dark_pink)
p2 = ax.bar(indices, temp.count_unknown, width, bottom=temp.count_domestic+temp.count_foreign, label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
ax.set_ylabel("Number of vessels")
plt.title("Domestic vs. foreign ownership flags\nfor flags with largest total foreign ownership")
plt.legend()
plt.show()

temp = df_ownership_by_flag.sort_values('count_foreign', ascending=False)[0:40]
flags = temp.index.to_list()
fig, ax = plt.subplots()
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.prop_domestic, width, label="Domestic owner", color=color_dark_blue)
p2 = ax.bar(indices, temp.prop_foreign, width, bottom=temp.prop_domestic, label="Foreign owner", color=color_dark_pink)
p2 = ax.bar(indices, temp.prop_unknown, width, bottom=temp.prop_domestic+temp.prop_foreign, label="Unknown owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
ax.set_ylabel("Proportion of vessels")
plt.title("Domestic vs. foreign ownership flags\nfor flags with largest total foreign ownership")
plt.legend()
plt.show()





# ### Likewise, Top X countries having the most vessels whose owners are registered to other countries tend to reflag more than the rest of countries?

# #### Filter down ownership metrics to only the top 50% of flags as used in the figures above

df_ownership_by_flag_filtered = df_ownership_by_flag[df_ownership_by_flag.index.isin(df_reflagging_flags_filtered.flag.to_list())]

# #### Testing out different ways to quantify foreign ownership

df_top20_flags_foreign_total = df_ownership_by_flag_filtered.sort_values('count_foreign', ascending=False)[0:20]
df_top20_flags_foreign_prop = df_ownership_by_flag_filtered.sort_values('prop_foreign', ascending=False)[0:20]


top_flags = df_top20_flags_foreign_total.index.to_list()
count_reflagging = [df_reflagging_flags[df_reflagging_flags.flag == flag].iloc[0].num_reflag if flag in df_reflagging_flags.flag.to_list() else 0 for flag in top_flags]
fig, ax = plt.subplots()
indices = np.arange(len(top_flags))
width = 0.5
p1 = ax.bar(indices, count_reflagging, width, color=color_dark_blue)
# p2 = ax.bar(indices, temp.count_foreign, width, bottom=temp.count_domestic, label="Foreign owner", color="hotpink")
# p3 = ax.bar(indices, count_unknown_owner, width, bottom=count_domestic_owner+count_foreign_owner, label="Unknown owner", color="hotpink")
ax.set_xticks(indices)
ax.set_xticklabels(top_flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
plt.title("Instances of reflagging\nfor flags with largest total foreign ownership")
plt.show()

top_flags = df_top20_flags_foreign_prop.index.to_list()
count_reflagging = [df_reflagging_flags[df_reflagging_flags.flag == flag].iloc[0].num_reflag if flag in df_reflagging_flags.flag.to_list() else 0 for flag in top_flags]
fig, ax = plt.subplots()
indices = np.arange(len(top_flags))
width = 0.5
p1 = ax.bar(indices, count_reflagging, width, color=color_dark_blue)
# p2 = ax.bar(indices, temp.count_foreign, width, bottom=temp.count_domestic, label="Foreign owner", color="hotpink")
# p3 = ax.bar(indices, count_unknown_owner, width, bottom=count_domestic_owner+count_foreign_owner, label="Unknown owner", color="hotpink")
ax.set_xticks(indices)
ax.set_xticklabels(top_flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
plt.title("Instances of reflagging\nfor flags with largest proportional foreign ownership")
plt.show()

# ### Plot normalized reflagging vs total reflagging

top_flags = df_top20_flags_foreign_total.index.to_list()
count_reflagging = [df_reflagging_flags[df_reflagging_flags.flag == flag].iloc[0].norm_reflag if flag in df_reflagging_flags.flag.to_list() else 0 for flag in top_flags]
fig, ax = plt.subplots()
indices = np.arange(len(top_flags))
width = 0.5
p1 = ax.bar(indices, count_reflagging, width, color=color_dark_blue)
# p2 = ax.bar(indices, temp.count_foreign, width, bottom=temp.count_domestic, label="Foreign owner", color="hotpink")
# p3 = ax.bar(indices, count_unknown_owner, width, bottom=count_domestic_owner+count_foreign_owner, label="Unknown owner", color="hotpink")
ax.set_xticks(indices)
ax.set_xticklabels(top_flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
plt.title("Proportional reflagging\nfor flags with largest total foreign ownership")
plt.show()

top_flags = df_top20_flags_foreign_prop.index.to_list()
count_reflagging = [df_reflagging_flags[df_reflagging_flags.flag == flag].iloc[0].norm_reflag if flag in df_reflagging_flags.flag.to_list() else 0 for flag in top_flags]
fig, ax = plt.subplots()
indices = np.arange(len(top_flags))
width = 0.5
p1 = ax.bar(indices, count_reflagging, width, color=color_dark_blue)
# p2 = ax.bar(indices, temp.count_foreign, width, bottom=temp.count_domestic, label="Foreign owner", color="hotpink")
# p3 = ax.bar(indices, count_unknown_owner, width, bottom=count_domestic_owner+count_foreign_owner, label="Unknown owner", color="hotpink")
ax.set_xticks(indices)
ax.set_xticklabels(top_flags, rotation=90)
ax.ticklabel_format(style='plain', axis="y")
plt.title("Proportional reflagging\nfor flags with largest proportional foreign ownership")
plt.show()



# # OLD CODE

# +
prop_domestic_owner = [df_top20_flags.count_domestic.sum()/df_top20_flags.count_total.sum(),
                        df_other_flags.count_domestic.sum()/df_other_flags.count_total.sum()]
prop_foreign_owner = [df_top20_flags.count_foreign.sum()/df_top20_flags.count_total.sum(),
                       df_other_flags.count_foreign.sum()/df_other_flags.count_total.sum()]
prop_unknown_owner = [df_top20_flags.count_unknown.sum()/df_top20_flags.count_total.sum(),
                       df_other_flags.count_unknown.sum()/df_other_flags.count_total.sum()]

x_labels = ['Top 20\nReflagged Flags', 'All Other Flags']
fig, ax = plt.subplots(figsize=(6, 8))
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax.bar(indices, prop_domestic_owner, width, label="Vessel registered to\nsame country as owner", color=color_dark_blue)
p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(x_labels, fontsize=16)
ax.set_ylabel("Proportion of vessels", fontsize=16)
plt.yticks(fontsize=14)
plt.title("Domestic vs. foreign ownership\nby top 20 most reflagged flags vs other flags\n(for top 50% of flags by number of identities)")
plt.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)
plt.tight_layout()
# plt.savefig('./figures/ownership_top20_reflagged_vs_others.png')
plt.show()
# +
norm_reflagging = [df_top20_foreign_ownership.num_reflag.sum()/df_top20_foreign_ownership.num_identities.sum(),
                   df_other_foreign_ownership.num_reflag.sum()/df_other_foreign_ownership.num_identities.sum()]

x_labels = ['Top 20\nForeign Ownership', 'All Other Flags']
fig, ax = plt.subplots(figsize=(6, 8))
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax.bar(indices, norm_reflagging, width, color=color_dark_blue)
# p2 = ax.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
# p3 = ax.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(x_labels, fontsize=16)
ax.set_ylabel("Normalized reflagging", fontsize=16)
plt.yticks(fontsize=14)
plt.title("Normalized reflagging\nby top 20 flags with most foreign ownership vs other flags\n(for top 50% of flags by number of identities)")
# plt.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)
plt.tight_layout()
# plt.savefig('./figures/ownership_top20_foreign_vs_others.png')
plt.show()
# -

