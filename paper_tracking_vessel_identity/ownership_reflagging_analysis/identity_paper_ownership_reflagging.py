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
import matplotlib.lines as mpl_lines
from sklearn.linear_model import LinearRegression
from operator import add
from jinja2 import Template

# Set plot parameters and styling
import pyseas.maps as psm
psm.use(psm.styles.chart_style)
plt.rcParams['figure.figsize'] = (10, 6)

# Show all dataframe rows
pd.set_option("max_rows", 20)
# -

# # Setup

# + [markdown] tags=[]
# ## Visualization settings
#
# Colors from the GFW Data Guidelines.
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

# ## Table Versions
#
# All tables are passed in as parameters so that changing in `queries/config.py` changes everywhere.

from config import PROJECT, VERSION, IDENTITY_TABLE, OWNER_TABLE, EEZ_INFO_TABLE, REFLAGGING_TABLE, FLAGS_OF_CONVENIENCE_TABLE

# ## Figures folder setup
#
# If folder not already available, create it.

# +
figures_folder = f'./figures_v{VERSION}'

if not os.path.exists(figures_folder):
    os.makedirs(figures_folder)
# -

# # Get flag state level metrics for analysis

# ## Ownership
#
# For each flag state, this query calculates how many identities were flagged 
# to that flag state with their owner in that same flag state (domestic), in 
# another flag state (foreign), both (foreign_and_domestic), or without a known 
# owner flag (unknown). These counts are then turned into proportions by dividing 
# by the total number of identities ever flagged to that flag state. `foreign` and 
# `foreign_and_domestic` are combined to create `foreign_combined` metrics as there 
# are only a very small number of identities labeled as `foreign_and_domestic` after
# extensive manual review.
#
# *See jinja2 query for details*

# +
# Open ownership_by_flag.sql.j2 file
with open('queries/ownership_by_flag.sql.j2') as f:
    sql_template = Template(f.read())
    
# Format the query according to the desired features
q = sql_template.render(
    PROJECT=PROJECT,
    VERSION=VERSION,
    EEZ_INFO_TABLE=EEZ_INFO_TABLE,
    IDENTITY_TABLE=IDENTITY_TABLE,
    OWNER_TABLE=OWNER_TABLE
)

# Comment out `print(q)` to get the query being run for QA purposes
# print(q)
df_ownership_by_flag = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_ownership_by_flag

# ## Reflagging
#
# For each flag state, calculate the number of **hulls** (different from identities) 
# ever flagged to that flag state that have reflagged at least once AND hulls that 
# have never reflagged. Proportions are calculated by dividing by the number of 
# **hulls** ever flagged to that flag state.
#
# *See jinja2 query for details*

# +
# Open reflagging_by_flag.sql.j2 file
with open('queries/reflagging_by_flag.sql.j2') as f:
    sql_template = Template(f.read())
    
# Format the query according to the desired features
q = sql_template.render(
    PROJECT=PROJECT,
    VERSION=VERSION,
    REFLAGGING_TABLE=REFLAGGING_TABLE,
    IDENTITY_TABLE=IDENTITY_TABLE,
    OWNER_TABLE=OWNER_TABLE
)

# Comment out `print(q)` to get the query being run for QA purposes
# print(q)
df_reflagging_by_flag = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_reflagging_by_flag

# ## Length of flag stays
#
# For each flag state, the average length that an identity flies that flag. 
#
# *See jinja2 query for details*

# +
# Open flag_stays_by_flag.sql.j2 file
with open('queries/flag_stays_by_flag.sql.j2') as f:
    sql_template = Template(f.read())
    
# Format the query according to the desired features
q = sql_template.render(
    PROJECT=PROJECT,
    VERSION=VERSION,
    IDENTITY_TABLE=IDENTITY_TABLE,
)

# Comment out `print(q)` to get the query being run for QA purposes
# print(q)
df_flag_stays_by_flag = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_flag_stays_by_flag.sort_values('avg_flag_stay')

# # Prepare a final dataset to use for analysis
#
# 1. Merge these three data tables together to get all of the necessary stats for each flag state
# 2. Narrow down to the top 50% of flag states by number of unique hulls every flagged to them (see that section for explanation)

df_flag_data = df_ownership_by_flag.merge(df_reflagging_by_flag, on='flag', how='outer').fillna(0)
df_flag_data = df_flag_data.merge(df_flag_stays_by_flag, on='flag', how='outer').fillna(0)
df_flag_data

# #### Which flag states to include in analysis?
#
# When using proportional metrics, there is the problem of small flag states 
# dominating the results because, for example, they only have 2 vessels, but 
# both of those vessels are foreign-owned leading to a`prop_foreign_combined` 
# of 1. We don't want this so we restrict the analysis to only the top 50% of 
# flag states by number of unique hulls ever flagged to that flag state during 
# the time period of the dataset.

df_flag_data.hulls_total.describe()

pct_50 = df_flag_data.hulls_total.describe()['50%']
print(f"Only keep flag states with at least {pct_50} hulls ever flagged to them.")

df_flag_data_filtered = df_flag_data[df_flag_data.hulls_total >= pct_50].sort_values('norm_reflag', ascending=False).reset_index(drop=True)
print(df_flag_data_filtered.shape[0])
df_flag_data_filtered

# # Analysis

# ## Reflagging and foreign ownership for top 20 flags

# ### The top 20 by normalized reflagging are as follows:

df_top20_reflagging = df_flag_data_filtered.sort_values('norm_reflag', ascending=False)[0:20].reset_index(drop=True)
df_other_reflagging = df_flag_data_filtered.sort_values('norm_reflag', ascending=False)[20:].reset_index(drop=True)
df_top20_reflagging

# ### The top 20 by foreign ownership ratio are as follows:
#
# This uses `prop_foreign_combined`, which is calculated using all identities
# that are marked as `foreign` OR `foreign_and_domestic`.

df_top20_foreign_ownership = df_flag_data_filtered.sort_values("prop_foreign_combined", ascending=False)[0:20].reset_index(drop=True)
df_other_foreign_ownership = df_flag_data_filtered.sort_values("prop_foreign_combined", ascending=False)[20:].reset_index(drop=True)
df_top20_foreign_ownership

# ### STATS

# +
q = f'''
WITH 

reflagging_counts AS (
    SELECT 
    COUNTIF(flag_in IN UNNEST({df_top20_reflagging.flag.to_list()})) as num_reflag_top20,
    COUNTIF(flag_in NOT IN UNNEST({df_top20_reflagging.flag.to_list()})) as num_reflag_other,
    COUNT(*) as total_reflag
    FROM `{PROJECT}.{REFLAGGING_TABLE}{VERSION}`
    WHERE flag_in IS NOT NULL
    AND (flag_in != flag_out OR flag_out IS NULL)
)

SELECT 
num_reflag_top20,
num_reflag_other,
total_reflag,
num_reflag_top20/total_reflag as prop_reflag_top20,
num_reflag_other/total_reflag as prop_reflag_other,
FROM reflagging_counts
'''

df_reflagging_stats = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_reflagging_stats

print(f"{df_reflagging_stats.iloc[0].prop_reflag_top20*100:.1f}% of reflagging instances in our dataset are attributed to the top 20 flag States exhibiting the highest ratio of reflagging to their total fleet size\
even though these flags account for only {df_top20_reflagging.ids_total.sum()/df_flag_data.ids_total.sum()*100:0.1f}% of all identities")


# ### Plot ownership types of Top 20 by reflagging (left) and reflagging of Top 20 by foreign ownership

# +
prop_domestic_owner = [df_top20_reflagging.prop_domestic.mean(),
                        df_other_reflagging.prop_domestic.mean()]
prop_foreign_owner = [df_top20_reflagging.prop_foreign_combined.mean(),
                       df_other_reflagging.prop_foreign_combined.mean()]
prop_unknown_owner = [df_top20_reflagging.prop_unknown.mean(),
                       df_other_reflagging.prop_unknown.mean()]

fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(12, 8), dpi=300)

### Ownership for Top 20 flags by normalized reflagging
x_labels = ['Top 20\nReflagged Flags', 'All Other Flags']
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax0.bar(indices, prop_domestic_owner, width, label="Vessel registered to\nsame country as owner", color=color_dark_blue)
p2 = ax0.bar(indices, prop_foreign_owner, width, bottom=prop_domestic_owner, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
p3 = ax0.bar(indices, prop_unknown_owner, width, bottom=list(map(add, prop_domestic_owner, prop_foreign_owner)), label="No information\navailable on owner", color=color_gray)
ax0.set_xticks(indices)
ax0.set_xticklabels(x_labels, fontsize=16)
ax0.set_ylabel("Mean ownership ratio", fontsize=16)
ax0.set_title("Ownership type for top 20 most\nreflagged flags vs. other flags", fontsize=18)
ax0.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=2)


### Reflagging for Top 20 flags by foreign ownership ratio
norm_reflagging = [df_top20_foreign_ownership.norm_reflag.mean(),
                   df_other_foreign_ownership.norm_reflag.mean()]
norm_not_reflagging = [df_top20_foreign_ownership.norm_not_reflag.mean(),
                       df_other_foreign_ownership.norm_not_reflag.mean()]

x_labels = ['Top 20 by \nForeign Ownership', 'All Other Flags']
indices = np.arange(len(x_labels))
width = 0.6
p1 = ax1.bar(indices, norm_reflagging, width, label="Hulls that were reflagged at least once", color=color_purple)
p2 = ax1.bar(indices, norm_not_reflagging, width, bottom=norm_reflagging, label="Hulls that were never reflagged", color=color_orange)
ax1.set_xticks(indices)
ax1.set_xticklabels(x_labels, fontsize=16)
ax1.set_ylabel("Proportion of hulls", fontsize=16)
ax1.set_title("Reflagging activity for top 20 flags with\nmost foreign ownership vs. other flags", fontsize=18)
ax1.legend(loc='upper left', bbox_to_anchor=[-0.15,-0.15], ncol=1)

### Global layout settings
plt.yticks(fontsize=14)
plt.tight_layout()
plt.savefig(f'{figures_folder}/top20_versus_others.png', dpi=300)
plt.show()
# -

# ### Stats references for the paper

print("                    [Top 20 by Reflagging, All Other Flags]")
print("Domestic:          ", prop_domestic_owner)
print("Foreign:           ", prop_foreign_owner)
print("Unknown:           ", prop_unknown_owner)

print("                     [Top 20 by Foreign Ownership, All Other Flags]")
print("Reflagged:          ", norm_reflagging)
print("Never reflagged:    ", norm_not_reflagging)


print(f"Foreign ownership and reflagging at the flag State level are positively correlated, and the 20 States with the highest fraction of reflagging have rates of foreign ownership {prop_foreign_owner[0]/prop_foreign_owner[1]:0.2f} times greater than all other flag States ({prop_foreign_owner[0]*100:0.1f}% vs. {prop_foreign_owner[1]*100:0.1f}%)")



print(f"Similarly, the top 20 flags with the highest proportion of identities with foreign ownership have a substantially higher proportion of their fleet involved in reflagging than the group of all other flag States ({norm_reflagging[0]*100:.1f}% to {norm_reflagging[1]*100:.1f}%), suggesting that a strong relationship between reflagging practice and foreign ownership exists.")


print(f"We found these 20 flag States that are most involved in the transfer of flags also have a greater tendency to have their reported owners registered to foreign flag States, or a flag State different than their vessel flag, with a rate of foreign ownership {prop_foreign_owner[0]/prop_domestic_owner[0]} times higher than domestic ownership")


# #### Flags of Convenience

# +
q = f'''
SELECT *
FROM {FLAGS_OF_CONVENIENCE_TABLE}
'''

df_focs_itf = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

# Full ITF list
# https://www.itfseafarers.org/en/focs/current-registries-listed-as-focs
# Referenced: October 13, 2021
focs_itf = df_focs_itf.iso3.to_list()
print(focs_itf)

top20_combined = list(set(df_top20_reflagging.flag.to_list()).union(df_top20_foreign_ownership.flag.to_list()))
top20_combined_foc = list(set(top20_combined).intersection(focs_itf))
print(f"{len(top20_combined_foc)} of the States in the top 20 for either foreign ownership or reflagging correspond to flags of convenience reported by ITF.")

# ## Correlation Analysis

# +
x_for = np.asarray(df_flag_data_filtered.norm_reflag.to_list()).reshape((-1,1))
y_for = np.array(df_flag_data_filtered.prop_foreign_combined.to_list())

model_for = LinearRegression().fit(x_for, y_for)
r_sq_for = model_for.score(x_for, y_for)
m_for = model_for.coef_[0]
b_for = model_for.intercept_
print(f"Reflagging and Foreign Ownership statistics by flag state are correlation with an R-squared of {r_sq_for:.2f}.")

# +
fig = plt.figure(figsize=(10,6), dpi=300)
ax = plt.gca()

flag_data_top20_combined = df_flag_data_filtered[df_flag_data_filtered.flag.isin(top20_combined)]
flag_data_other = df_flag_data_filtered[~df_flag_data_filtered.flag.isin(top20_combined)]

# Plot all non-top20 flags as one marker type,
# making FOCs a different color.
flag_data_other_foc = flag_data_other[flag_data_other.flag.isin(focs_itf)]
plt.scatter(flag_data_other_foc.norm_reflag, flag_data_other_foc.prop_foreign_combined, \
            color=color_dark_pink, zorder=3, label="ITF FOC flag")

scatter = plt.scatter(flag_data_other.norm_reflag, flag_data_other.prop_foreign_combined, \
                      color=color_dark_blue, zorder=2, label="All other flags")



# Plot the top20 flags as another marker type,
# making FOCs a different color.
flag_data_top20_foc = flag_data_top20_combined[flag_data_top20_combined.flag.isin(focs_itf)]
plt.scatter(flag_data_top20_foc.norm_reflag, flag_data_top20_foc.prop_foreign_combined, \
            color=color_dark_pink, marker='s', zorder=4)

scatter = plt.scatter(flag_data_top20_combined.norm_reflag, flag_data_top20_combined.prop_foreign_combined, \
                      color=color_dark_blue, marker='s', zorder=3)

# Flag annotations
thresh_reflag = 0.3
thresh_foreign = 0.15
for i, row in df_flag_data_filtered.iterrows():
    if (row.norm_reflag >= thresh_reflag) or (row.prop_foreign_combined >= thresh_foreign):
            if row.flag == 'PAN':
                ## Move slightly to not overlap with LBR
                ax.annotate(row.flag, xy=(row.norm_reflag+0.005, row.prop_foreign_combined-0.02), zorder=2)
            else:
                ax.annotate(row.flag, xy=(row.norm_reflag+0.005, row.prop_foreign_combined+0.005), zorder=2)

# Plot regression line
x_reg = list(np.arange(0, 1.1, 0.1))
y_reg = [b_for + x0*m_for for x0 in x_reg]
ax.plot(x_reg, y_reg, color='black', zorder=1)

plt.title(f"Proportion foreign ownership vs. normalized reflagging")
plt.xlabel("Normalized incidents of reflagging")
plt.ylabel("Proportion foreign ownership")

# Legend
handles, labels = ax.get_legend_handles_labels()
square_patch = mpl_lines.Line2D([], [], marker="s", markersize=10, markeredgecolor=color_gray, linewidth=1, color=(0,0,0,0), label="Top 20 for ownership\nand/or reflagging")
handles.append(square_patch)
plt.legend(handles=handles, fontsize=14, loc='lower right', bbox_to_anchor=(1.0,0.08))


plt.tight_layout()
plt.savefig(f'{figures_folder}/regression_ownership_reflagging_foc.png', dpi=300)
plt.show()

# +
### OLD CODE COLORED BY FLAG STAY
# fig = plt.figure(figsize=(10,6), dpi=300)
# ax = plt.gca()
# scatter = plt.scatter(df_flag_data_filtered.norm_reflag, df_flag_data_filtered.prop_foreign_combined, \
#                       c=df_flag_data_filtered.avg_flag_stay/365, cmap='coolwarm_r', zorder=2, \
#                       edgecolors='black', linewidth=0.5)
# # Flag annotations
# thresh_reflag = 0.3
# thresh_foreign = 0.15
# for i, row in df_flag_data_filtered.iterrows():
#     if (row.norm_reflag >= thresh_reflag) or (row.prop_foreign_combined >= thresh_foreign):
#             if row.flag == 'PAN':
#                 ## Move slightly to not overlap with LBR
#                 ax.annotate(row.flag, xy=(row.norm_reflag+0.005, row.prop_foreign_combined-0.02), zorder=2)
#             else:
#                 ax.annotate(row.flag, xy=(row.norm_reflag+0.005, row.prop_foreign_combined+0.005), zorder=2)

# # Plot regression line
# x_reg = list(np.arange(0, 1.1, 0.1))
# y_reg = [b_for + x0*m_for for x0 in x_reg]
# ax.plot(x_reg, y_reg, color='black', zorder=1)

# plt.title(f"Proportion foreign ownership vs. normalized reflagging")
# plt.xlabel("Normalized incidents of reflagging")
# plt.ylabel("Proportion foreign ownership")
# plt.tight_layout()

# # Legend
# cbar = plt.colorbar(scatter, location='right', shrink=0.5, pad=0.03)
# cbar.set_label("Average Flag Stay (years)", fontsize=10)

# plt.savefig(f'{figures_folder}/regression_ownership_reflagging_foreign.png', dpi=300)
# plt.show()
# -

# ### Flag stay stats

top20_shared = list(set(df_top20_reflagging.flag.to_list()).intersection(df_top20_foreign_ownership.flag.to_list()))
avg_stay_shared = df_flag_data_filtered[df_flag_data_filtered.flag.isin(top20_shared)].avg_flag_stay.mean()
avg_stay_other = df_flag_data_filtered[~df_flag_data_filtered.flag.isin(top20_shared)].avg_flag_stay.mean()
print(f"The average length of flag stay of the {len(top20_shared)} States that occupy the top 20 in both categories is {avg_stay_shared/avg_stay_other:0.2f} times as long as that of the group of the remaining States ({avg_stay_shared/365:0.1f} vs {avg_stay_other/365:0.1f} years).")

# ---
# # Supplementary Materials

# ## Top 20 countries by normalized reflagging
#
# To be copied into doc and formatted.

table_top20_reflagging = df_top20_reflagging[['flag', 'ids_total', 'hulls_total', 'hulls_reflagged', 'norm_reflag']].copy()
table_top20_reflagging = table_top20_reflagging.astype({'hulls_total': 'int64', 'hulls_reflagged': 'int64'})
table_top20_reflagging.norm_reflag = table_top20_reflagging.norm_reflag.round(3)
table_top20_reflagging

# + [markdown] tags=[]
# ## Top 20 countries by foreign ownership
#
#
# -

table_top20_foreign_ownership = df_top20_foreign_ownership[['flag', 'ids_total', 'hulls_total', 'ids_foreign_combined', 'prop_foreign_combined']].copy()
table_top20_foreign_ownership = table_top20_foreign_ownership.astype({'hulls_total': 'int64'})
table_top20_foreign_ownership.prop_foreign_combined = table_top20_foreign_ownership.prop_foreign_combined.round(3)
table_top20_foreign_ownership

# ## Breakdown in ownership stats for each flag in Top 20 by reflagging

temp = df_top20_reflagging.copy()
flags = temp.flag.to_list()
fig, ax = plt.subplots(figsize=(8,7), dpi=300)
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.prop_domestic, width, label="Vessel registered to\nsame country as owner", color=color_dark_blue)
p2 = ax.bar(indices, temp.prop_foreign_combined, width, bottom=temp.prop_domestic, label="Vessel registered to\ndifferent country than owner", color=color_dark_pink)
p3 = ax.bar(indices, temp.prop_unknown, width, bottom=temp.prop_domestic+temp.prop_foreign_combined, label="No information\navailable on owner", color=color_gray)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90, fontsize=14)
plt.yticks(fontsize=14)
ax.set_ylabel("Proportion of vessels", fontsize=16)
plt.title("Vessel ownership type\nfor top 20 most reflagged flags")
plt.legend(loc='upper left', bbox_to_anchor=[0.08,-0.18], ncol=2)
plt.tight_layout()
plt.savefig(f'{figures_folder}/ownership_top20_reflagged_flags.png', dpi=300)
plt.show()

temp = df_top20_foreign_ownership.copy()
flags = temp.flag.to_list()
fig, ax = plt.subplots(figsize=(8,7), dpi=300)
indices = np.arange(len(flags))
width = 0.5
p1 = ax.bar(indices, temp.norm_reflag, width, label="Hulls that were reflagged at least once", color=color_purple)
p2 = ax.bar(indices, temp.norm_not_reflag, width, bottom=temp.norm_reflag, label="Hulls that were never reflagged", color=color_orange)
ax.set_xticks(indices)
ax.set_xticklabels(flags, rotation=90, fontsize=14)
plt.yticks(fontsize=14)
ax.set_ylabel("Proportion of hulls", fontsize=16)
plt.title("Reflagging\nfor top 20 flags with highest foreign ownership")
plt.legend(loc='upper left', bbox_to_anchor=[0.08,-0.18], ncol=1)
plt.tight_layout()
plt.savefig(f'{figures_folder}/reflagging_top20_foreign_ownership_flags.png', dpi=300)
plt.show()



# ## Push rendered notebook to `rendered` repo

# +
# import rendered
# rendered.publish_to_github(f'./identity_paper_ownership_reflagging.ipynb', 
#                            f'vessel-identity-paper-v{VERSION}/', action='push')
# -

