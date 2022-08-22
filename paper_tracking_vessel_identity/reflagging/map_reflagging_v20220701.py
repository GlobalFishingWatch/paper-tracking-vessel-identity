# ---
# jupyter:
#   jupytext:
#     formats: py:light,ipynb
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.1
#   kernelspec:
#     display_name: py37
#     language: python
#     name: py37
# ---

# # Vessel Reflagging History and Ranking By Year

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#
# Version of the data 
YYYYMMDD = '20220701'

# ## Reflagging History of Fishing and Support Vessels

# +
colors_for_flag = {
    'RUS': "#f8ba47",
    'PAN': "#ee6256", 
    'EU': "#0c276c",
    'KOR': "#ebe55d", 
    'CHN': "#ff4573", #"#e74327",
    'NOR': "#b0d6e3", 
    'BLZ': "#f79f8d", 
    'NAM': "#88d498", 
    'BHS': "#88d498", 
    'FRO': "#204280", 
    
    'KIR': "#8abbc7",
    'LBR': "#ad2176",
    'MHL': "#FFc414",
    'SLE': "#7dad38",
    'ISL': "#457b9d",
    'KNA': "#f59e8b",
    'VUT': "#e89d2d",
    'SGP': "#3b9088",
    'KHM': "#ff4573",
    'JPN': "#f68d4b",
    
    'FSM': "#ffdd49",
    'ARG': "#FFc414", #"#89c6b4", 
    'CMR': "#1d7874",
    'MAR': "#D291BC",
    'NRU': "#FFD51A",
    'NZL': "#fccd81",
    'COK': "#f8a26b",
    'TGO': "#1d7874", 
    'MNG': "#ff4573",
    'CHL': "#dd677e",
    
    'GEO': "#00ffc3",
    'PLW': "#eee981",
    'PNG': "#c85037",
    'MNG': "#a23e2a",
    'ATG': "#b9558a",
    'TUR': "#f68d4b",
    'USA': "#89c6b4",
    'IDN': "#f79f8d",
    'TWN': "#ad2176",
    'MDV': "#b2b2b2",
    'GNB': "#b2b2b2",
    'SEN': "#f68d4b",
    'OTHERS':"#b2b2b2" 
}

def color_mapping (flags):
    col = flags.apply(lambda x: colors_for_flag[x] 
                      if x in colors_for_flag.keys() 
                      else colors_for_flag['OTHERS'])
    return col


# -

# ## Reflagging History of Fishing Vessels

#
# Get the fishing vessels' reflagging history
q = f"""
SELECT *
FROM `vessel_identity.reflagging_history_map_top15_fishing_v{YYYYMMDD}`
"""
top_fishing = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Display the top 15 most reflagged fishing vessels as of 2021-07-01
# Each dot on the figure represents an activity in a 5-day window.
# Flags are color-coded
df_reflag_fishing = top_fishing[top_fishing['rank_1'] <= 15].copy()
fig = plt.figure(figsize=(8, 10), dpi=500, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(df_reflag_fishing.timestamp,
           df_reflag_fishing.vessel_record_id,
           facecolor=color_mapping(df_reflag_fishing.flag),
           s=1.1, edgecolor='none', alpha=1)

#
# Add current flag information on y-axis on the right side
xposition = 0.918
fontsize = 14
flags = df_reflag_fishing['latest_flag'].unique()[::-1][:15]
cnt = df_reflag_fishing.groupby('latest_flag')['vessel_record_id'].nunique().sort_values()[::-1]
yposition = [0.78, 0.60, 0.53, 0.48, 0.425, 
             0.375, 0.325, 0.28, 0.235, 0.196,
             0.16, 0.125, 0.09, 0.057, 0.024]
for i in range(len(flags)):
    # Flags
    ax.text(xposition, yposition[i], flags[i],
            fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[flags[i]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    # Numbers
    ax.text(xposition + 0.135, yposition[i], str(int(round(cnt[i]))), ha='right',
            fontsize=fontsize-2, transform=ax.transAxes)
#   
# Manual addition of flag tags for groups of previous flags
base = 0.02
yoff = 0.02
tags = {
    "JPN": [base + 0.25, yoff + 0.92],
    "EU": [base + 0.20, yoff + 0.852],
    "NOR": [base + 0.15, yoff + 0.802],
    "ISL": [base + 0.10, yoff + 0.768],
    "KOR": [base + 0.05, yoff + 0.744],
    "CHN": [base, yoff + 0.718],
    "NOR-2": [base + 0.05, yoff + 0.61],
    "RUS": [base, yoff + 0.582],
#     "EU-2": [base + 0.05, yoff + 0.5],
    "EU-4": [base + 0.05, yoff + 0.53],
    "ISL-2": [base, yoff + 0.447],
#     "RUS-2": [0.10, 0.54],
    "EU-3": [base + 0.05, yoff + 0.474],
    "JPN-2": [base, yoff + 0.377],
    "RUS-3": [base + 0.2, yoff + 0.367],
    "MDV": [base + 0.1, yoff + 0.356],
    "CHN-2": [base + 0.15, yoff + 0.316],
    "KIR": [base + 0.5, yoff + 0.305],
    "PNG-2": [base + 0.2, yoff + 0.23],
#     "JPN-3": [base, yoff + 0.312],
    "NOR-4": [base, yoff + 0.27],
    "GEO": [base + 0.55, yoff + 0.18],
    "NOR-5": [base, yoff + 0.05],
    "NOR-3": [base, yoff + 0.15],
    "PNG": [base + 0.15, yoff + 0.11],
    "KOR-2": [base, yoff + 0.012],
    "USA": [base + 0.2, yoff + 0.09],
    "TUR": [base + 0.1, yoff + 0.073]
}
for key in tags:
    ax.text(tags[key][0], tags[key][1], key.split('-')[0], 
            fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[key.split('-')[0]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    

plt.yticks([0] + np.cumsum(cnt[::-1].values).tolist(), labels=[None] * 16)
ax.tick_params(axis="y",direction="in", pad=-50)

ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
plt.xlim(pd.Timestamp("2011-12-01"), pd.Timestamp("2022-12-31"))
# labels = [''] + list(range(2012, 2022, 1))
# ax.set_xticklabels(labels)
plt.xticks(fontsize=12)
plt.margins(y=0.02)
plt.title('Flagging History of Reflagged Fishing Vessels Since 2012', fontsize=14)
plt.show()
# -

# ## Reflagging History of Support Vessels

#
# Get the support vessels' reflagging history
q = f"""
SELECT *
FROM `vessel_identity.reflagging_history_map_top15_support_v{YYYYMMDD}`
"""
top_support = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Display the top 15 most reflagged support vessels as of 2021-07-01
# Each dot on the figure represents an activity in a 5-day window.
# Flags are color-coded
df_reflag_support = top_support[top_support['rank_1'] <= 15].copy()
fig = plt.figure(figsize=(8, 10), dpi=500, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(df_reflag_support.timestamp, 
           df_reflag_support.vessel_record_id, 
           facecolor=color_mapping(df_reflag_support.flag), 
           s=0.9, edgecolor='none', alpha=1)

#
# Add current flag information on y-axis on the right side
xposition = 0.918
fontsize = 14
flags = list(df_reflag_support['latest_flag'].unique())[::-1][:15]
#
# Swap PLW and LBR
# index1 = flags.index("PLW")
# index2 = flags.index("LBR")
# flags[index1], flags[index2] = flags[index2], flags[index1]

cnt = df_reflag_support.groupby('latest_flag')['vessel_record_id'].nunique().sort_values()[::-1]
yposition = [0.82, 0.605, 0.497, 0.427, 0.36, 
             0.309, 0.27, 0.225, 0.19, 0.155,
             0.125, 0.099, 0.072, 0.048, 0.02]
for i in range(len(flags)):
    # Flags
    ax.text(xposition, yposition[i], flags[i],
            fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[flags[i]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    # Numbers
    ax.text(xposition + 0.135, yposition[i], str(int(round(cnt[i]))), ha='right',
            fontsize=fontsize-2, transform=ax.transAxes)
    
#   
# Manual addition of flag tags for groups of previous flags
base = 0.02
yoff = 0.02
tags = {
    "LBR": [base + 0.30, yoff + 0.93],
    "KIR": [base + 0.25, yoff + 0.875],
    "VUT": [base + 0.20, yoff + 0.835],
    "EU": [base + 0.15, yoff + 0.805],
    "MHL": [base + 0.10, yoff + 0.78],
    "BHS": [base + 0.05, yoff + 0.757],
    "BLZ": [base, yoff + 0.735],
    "PAN": [base + 0.15, yoff + 0.654],
    "EU-2": [base + 0.10, yoff + 0.604],
    "LBR-2": [base + 0.05, yoff + 0.581],
    "KNA": [base, yoff + 0.56],
    "LBR-3": [base + 0.1, yoff + 0.512],
    "MHL-2": [base + 0.05, yoff + 0.49],
    "NOR": [base, yoff + 0.47],
    "PAN-3": [base + 0.10, yoff + 0.44],
    "LBR-5": [base + 0.05, yoff + 0.42],
    "MHL-4": [base, yoff + 0.40],
#     "LBR-4": [base, yoff + 0.37],
    "VUT-2": [base + 0.10, yoff + 0.375],
    "MHL-3": [base + 0.05, yoff + 0.355],
    "PAN-2": [base, yoff + 0.335],
    "NOR-2": [base, yoff + 0.255],
    "PAN-4": [base, yoff + 0.18],
    "ATG": [base, yoff + 0.113],
    "PAN-5": [base + 0.0, yoff + 0.01]
}
for key in tags:
    ax.text(tags[key][0], tags[key][1], key.split('-')[0], fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[key.split('-')[0]], edgecolor='none', pad=1.5, alpha=0.9))
    
plt.yticks([0] + np.cumsum(cnt[::-1].values).tolist(), labels=[None] * 16)
ax.tick_params(axis="y",direction="in", pad=-50)

ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
plt.xlim(pd.Timestamp("2011-12-01"), pd.Timestamp("2022-12-31"))
# labels = [''] + list(range(2012, 2022, 1))
# ax.set_xticklabels(labels)
plt.xticks(fontsize=12)
plt.margins(y=0.02)
plt.title('Flagging History of Reflagged Support Vessels Since 2012', fontsize=14)
plt.show()
# -

# ## Flagging History of All Support Vessels

#
# Get the all support vessels' reflagging history
q = f"""
SELECT *
FROM `vessel_identity.all_flagging_support_v{YYYYMMDD}`
ORDER BY rank_1 DESC, rank_major_flag DESC, second_last_timestamp ASC,
  TIMESTAMP_DIFF (last_timestamp, first_timestamp, DAY) ASC
"""
all_flagging_support = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Display activities of all support vessels flying the top 15 reflagging flags
# Each dot on the figure represents an activity in a 5-day window.
# Flags are color-coded
df_all_support = all_flagging_support[all_flagging_support['rank_1'] <= 15].copy()
fig = plt.figure(figsize=(10, 12), dpi=500, facecolor='white')
ax = fig.add_subplot(111)
ax.scatter(df_all_support.timestamp, 
           df_all_support.vessel_record_id, 
           facecolor=color_mapping(df_all_support.flag), 
           s=0.8, edgecolor='none', alpha=1)

#
# Add current flag information on y-axis on the right side
xposition = 0.96
fontsize = 8
flags = df_all_support['latest_flag'].unique()[::-1][:15]
ratio = df_all_support[['latest_flag', 'reflag_ratio']].drop_duplicates()['reflag_ratio'].values[::-1][:15]
yposition = [0.865, 0.71, 0.60, 0.51, 0.43,
             0.36, 0.3, 0.242, 0.183, 0.138,
             0.107, 0.074, 0.054, 0.043, 0.033]
for i in range(len(flags)):
    ax.text(xposition, yposition[i], flags[i] + " (" + str(int(round(ratio[i] *100))) + " %)",
            fontsize=fontsize, transform=ax.transAxes)

ax.get_yaxis().set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
plt.title('Flag History of All Support Vessels Flying Top 15 Flags as of 2021 (2012-2021)', fontsize=10)
plt.show()
# -

# ## Reflagging Fishing Vessels By Year

#
# Top 10 colorcodes
colors = ['#204280', '#884a8f', '#d3567f', '#fa7f5f', '#f8ba47', 
          '#c2bb49', '#92b55a', '#68ac6e', '#489f7f', '#3b9088']
grey = '#b2b2b2'

#
# Get the data regarding the yearly counts of reflagging fishing vessels by flag
q = f"""
SELECT *
FROM `vessel_identity.reflagging_counts_byyear_fishing_v{YYYYMMDD}`
WHERE year <= 2021
"""
df_yearly_fishing = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Top 10 as of 2020 
top10 = df_yearly_fishing[(df_yearly_fishing.year == 2021) & 
                          (df_yearly_fishing.rank_ratio <= 10)].sort_values('rank_ratio')
top10_flags = top10.sort_values('rank_ratio').flag_in_eu.values
top10_ratio = top10.sort_values('rank_ratio').cnt_log.values

fig = plt.figure(figsize=(10, 7), dpi=300, facecolor='w')
ax = fig.add_subplot(111)

#
# Scatter plot with grey color for all counts by year
for i, flag_in_eu in enumerate(df_yearly_fishing.flag_in_eu.unique()):
    df_base = df_yearly_fishing[df_yearly_fishing.flag_in_eu == flag_in_eu].sort_values('year')
    ax.plot(df_base.year, df_base.cnt_log, c=grey, zorder=1)
    ax.scatter(df_base.year, df_base.cnt_log, linewidth=2, s=60,
               edgecolor=grey, facecolor='w',
               marker='o', zorder=2) 
#
# Color code differently for the top 10 as of 2020
for i, flag_in_eu in enumerate(top10_flags):
    df_top10 = df_yearly_fishing[df_yearly_fishing.flag_in_eu == flag_in_eu].sort_values('year')
    ax.scatter(df_top10.year, df_top10.cnt_log, linewidth=2, s=60,
               marker='o', edgecolor=colors[i], facecolor='w', zorder=4) 
    ax.plot(df_top10.year, df_top10.cnt_log, c=colors[i], zorder=3)

#
# Annotate for top 10 on the right side y-axis
xposition = 0.98
fontsize = 10
# flags = ['Russia', 'Norway', 'Namibia', 'Georgia', 'China',
#          'Belize', 'Iceland', 'South Korea', 'UK', 'Spain']
flags = ['Russia', 'E.U.', 'Norway', 'Namibia', 'China',
         'Georgia', 'Iceland', 'Belize', 'S. Korea', 'Japan']
flagnames_extra = ['Japan']
x_extra = [0.89]
y_extra = [0.02]
yposition = [0.96, 0.61, 0.50, 0.47, 0.448, 
             0.425, 0.403, 0.378, 0.315, 0.18]
for i in range(len(flags)):
    ax.text(xposition, yposition[i], flags[i] + " (" + str(top10.cnt.iloc[i]) + ")",
            fontsize=fontsize, transform=ax.transAxes)
    
# for i in range(len(flagnames_extra)):
#     ax.text(x_extra[i], y_extra[i], flagnames_extra[i] + " (6)", 
#             fontsize=fontsize, transform=ax.transAxes)

ax.set_ylim(0.65, 2.25)
ax.set_xticks(range(2012, 2022))
plt.xlabel('Year')
plt.ylabel('Number of Vessels (log base 10 scale)')
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
plt.grid(True, linestyle='--', linewidth=0.2)
plt.title('Number of Fishing Vessels by Flag States with Reflagging History (2012-2021)', fontsize=12)
plt.show()
# -

#
# Get the data regarding the yearly counts of reflagging support vessels by flag
q = f"""
SELECT *
FROM `vessel_identity.reflagging_counts_byyear_support_v{YYYYMMDD}`
WHERE year <= 2021
"""
df_yearly_support = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Top 10 as of 2020 
top10 = df_yearly_support[(df_yearly_support.year == 2021) & 
                          (df_yearly_support.rank_ratio <= 10)].sort_values('rank_ratio')
top10_flags = top10.sort_values('rank_ratio').flag_in_eu.values
top10_ratio = top10.sort_values('rank_ratio').cnt_log.values
colors = ['#204280', '#884a8f', '#d3567f', '#fa7f5f', '#f8ba47', 
          '#c2bb49', '#92b55a', '#68ac6e', '#489f7f', '#3b9088']
grey = '#b2b2b2'

fig = plt.figure(figsize=(10, 7), dpi=300, facecolor='w')
ax = fig.add_subplot(111)

#
# Scatter plot with grey color for all counts by year
for i, flag_in_eu in enumerate(df_yearly_support.flag_in_eu.unique()):
    df_base = df_yearly_support[df_yearly_support.flag_in_eu == flag_in_eu].sort_values('year')
    ax.plot(df_base.year, df_base.cnt_log, c=grey, zorder=1)
    ax.scatter(df_base.year, df_base.cnt_log, linewidth=2, s=60,
               edgecolor=grey, facecolor='w',
               marker='o', zorder=2) 
#
# Color code differently for the top 10 as of 2020
for i, flag_in_eu in enumerate(top10_flags):
    df_top10 = df_yearly_support[df_yearly_support.flag_in_eu == flag_in_eu].sort_values('year')
    ax.scatter(df_top10.year, df_top10.cnt_log, linewidth=2, s=60,
               marker='o', edgecolor=colors[i], facecolor='w', zorder=4) 
    ax.plot(df_top10.year, df_top10.cnt_log, c=colors[i], zorder=3)

#
# Annotate for top 10 on the right side y-axis
xposition = 0.98
fontsize = 10
flags = ['Panama', 'Russia', 'E.U.', 'South Korea', 'Bahamas', 
         'Liberia', 'Marshall Islands', 'Belize', 'Kiribati']
flagnames_extra = ['Vanuatu'] #'St. Kitts & Nevis', 
x_extra = [0.665]
y_extra = [0.025]
yposition = [0.92, 0.76, 0.575, 0.55, 0.525, 0.35, 0.235, 0.07, 0.02]
for i in range(len(flags)):
    ax.text(xposition, yposition[i], flags[i] + " (" + str(top10.cnt.iloc[i]) + ")",
            fontsize=fontsize, transform=ax.transAxes)
    
for i in range(len(flagnames_extra)):
    ax.text(x_extra[i], y_extra[i], flagnames_extra[i] + " (" + str(top10.cnt.iloc[i+9]) + ")", 
            fontsize=fontsize, transform=ax.transAxes)

ax.set_ylim(0.65, 2.25)
ax.set_xticks(range(2012, 2022))
plt.xlabel('Year')
plt.ylabel('Number of Vessels (log base 10 scale)')
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
plt.grid(True, linestyle='--', linewidth=0.2)
plt.title('Number of Support Vessels by Flag States with Reflagging History (2012-2021)', fontsize=12)
plt.show()
# -
# import rendered
# rendered.publish_to_github('./map_reflagging.ipynb',
#                            'vessel-identity-paper/', action='push')



