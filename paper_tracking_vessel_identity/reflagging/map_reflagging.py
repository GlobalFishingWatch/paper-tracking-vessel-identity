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
YYYYMMDD = '20210701'

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
    'ARG': "#89c6b4", 
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
fig = plt.figure(figsize=(12, 9), dpi=500, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(df_reflag_fishing.timestamp,
           df_reflag_fishing.vessel_record_id,
           facecolor=color_mapping(df_reflag_fishing.flag),
           s=1.1, edgecolor='none', alpha=1)

#
# Add current flag information on y-axis on the right side
xposition = 0.915
fontsize = 14
flags = df_reflag_fishing['latest_flag'].unique()[::-1][:15]
cnt = df_reflag_fishing.groupby('latest_flag')['vessel_record_id'].nunique().sort_values()[::-1]
yposition = [0.79, 0.605, 0.54, 0.49, 0.435, 
             0.38, 0.33, 0.28, 0.24, 0.2,
             0.16, 0.12, 0.09, 0.06, 0.03]
for i in range(len(flags)):
    # Flags
    ax.text(xposition, yposition[i], flags[i],
            fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[flags[i]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    # Numbers
    ax.text(xposition + 0.082, yposition[i], str(int(round(cnt[i]))), ha='right',
            fontsize=fontsize-2, transform=ax.transAxes)
#   
# Manual addition of flag tags for groups of previous flags
base = 0.02
yoff = 0.02
tags = {
    "JPN": [base + 0.25, yoff + 0.92],
    "EU": [base + 0.20, yoff + 0.86],
    "NOR": [base + 0.15, yoff + 0.823],
    "ISL": [base + 0.10, yoff + 0.785],
    "KOR": [base + 0.05, yoff + 0.76],
    "CHN": [base, yoff + 0.725],
    "NOR-2": [base + 0.05, yoff + 0.62],
    "RUS": [base, yoff + 0.6],
#     "EU-2": [base + 0.05, yoff + 0.5],
    "NOR-3": [base, yoff + 0.528],
#     "RUS-2": [0.10, 0.54],
    "EU-3": [base, yoff + 0.48],
    "EU-4": [base + 0.05, yoff + 0.43],
    "ISL-2": [base, yoff + 0.405],
    "JPN-2": [base, yoff + 0.375],
    "RUS-3": [base + 0.2, yoff + 0.365],
    "PNG-2": [base + 0.2, yoff + 0.33],
#     "JPN-3": [base, yoff + 0.312],
    "CHN-2": [base + 0.2, yoff + 0.275],
    "NOR-4": [base, yoff + 0.227],
    "GEO": [base + 0.55, yoff + 0.19],
    "BLZ": [base, yoff + 0.148],
    "PNG": [base + 0.35, yoff + 0.113],
    "USA": [base + 0.30, yoff + 0.091],
    "KOR-2": [base, yoff + 0.01]
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
plt.xlim(pd.Timestamp("2011-12-01"), pd.Timestamp("2022-06-15"))
labels = [''] + list(range(2012, 2022, 1))
ax.set_xticklabels(labels)
plt.xticks(fontsize=12)
plt.margins(y=0.02)
plt.title('Flagging History of Reflagged Fishing Vessels Before 2021', fontsize=18)
plt.show()
# -

#
# Get the fishing vessels' reflagging history
q = """
SELECT *
FROM `scratch_jaeyoon.reflagging_history_fishing_test`
"""
top_fishing_rev = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Display the top 15 most reflagged fishing vessels as of 2021-07-01
# Each dot on the figure represents an activity in a 5-day window.
# Flags are color-coded
df_reflag_fishing = top_fishing_rev[top_fishing_rev['rank_1'] <= 15].copy()
fig = plt.figure(figsize=(12, 9), dpi=500, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(df_reflag_fishing.timestamp,
           df_reflag_fishing.vessel_record_id,
           facecolor=color_mapping(df_reflag_fishing.flag),
           s=1.1, edgecolor='none', alpha=1)

#
# Add current flag information on y-axis on the right side
xposition = 0.084
fontsize = 14
flags = df_reflag_fishing['earliest_flag'].unique()[::-1][:15]
cnt = df_reflag_fishing.groupby('earliest_flag')['vessel_record_id'].nunique().sort_values()[::-1]
yposition = [0.89, 0.72, 0.58, 0.47, 0.39, 
             0.32, 0.25, 0.21, 0.17, 0.135,
             0.11, 0.08, 0.055, 0.03, 0.005]
for i in range(len(flags)):
    # Flags
    ax.text(xposition, yposition[i], flags[i],
            fontsize=fontsize-1, fontweight='bold', ha='right',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[flags[i]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    # Numbers
    ax.text(xposition - 0.082, yposition[i], str(int(round(cnt[i]))), ha='left',
            fontsize=fontsize-2, transform=ax.transAxes)
#   
# Manual addition of flag tags for groups of previous flags
base = 0.95
yoff = 0.02
tags = {
    "RUS": [base - 0.2, yoff + 0.94],
    "NOR": [base - 0.15, yoff + 0.91],
    "NAM": [base - 0.10, yoff + 0.89],
    "FRO": [base - 0.05, yoff + 0.87],
    "BLZ": [base, yoff + 0.85],
    "RUS-2": [base - 0.15, yoff + 0.765],
    "EU": [base - 0.10, yoff + 0.74],
    "FRO-2": [base - 0.05, yoff + 0.715],
    "ISL": [base, yoff + 0.69],
    "RUS-3": [base - 0.1, yoff + 0.60],
    "KOR": [base - 0.05, yoff + 0.568],
    "CHN": [base, yoff + 0.55],
    "RUS-4": [base - 0.05, yoff + 0.50],
    "KIR": [base, yoff + 0.48],
    "RUS-5": [base - 0.05, yoff + 0.39],
    "EU-2": [base, yoff + 0.37],
    "EU-3": [base - 0.05, yoff + 0.325],
    "CHN-2": [base, yoff + 0.31],
    "RUS-6": [base, yoff + 0.235],
    "GEO": [base, yoff + 0.19],
    "NRU": [base, yoff + 0.12],
}
for key in tags:
    ax.text(tags[key][0], tags[key][1], key.split('-')[0], 
            fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[key.split('-')[0]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    

plt.yticks([0] + np.cumsum(cnt[::-1].values).tolist(), labels=[None] * 16)
ax.yaxis.tick_right()
ax.tick_params(axis="y",direction="in", pad=-50)

ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
plt.xlim(pd.Timestamp("2011-01-15"), pd.Timestamp("2021-08-01"))
labels = [''] + list(range(2012, 2022, 1))
ax.set_xticklabels(labels)
plt.xticks(fontsize=12)
plt.margins(y=0.02)
plt.title('Flagging History of Reflagged Fishing Vessels Since 2012', fontsize=18)
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
fig = plt.figure(figsize=(12, 9), dpi=500, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(df_reflag_support.timestamp, 
           df_reflag_support.vessel_record_id, 
           facecolor=color_mapping(df_reflag_support.flag), 
           s=1.1, edgecolor='none', alpha=1)

#
# Add current flag information on y-axis on the right side
xposition = 0.915
fontsize = 14
flags = list(df_reflag_support['latest_flag'].unique())[::-1][:15]
#
# Swap PLW and LBR
# index1 = flags.index("PLW")
# index2 = flags.index("LBR")
# flags[index1], flags[index2] = flags[index2], flags[index1]

cnt = df_reflag_support.groupby('latest_flag')['vessel_record_id'].nunique().sort_values()[::-1]
yposition = [0.82, 0.595, 0.47, 0.40, 0.345, 
             0.29, 0.25, 0.21, 0.18, 0.15,
             0.12, 0.092, 0.068, 0.04, 0.01]
for i in range(len(flags)):
    # Flags
    ax.text(xposition, yposition[i], flags[i],
            fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[flags[i]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    # Numbers
    ax.text(xposition + 0.082, yposition[i], str(int(round(cnt[i]))), ha='right',
            fontsize=fontsize-2, transform=ax.transAxes)
    
#   
# Manual addition of flag tags for groups of previous flags
base = 0.02
yoff = 0.02
tags = {
    "LBR": [base + 0.30, yoff + 0.925],
    "KIR": [base + 0.25, yoff + 0.87],
    "VUT": [base + 0.20, yoff + 0.83],
    "EU": [base + 0.15, yoff + 0.798],
    "BHS": [base + 0.10, yoff + 0.77],
    "MHL": [base + 0.05, yoff + 0.75],
    "BLZ": [base, yoff + 0.728],
    "PAN": [base + 0.15, yoff + 0.632],
    "EU-2": [base + 0.10, yoff + 0.585],
    "LBR-2": [base + 0.05, yoff + 0.563],
    "KNA": [base, yoff + 0.545],
    "LBR-3": [base + 0.10, yoff + 0.487],
    "MHL-2": [base + 0.05, yoff + 0.473],
    "NOR": [base, yoff + 0.447],
    "MHL-3": [base + 0.15, yoff + 0.408],
    "VUT-2": [base + 0.10, yoff + 0.395],
    "PAN-2": [base + 0.05, yoff + 0.38],
    "LBR-4": [base, yoff + 0.367],
    "PAN-3": [base + 0.10, yoff + 0.34],
    "LBR-5": [base + 0.05, yoff + 0.318],
    "EU-3": [base, yoff + 0.305],
    "NOR-2": [base, yoff + 0.24],
    "SGP-2": [base, yoff + 0.13],
    "ATG": [base, yoff + 0.01]
}
for key in tags:
    ax.text(tags[key][0], tags[key][1], key.split('-')[0], fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[key.split('-')[0]], edgecolor='none', pad=1.5, alpha=0.9))
    
plt.yticks([0] + np.cumsum(cnt[::-1].values).tolist(), labels=[None] * 16)
ax.tick_params(axis="y",direction="in", pad=-50)

ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
plt.xlim(pd.Timestamp("2011-12-01"), pd.Timestamp("2022-06-15"))
labels = [''] + list(range(2012, 2022, 1))
ax.set_xticklabels(labels)
plt.xticks(fontsize=12)
plt.margins(y=0.02)
plt.title('Flagging History of Reflagged Support Vessels Before 2021', fontsize=18)
plt.show()
# -

#
# Get the support vessels' reflagging history
q = """
SELECT *
FROM `scratch_jaeyoon.reflagging_history_support_test`
"""
top_support_rev = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Display the top 15 most reflagged support vessels as of 2021-07-01
# Each dot on the figure represents an activity in a 5-day window.
# Flags are color-coded
df_reflag_support = top_support_rev[top_support_rev['rank_1'] <= 15].copy()
fig = plt.figure(figsize=(12, 9), dpi=500, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(df_reflag_support.timestamp, 
           df_reflag_support.vessel_record_id, 
           facecolor=color_mapping(df_reflag_support.flag), 
           s=1.1, edgecolor='none', alpha=1)

#
# Add current flag information on y-axis on the right side
xposition = 0.084
fontsize = 14
flags = df_reflag_support['earliest_flag'].unique()[::-1][:15]
cnt = df_reflag_support.groupby('earliest_flag')['vessel_record_id'].nunique().sort_values()[::-1]
num = [0] + np.cumsum(cnt[::-1].values).tolist()
y = [(a + b) / 2. / num[-1] for a, b in zip(num[:], num[1:])][::-1]
yoff = [-0.03, -0.02, 0, 0, 0,
        0, 0, 0, 0, 0.01,
        0.01, 0.01, 0.01, 0.005, 0]
yposition = [a + b for a, b in zip (y, yoff)]
for i in range(len(flags)):
    # Flags
    ax.text(xposition, yposition[i], flags[i],
            fontsize=fontsize-1, fontweight='bold', ha='right',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[flags[i]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    # Numbers
    ax.text(xposition - 0.082, yposition[i], str(int(round(cnt[i]))), ha='left',
            fontsize=fontsize-2, transform=ax.transAxes)
#   
# Manual addition of flag tags for groups of previous flags
base = 0.95
yoff = 0.02
tags = {
    "RUS": [base - 0.2, yoff + 0.93],
    "KIR": [base - 0.15, yoff + 0.89],
    "KOR": [base - 0.10, yoff + 0.87],
    "BHS": [base - 0.05, yoff + 0.84],
    "BLZ": [base, yoff + 0.82],
    "MHL": [base - 0.1, yoff + 0.7],
    "RUS-2": [base - 0.05, yoff + 0.675],
    "LBR": [base, yoff + 0.65],
    "PAN": [base - 0.1, yoff + 0.58],
    "BHS-2": [base - 0.05, yoff + 0.56],
    "EU": [base, yoff + 0.54],
    "PAN-2": [base - 0.05, yoff + 0.425],
    "KOR-2": [base, yoff + 0.41],
    "PAN-3": [base, yoff + 0.36],
    "CHL": [base, yoff + 0.305],
    "PAN-4": [base - 0.1, yoff + 0.26],
    "LBR-2": [base - 0.05, yoff + 0.245],
    "KOR-3": [base, yoff + 0.235],
    "RUS-3": [base, yoff + 0.21],
    "PAN-5": [base, yoff + 0.155],
    "KNA": [base, yoff + 0.05],
}
for key in tags:
    ax.text(tags[key][0], tags[key][1], key.split('-')[0], 
            fontsize=fontsize-1, fontweight='bold',
            color='white', transform=ax.transAxes,
            bbox=dict(facecolor=colors_for_flag[key.split('-')[0]], 
                      edgecolor='none', pad=1.5, alpha=0.9))
    

plt.yticks(num, labels=[None] * 16)
ax.yaxis.tick_right()
ax.tick_params(axis="y",direction="in", pad=-50)

ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
plt.xlim(pd.Timestamp("2011-01-15"), pd.Timestamp("2021-08-01"))
labels = [''] + list(range(2012, 2022, 1))
ax.set_xticklabels(labels)
plt.xticks(fontsize=12)
plt.margins(y=0.02)
plt.title('Flagging History of Reflagged Support Vessels Since 2012', fontsize=18)
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
ratio = df_all_support['reflag_ratio'].unique()[::-1][:15]
yposition = [0.865, 0.71, 0.60, 0.53, 0.465,
             0.395, 0.335, 0.28, 0.225, 0.175,
             0.14, 0.11, 0.08, 0.06, 0.045]
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
WHERE year < 2021
"""
df_yearly_fishing = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Top 10 as of 2020 
top10 = df_yearly_fishing[(df_yearly_fishing.year == 2020) & 
                          (df_yearly_fishing.rank_ratio <= 10)].sort_values('rank_ratio')
top10_flags = top10.sort_values('rank_ratio').flag_in.values
top10_ratio = top10.sort_values('rank_ratio').cnt_log.values

fig = plt.figure(figsize=(10, 7), dpi=300, facecolor='w')
ax = fig.add_subplot(111)

#
# Scatter plot with grey color for all counts by year
for i, flag_in in enumerate(df_yearly_fishing.flag_in.unique()):
    df_base = df_yearly_fishing[df_yearly_fishing.flag_in == flag_in].sort_values('year')
    ax.plot(df_base.year, df_base.cnt_log, c=grey, zorder=1)
    ax.scatter(df_base.year, df_base.cnt_log, linewidth=2, s=60,
               edgecolor=grey, facecolor='w',
               marker='o', zorder=2) 
#
# Color code differently for the top 10 as of 2020
for i, flag_in in enumerate(top10_flags):
    df_top10 = df_yearly_fishing[df_yearly_fishing.flag_in == flag_in].sort_values('year')
    ax.scatter(df_top10.year, df_top10.cnt_log, linewidth=2, s=60,
               marker='o', edgecolor=colors[i], facecolor='w', zorder=4) 
    ax.plot(df_top10.year, df_top10.cnt_log, c=colors[i], zorder=3)

#
# Annotate for top 10 on the right side y-axis
xposition = 0.98
fontsize = 10
flags = ['Russia', 'Norway', 'Namibia', 'Georgia', 'China',
         'Belize', 'Iceland', 'South Korea', 'UK', 'Spain']
flagnames_extra = ['Japan']
x_extra = [0.89]
y_extra = [0.02]
yposition = [0.915, 0.45, 0.392, 0.364, 0.33, 
             0.304, 0.275, 0.248, 0.175, 0.145]
for i in range(len(flags)):
    ax.text(xposition, yposition[i], flags[i] + " (" + str(top10.cnt.iloc[i]) + ")",
            fontsize=fontsize, transform=ax.transAxes)
    
for i in range(len(flagnames_extra)):
    ax.text(x_extra[i], y_extra[i], flagnames_extra[i] + " (6)", 
            fontsize=fontsize, transform=ax.transAxes)

ax.set_ylim(0.9, 2.25)
plt.xlabel('Year')
plt.ylabel('Number of Vessels (log base 10 scale)')
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
plt.grid(True, linestyle='--', linewidth=0.2)
plt.title('Number of Fishing Vessels by Flag States with Reflagging History (2012-2020)', fontsize=12)
plt.show()
# -

#
# Get the data regarding the yearly counts of reflagging support vessels by flag
q = f"""
SELECT *
FROM `vessel_identity.reflagging_counts_byyear_support_v{YYYYMMDD}`
WHERE year < 2021
"""
df_yearly_support = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Top 10 as of 2020 
top10 = df_yearly_support[(df_yearly_support.year == 2020) & 
                          (df_yearly_support.rank_ratio <= 10)].sort_values('rank_ratio')
top10_flags = top10.sort_values('rank_ratio').flag_in.values
top10_ratio = top10.sort_values('rank_ratio').cnt_log.values
colors = ['#204280', '#884a8f', '#d3567f', '#fa7f5f', '#f8ba47', 
          '#c2bb49', '#92b55a', '#68ac6e', '#489f7f', '#3b9088']
grey = '#b2b2b2'

fig = plt.figure(figsize=(10, 7), dpi=300, facecolor='w')
ax = fig.add_subplot(111)

#
# Scatter plot with grey color for all counts by year
for i, flag_in in enumerate(df_yearly_support.flag_in.unique()):
    df_base = df_yearly_support[df_yearly_support.flag_in == flag_in].sort_values('year')
    ax.plot(df_base.year, df_base.cnt_log, c=grey, zorder=1)
    ax.scatter(df_base.year, df_base.cnt_log, linewidth=2, s=60,
               edgecolor=grey, facecolor='w',
               marker='o', zorder=2) 
#
# Color code differently for the top 10 as of 2020
for i, flag_in in enumerate(top10_flags):
    df_top10 = df_yearly_support[df_yearly_support.flag_in == flag_in].sort_values('year')
    ax.scatter(df_top10.year, df_top10.cnt_log, linewidth=2, s=60,
               marker='o', edgecolor=colors[i], facecolor='w', zorder=4) 
    ax.plot(df_top10.year, df_top10.cnt_log, c=colors[i], zorder=3)

#
# Annotate for top 10 on the right side y-axis
xposition = 0.98
fontsize = 10
flags = ['Panama', 'Russia', 'South Korea', 'Bahamas', 'Liberia', 
         'Marshall Islands']
flagnames_extra = ['Belize', 'Kiribati', 'Vanuatu'] #'St. Kitts & Nevis', 
x_extra = [0.69, 0.645, 0.565]
y_extra = [0.035, 0.142, 0.025]
yposition = [0.895, 0.713, 0.442, 0.372, 0.285, 0.215]
for i in range(len(flags)):
    ax.text(xposition, yposition[i], flags[i] + " (" + str(top10.cnt.iloc[i]) + ")",
            fontsize=fontsize, transform=ax.transAxes)
    
for i in range(len(flagnames_extra)):
    ax.text(x_extra[i], y_extra[i], flagnames_extra[i] + " (" + str(top10.cnt.iloc[i+6]) + ")", 
            fontsize=fontsize, transform=ax.transAxes)

ax.set_ylim(0.9, 2.25)
plt.xlabel('Year')
plt.ylabel('Number of Vessels (log base 10 scale)')
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
plt.grid(True, linestyle='--', linewidth=0.2)
plt.title('Number of Support Vessels by Flag States with Reflagging History (2012-2020)', fontsize=12)
plt.show()
# -
# import rendered
# rendered.publish_to_github('./map_reflagging.ipynb',
#                            'vessel-identity-paper/', action='push')



