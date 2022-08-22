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

# # Identity data Statistics and Visual Representation

# +
#
# Libraries
import pandas as pd
import numpy as np
pd.set_option('max_columns', 100)
pd.set_option('max_rows', 100)
pd.options.mode.chained_assignment = None

import matplotlib.pyplot as plt
plt.style.use('bmh')
# %matplotlib inline

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# +
# #
# # import explicitely roboto-font 
# import matplotlib.font_manager as fm
# rbt_med = fm.FontProperties(fname='../utilities/Roboto/Roboto-Medium.ttf')
# rbt_reg = fm.FontProperties(fname='../utilities/Roboto/Roboto-Regular.ttf')
# rbt_ita = fm.FontProperties(fname='../utilities/Roboto/Roboto-Italic.ttf')
# -

q = """
SELECT *
FROM `vessel_identity.identity_data_summary_allyears_v20220701`
WHERE flag != "UNK"
ORDER BY total_active DESC, rate_matched_reg_ais DESC
"""
allyears = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Create other tables to compare China vs. the rest of the world
df_non_chn = allyears[allyears['flag'] != "CHN"]
df_chn = allyears[(allyears['flag'] == "CHN")]

df_non_chn['flag'] = "NON_CHN"
df_non_chn = df_non_chn.groupby('flag').agg(
    {'matched_reg_ais': sum, 
     'unmatched_reg': sum, 
     'unmatched_ais': sum}).reset_index()
df_non_chn['rate_matched_reg_ais'] = df_non_chn.apply(
    lambda x: x['matched_reg_ais'] / (x['matched_reg_ais'] + x['unmatched_ais']), axis=1)
# -

# ## Plot a horizontal bar chart that compares China vs. Rest of the World

# +
#
# Prepare for a diverging horizontal bar chart
colors = ['white', '#f8ba47','#204280','#d73b68']
df_chn['label'] = "China"
df_non_chn['label'] = "Rest of\nthe World"
chn_vs_non_chn = pd.concat([df_chn, df_non_chn], sort=False)
chn_vs_non_chn = chn_vs_non_chn[["label", "unmatched_reg", "matched_reg_ais", "unmatched_ais"]]

middles = chn_vs_non_chn["unmatched_reg"]
longest_registry = chn_vs_non_chn["unmatched_reg"].max()
longest_ais = chn_vs_non_chn[["matched_reg_ais", "unmatched_ais"]].sum(axis=1).max()
complete_longest = longest_registry + longest_ais
chn_vs_non_chn.insert(0, '', (middles - longest_registry).abs())
chn_vs_non_chn['total_reg'] = chn_vs_non_chn.apply(
    lambda x: x['unmatched_reg'] + x['matched_reg_ais'], axis=1)
chn_vs_non_chn['total_ais'] = chn_vs_non_chn.apply(
    lambda x: x['unmatched_ais'] + x['matched_reg_ais'], axis=1)

columns = ['', 'label', 'Unmatched Registry', 
           'Matched Between Registries and AIS', 'Unmatched AIS',
           'Total Registered', 'Total AIS Active']
chn_vs_non_chn.columns = columns

#
# Plot
fig = plt.figure(figsize=(12, 4), dpi=300, facecolor='#f7f7f7')# 'white')
ax = fig.add_subplot(111, facecolor='white')
chn_vs_non_chn[['', 'label', 'Unmatched Registry', 
                'Matched Between Registries and AIS',
                'Unmatched AIS']].sort_values('label', ascending=False)\
                .set_index('label')\
                .plot.barh(ax=ax, stacked=True, width=0.6, 
                           color=colors, edgecolor='none', legend=True)

colors = ['#f68d4b', '#742980', '#8abbc7','red'] #'#f8ba47','#204280','#d73b68']
chn_vs_non_chn[['', 'label', 'Total Registered', "Total AIS Active"]]\
                .sort_values('label', ascending=False)\
                .set_index('label')\
                .plot.barh(ax=ax, stacked=True, width=0.58, #linestyle='--', 
                           color='none', lw=2, edgecolor='none', legend=True)
for rect in ax.patches[-4:-2]:
    facecolor = list(rect.get_facecolor())
    rect.set_edgecolor(colors[0])
#     rect.set_alpha(0.5)

chn_vs_non_chn[['', 'label', 'Unmatched Registry', "Total AIS Active"]]\
                .sort_values('label', ascending=False)\
                .set_index('label')\
                .plot.barh(ax=ax, stacked=True, width=0.61, linestyle='--', 
                           color='none', lw=1.5, edgecolor='none', legend=True)
for rect in ax.patches[-2:]:
    facecolor = list(rect.get_facecolor())
    rect.set_edgecolor(colors[1])
#     rect.set_alpha(0.5)
    
    
z = plt.axvline(longest_registry, linewidth=0.5, linestyle='-', color='black', alpha=1)
z.set_zorder(-1)

#
# Ticks, labels, and legends
plt.xlim(0 - complete_longest * 0.05, complete_longest * 1.05)
gap = 5000
xvalues = range(longest_registry % gap, complete_longest, gap)
xlabels = [str(abs(x-longest_registry))[:-3] + ',' + str(abs(x-longest_registry))[-3:] 
           if abs(x-longest_registry) != 0 else str(abs(x-longest_registry)) for x in xvalues]
xlabels = ['' if '5,000' in x else x for x in xlabels]
plt.xticks(xvalues, xlabels, fontsize=16)
plt.yticks(None, fontsize=16)

handles, labels = plt.gca().get_legend_handles_labels()
order = [0, 3, 1, 6, 2]
leg = plt.legend([handles[idx] for idx in order],
                 [labels[idx] for idx in order],
                 ncol=3, bbox_to_anchor=(-0.0, 0.96),
                 loc='lower left', fontsize=12, frameon=False )

ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
plt.ylabel(None)
ax.yaxis.grid(False)
ax.xaxis.grid(True, lw=0.4)
plt.tick_params(axis='y', which='both', left=False, labelbottom=False)
plt.tick_params(axis='x', which='both', bottom=False, labelbottom=True)

plt.title("Number of Vessel Identities in Registry and AIS (>24 m, 2012-2021)", pad=45, fontsize=18)
plt.show()
# -

# ## By country

# +
#
# plot matched/unmatched record numbers by country for all years
colors = ['white', '#f8ba47','#204280','#d73b68']
columns={
    'unmatched_reg': 'Unmatched Registry', 
    'matched_reg_ais': 'Matched Between Registries and AIS',
    'unmatched_ais': 'Unmatched AIS'
}
fig = plt.figure(figsize=(12, 7.5), dpi=400, facecolor='#f7f7f7')
ax = fig.add_subplot(111, facecolor='white')

#
# Prepare for a diverging horizontal bar chart (Top 30, excluding China and invalid Flag code)
byctr = allyears[(allyears.flag != "CHN") & (allyears.flag.notnull())][:30]
byctr = byctr[["flag", "unmatched_reg", "matched_reg_ais", "unmatched_ais"]]

middles = byctr["unmatched_reg"]
longest = middles.max()
complete_longest = byctr["unmatched_reg"].max() + \
                    byctr[["matched_reg_ais", "unmatched_ais"]].sum(axis=1).max()
byctr.insert(0, '', (middles - longest).abs()) # Base transparent bars without name to exclude it from legends
byctr['total_ais_active'] = byctr['matched_reg_ais'] + byctr['unmatched_ais'] 
byctr = byctr.rename(columns=columns)

#
# Plot
byctr = byctr.set_index('flag').sort_values('total_ais_active')
byctr.drop('total_ais_active', axis=1)\
     .plot.barh(ax=ax, stacked=True, width=0.8, 
                color=colors, edgecolor='none', legend=True)
z = plt.axvline(longest, linewidth=1, linestyle='-', color='black', alpha=.5)
z.set_zorder(-1)

#
# Ticks, labels, legends
plt.xlim(complete_longest * -0.01, complete_longest * 1.05)
gap = 500
xvalues = range(longest % gap, complete_longest, gap)
xlabels = [str(abs(x-longest))[:-3] + ',' + str(abs(x-longest))[-3:] 
           if len(str(abs(x-longest))) > 3 else str(abs(x-longest)) for x in xvalues]
plt.xticks(xvalues, xlabels, fontsize=16)
plt.yticks(None)

#
# Legends
ax.legend(ncol=3, bbox_to_anchor=(0.05, 0.99),
          loc='lower left', fontsize=12, frameon=False)

ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
plt.ylabel(None)
ax.yaxis.grid(False)
ax.xaxis.grid(True, lw=0.4)
plt.tick_params(axis='y', which='both', left=False, labelbottom=False, labelleft=False, labelright=True,
                pad=-420, labelcolor='white', labelsize=11)
labels = ax.get_yticklabels()
[label.set_fontweight('bold') for label in labels]
plt.tick_params(axis='x', which='both', bottom=False, labelbottom=True)

plt.title("Number of Vessel Identities in Registry and AIS\nTop 30 Flag States Excluding China (>24 m, 2012-2021)", 
          fontsize=18, pad=30)
plt.show()
# -

# ## Combined Top 30

# +
#
# plot matched/unmatched record numbers by country for all years

##############################################
# Top 30 without China in a vertical bar chart
##############################################
colors = ['white', '#f8ba47','#204280','#d73b68']
columns={
    'unmatched_reg': 'Vessel identities in registry unmatched to AIS', 
    'matched_reg_ais': 'Vessel identities matched between registries and AIS',
    'unmatched_ais': 'Vessel identities in AIS unmatched to registry'
}
fig = plt.figure(figsize=(12, 7.5), dpi=400, facecolor='#f7f7f7')
ax = fig.add_subplot(111, facecolor='white')

#
# Prepare for a diverging vertical bar chart (Top 30, excluding China and invalid Flag code)
byctr = allyears[(allyears.flag != "CHN") & (allyears.flag.notnull())][:29]
byctr = byctr[["flag", "unmatched_reg", "matched_reg_ais", "unmatched_ais"]]

middles = byctr["unmatched_reg"]
longest = middles.max()
complete_longest = byctr["unmatched_reg"].max() + \
                    byctr[["matched_reg_ais", "unmatched_ais"]].sum(axis=1).max()
byctr.insert(0, '', (middles - longest).abs()) # Base transparent bars without name to exclude it from legends
byctr['total_ais_active'] = byctr['matched_reg_ais'] + byctr['unmatched_ais'] 
byctr = byctr.rename(columns=columns)

#
# Plot for the Top 30 without China
byctr = byctr.set_index('flag').sort_values('total_ais_active', ascending=False)
byctr.drop('total_ais_active', axis=1)\
     .plot.bar(ax=ax, stacked=True, width=0.8, 
                color=colors, edgecolor='none', legend=True)
z = plt.axhline(longest, linewidth=1, linestyle='-', color='black', alpha=.5)
z.set_zorder(-1)

#
# Ticks, labels, legends
plt.ylim(complete_longest * -0.01, complete_longest * 1.05)
gap = 500
yvalues = range(longest % gap, complete_longest, gap)
ylabels = [str(abs(y-longest))[:-3] + ',' + str(abs(y-longest))[-3:] 
           if len(str(abs(y-longest))) > 3 else str(abs(y-longest)) for y in yvalues]
plt.yticks(yvalues, ylabels, fontsize=10)

#
# Legends
handles, labels = plt.gca().get_legend_handles_labels()
order = [2, 1, 0]
leg = plt.legend([handles[idx] for idx in order],
                 [labels[idx] for idx in order],
                 ncol=1, bbox_to_anchor=(0.98, 0.),
                 loc='lower right', fontsize=12, frameon=False )

ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
plt.xlabel(None)
plt.ylabel('Number of vessel identities by flag state')
ax.xaxis.grid(False)
ax.yaxis.grid(True, lw=0.4)
plt.tick_params(axis='x', which='both', bottom=False, labelbottom=True, 
                pad=-154, labelcolor='white', labelsize=9)
labels = ax.get_xticklabels()
[label.set_fontweight('bold') for label in labels]
[label.set_va('bottom') for label in labels]
plt.xticks(None)
plt.tick_params(axis='y', which='both', left=False, labelleft=True)
plt.title("Number of Vessel Identities by Top 30 Flag States (>24 m, 2012-2021)", 
          fontsize=13, pad=10)


#######################################################
# China vs. the rest of the world in a horizontal chart
#######################################################
#
# inset axes.
axins = ax.inset_axes([0.45, 0.7, 0.53, 0.27], facecolor='white')

#
# Prepare for a diverging horizontal bar chart
df_chn['label'] = "China"
df_non_chn['label'] = "Rest of\nthe World"
chn_vs_non_chn = pd.concat([df_chn, df_non_chn], sort=False)
chn_vs_non_chn = chn_vs_non_chn[["label", "unmatched_reg", "matched_reg_ais", "unmatched_ais"]]

middles = chn_vs_non_chn["unmatched_reg"]
longest = middles.max()
complete_longest = chn_vs_non_chn[["unmatched_reg", "matched_reg_ais", "unmatched_ais"]].sum(axis=1).max()
chn_vs_non_chn.insert(0, '', (middles - longest).abs())

columns = ['', 'label', 'Unmatched Registry', 
           'Matched Between Registries and AIS', 'Unmatched AIS']
chn_vs_non_chn.columns = columns

#
# Plot
chn_vs_non_chn[['', 'label', 'Unmatched Registry', 
                'Matched Between Registries and AIS',
                'Unmatched AIS']].sort_values('label', ascending=False)\
                .set_index('label')\
                .plot.barh(ax=axins, stacked=True, width=0.6, 
                           color=colors, edgecolor='none', legend=False)

for axis in ['top','bottom','left','right']:
    axins.spines[axis].set_linewidth(5)
    axins.spines[axis].set_color('#f7f7f7')
axins.yaxis.grid(False)
axins.xaxis.grid(True, lw=0.4)
axins.set_ylabel(None)
axins.set_xlabel('Number of vessel identities')

#
# Ticks, labels, and legends
chn_vs_non_chn = pd.concat([df_chn, df_non_chn], sort=False)
chn_vs_non_chn = chn_vs_non_chn[["label", "unmatched_reg", "matched_reg_ais", "unmatched_ais"]]
middles = chn_vs_non_chn["unmatched_reg"]
longest = middles.max()
longest_registry = chn_vs_non_chn["unmatched_reg"].max()
longest_ais = chn_vs_non_chn[["matched_reg_ais", "unmatched_ais"]].sum(axis=1).max()
complete_longest = longest_registry + longest_ais
axins.set_xlim(0 - complete_longest * 0.05, complete_longest * 1.05)
gap = 5000
xvalues = range(longest % gap, complete_longest, gap)
xlabels = [str(abs(x-longest))[:-3] + ',' + str(abs(x-longest))[-3:] 
           if abs(x-longest) != 0 else str(abs(x-longest)) for x in xvalues]
xlabels = ['' if '5,000' in x else x for x in xlabels]
axins.set_xticks(xvalues)
axins.set_xticklabels(xlabels) #, fontsize=16)
axins.axvline(longest, linewidth=0.5, linestyle='-', color='black', alpha=1)


axins.tick_params(axis='y', which='both', left=False, labelbottom=False,
                  pad=-105, labelcolor='white', labelsize=11)
axins.tick_params(axis='x', which='both', bottom=False, labelbottom=True, zorder=5)
labels = axins.get_yticklabels()
[label.set_fontweight('bold') for label in labels]
[label.set_ha('left') for label in labels]

plt.show()
# -

# ## Yearly Comparison of China vs. Rest of the World

q = """
SELECT *
FROM `vessel_identity.identity_data_summary_byyear_v20220701`
ORDER BY year, total_active DESC, rate_matched_reg_ais DESC
"""
byyear = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Create a tables to compare China vs. the rest of the world by year
byyear['year'] = byyear['year'].astype(int)
df_non_chn = byyear[byyear['flag'] != "CHN"]
df_chn = byyear[(byyear['flag'] == "CHN")]

df_non_chn['flag'] = "NON_CHN"
df_non_chn = df_non_chn.groupby(['year', 'flag']).agg(
    {'matched_reg_ais': sum, 
     'unmatched_reg': sum, 
     'unmatched_ais': sum}).reset_index()

#
# Create other tables to compare China vs. the rest of the world
df_chn['label'] = "China"
df_non_chn['label'] = "Rest of the World"
chn_vs_non_chn = pd.concat([df_chn, df_non_chn], sort=False)

# +
#
# Plot China vs Rest of the world by year
colors = ['white', '#f8ba47','#204280','#d73b68']
labels = ['Unmatched Registry Records', 
          'Matched Records Between Registries and AIS',
          'Unmatched AIS Records']
width = 0.3
offset = 0.02

fig = plt.figure(figsize=(10, 6), dpi=400, facecolor='#f7f7f7')# 'white')
ax = fig.add_subplot(111, facecolor='white')

#
# Base transparent bars 
max_unm_reg = chn_vs_non_chn['unmatched_reg'].max()
chn_vs_non_chn['new_base'] = chn_vs_non_chn.apply(lambda x: max_unm_reg - (x['unmatched_reg']), axis=1)

#
# China's yearly stacked bars
chn_y = chn_vs_non_chn[chn_vs_non_chn.label == "China"]\
        [['new_base', 'year', 'unmatched_reg', 
          'matched_reg_ais', 'unmatched_ais']].sort_values('year', ascending=True)

ax.bar(chn_y['year'] - width/2 - offset, chn_y['new_base'], width=width, color=colors[0])
ax.bar(chn_y['year'] - width/2 - offset, chn_y['unmatched_reg'], width=width, color=colors[1], 
       bottom=chn_y['new_base'], 
       label=labels[0])
ax.bar(chn_y['year'] - width/2 - offset, chn_y['matched_reg_ais'], width=width, color=colors[2], 
       bottom=chn_y['new_base'] + chn_y['unmatched_reg'],
       label=labels[1])
ax.bar(chn_y['year'] - width/2 - offset, chn_y['unmatched_ais'], width=width, color=colors[3], 
       bottom=chn_y['new_base'] + chn_y['unmatched_reg'] + chn_y['matched_reg_ais'],
       label=labels[2])
#
# Rest of the world's yearly stacked bars
rest_y = chn_vs_non_chn[chn_vs_non_chn.label == "Rest of the World"]\
        [['new_base', 'year', 'unmatched_reg', 
          'matched_reg_ais', 'unmatched_ais']].sort_values('year', ascending=True)

ax.bar(rest_y['year'] + width/2 + offset, rest_y['new_base'], width=width, color=colors[0])
ax.bar(rest_y['year'] + width/2 + offset, rest_y['unmatched_reg'], width=width, color=colors[1], 
       bottom=rest_y['new_base'])
ax.bar(rest_y['year'] + width/2 + offset, rest_y['matched_reg_ais'], width=width, color=colors[2], 
       bottom=rest_y['new_base'] + rest_y['unmatched_reg'])
ax.bar(rest_y['year'] + width/2 + offset, rest_y['unmatched_ais'], width=width, color=colors[3], 
       bottom=rest_y['new_base'] + rest_y['unmatched_reg'] + rest_y['matched_reg_ais'])

#
# Ticks, labels, and legends
complete_longest = (rest_y['new_base'] + rest_y['unmatched_reg'] + \
                    rest_y['matched_reg_ais'] + rest_y['unmatched_ais']).max()
plt.ylim(-1700, 32000)
gap = 5000
yvalues = range(max_unm_reg % gap, complete_longest, gap)
ylabels = [str(abs(y-max_unm_reg))[:-3] + ',' + str(abs(y-max_unm_reg))[-3:] 
           if abs(y-max_unm_reg) != 0 else str(abs(y-max_unm_reg)) for y in yvalues]
plt.yticks(yvalues, ylabels)
plt.xticks(range(2012, 2022, 1))
plt.xlim(2011.5, 2021.5)

#
# Annotation on the first bars
xpos = [0.025, 0.065]
ypos = [0.37, 0.37]
texts = ['China', 'Rest of the World']
fs = 7
for i in range(len(texts)):
    ax.text(xpos[i], ypos[i], texts[i], rotation=90, color='white',
                fontsize=fs, weight='bold', transform=ax.transAxes)
#
# Legends
handles, labels = plt.gca().get_legend_handles_labels()
order = [2, 1, 0]
leg = plt.legend([handles[idx] for idx in order],
                 [labels[idx] for idx in order],
                 ncol=1, bbox_to_anchor=(0, 0.85),
                 loc='lower left', fontsize='small', frameon=False )

ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.yaxis.grid(True, lw=0.4)
ax.xaxis.grid(True, lw=0.2)
plt.tick_params(axis='y', which='both', left=False, labelbottom=False)
plt.tick_params(axis='x', which='both', bottom=False, labelbottom=True)

plt.title("Number of Vessel Identities in Registry and AIS by Year\nChina vs. Rest of the World (>24 m, 2012-2020)", 
          pad=16)
plt.show()
# -

# ## By country for 2018

q = """
SELECT *
FROM `vessel_identity.identity_data_summary_byyear_v20220701`
WHERE year = 2018
"""
df2018 = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

#
# Reference reported numbers of vessels >24 meters 
# (sourced from FAO SOFIA report 2020 and OECD database)
spain2018 = 382 + 164 + 83 + 35 + 15 + 25
korea2018 = 1336
mexico2018 = 240
srilanka = 20

#
# Create a table containing country information
country = df2018[(df2018.flag == 'KOR') | 
                 (df2018.flag == 'ESP') | 
                 (df2018.flag == 'MEX') | 
                 (df2018.flag == 'LKA')]
country_map = {'KOR': 'Rep.\nof Korea',
               'ESP': 'Spain',
               'MEX': 'Mexico',
               'LKA': 'Sri\nLanka'}
reported_map = {'KOR': korea2018,
               'ESP': spain2018,
               'MEX': mexico2018,
               'LKA': srilanka}
country['label'] = country.flag.apply(lambda x: country_map[x]) 
country['reported'] = country.flag.apply(lambda x: reported_map[x])
country = country.sort_values('unmatched_reg', ascending=False)
country

# +
#
# Plot selected countries matching summary
colors = ['#f8ba47', '#204280', '#d73b68']
color_for_line = '#8abbc7'
column_map = {'unmatched_reg': 'Unmatched Registry', 
              'matched_reg_ais': 'Matched Between Registries and AIS',
              'unmatched_ais': 'Unmatched AIS',
              'reported': 'Reported Number of Vessels'}
fig = plt.figure(figsize=(12, 5), dpi=400, facecolor='#f7f7f7')
gaps = [500, 500, 100, 20]

for i, flag in enumerate(country.flag):
    temp = country[country.flag == flag]
    ax = fig.add_subplot(int(f'22{i+1}'), facecolor='white')
    temp[['label', 'unmatched_reg', 'matched_reg_ais', 'unmatched_ais']]\
        .set_index('label')\
        .plot.barh(ax=ax, stacked=True, width=0.6, 
                   color=colors, edgecolor='none', legend=True)

    temp[['label', 'unmatched_reg', 'reported']]\
        .set_index('label')\
        .plot.barh(ax=ax, stacked=True, width=0.60,
                   color='none', lw=2.5, linestyle='--', 
                   edgecolor='none', legend=True)
    for rect in ax.patches[-1:]:
        facecolor = list(rect.get_facecolor())
        rect.set_edgecolor(color_for_line)


    # Ticks, labels, and legends
    gap = gaps[i]
    total_length = max(temp['matched_reg_ais'].iloc[0] + 
                       temp['unmatched_reg'].iloc[0] + \
                       temp['unmatched_ais'].iloc[0],
                       temp['unmatched_reg'].iloc[0] + \
                       temp['reported'].iloc[0])
    base = temp["unmatched_reg"].iloc[0]
    plt.xlim(0 - total_length * 0.05, total_length * 1.05)
    xvalues = range(base % gap, int(total_length * 1.05), gap)
    xlabels = [str(abs(x-base))[:-3] + ',' + str(abs(x-base))[-3:] 
               if abs(x-base) != 0 and len(str(abs(x-base))) > 3 
               else str(abs(x-base)) for x in xvalues]
    plt.xticks(xvalues, xlabels, fontsize=16)
    plt.yticks(None, fontsize=16)

    if i == 0:
        handles, labels = plt.gca().get_legend_handles_labels()
        order = [0, 4, 1, 2]
        leg = plt.legend([handles[idx] for idx in order],
                         [column_map[labels[idx]] for idx in order],
                         ncol=3, bbox_to_anchor=(-0, 0.95),
                         loc='lower left', fontsize=12, frameon=False )
    else:
        ax.legend().set_visible(False)

    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    plt.ylabel(None)
    ax.yaxis.grid(False)
    ax.xaxis.grid(True, lw=0.4)
    plt.tick_params(axis='y', which='both', left=False, labelbottom=False)
    plt.tick_params(axis='x', which='both', bottom=False, labelbottom=True)

fig.text(0.5, 1.02, fontsize=18, ha='center',
         s="Number of Fishing Vessels in Registry and AIS " +\
         "vs. Reported total (>24 m, 2018)")
plt.show()
# -

# ## Chinese Distant Water Fleet

q = """
SELECT *
FROM `vessel_identity.identity_data_summary_chinese_dwf_v20220701`
"""
china_dwf = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

china_dwf['reported_total'] = [1830, 2159, 2460, 2512, 2571, 
                               2491, 2654, 2701, 2705, np.nan, np.nan]
china_dwf['frac_matched_dwf_to_reported_total'] = china_dwf['total_matched_dwf_vessels'] / \
                                                    china_dwf['reported_total']

china_dwf

# +
#
# Plot Chinese Distant Water Fleet
colors = ['#204280', '#ee6256', '#8abbc7','#d73b68']
labels = ['Chinese Fishing Vessels Matched Between AIS and Registry', 
          'Chinese DWF Matched Between AIS and Registry', 
          'Reported Total Size of Chinese DWF', 
          'Chinese DWF on AIS',
          'Fraction of Matched Chinese DWF to the Reported Total']
width = 0.45
offset = 0.02

fig = plt.figure(figsize=(11, 6.5), dpi=400, facecolor='#f7f7f7')
ax = fig.add_subplot(111, facecolor='white')

#
# Bars for reported DWF, matched all fishing vessels, matched DWF, and DWF on AIS
ax.bar(china_dwf['year'], china_dwf['total_matched_vessels'], width=width + 0.15,
       color=colors[0], label=labels[0])
ax.bar(china_dwf['year'], china_dwf['total_matched_dwf_vessels'], width=width, 
       color=colors[1], label=labels[1])
ax.bar(china_dwf['year'], china_dwf['reported_total'], width=width + 0.18, 
       facecolor='none', edgecolor=colors[2], linestyle='--', linewidth=1.5, 
       label=labels[2])
ax.bar(china_dwf['year'], china_dwf['total_ais_dwf_vessels'], width=width + -0.03, 
       facecolor='none', edgecolor=colors[1], linestyle='--', linewidth=1.5, 
       label=labels[3])
ax.set_ylim(0, 3200)

#
# Line graph for the fraction of matched DWF to the reported total
ax2 = ax.twinx()
ax2.plot(china_dwf['year'], china_dwf['frac_matched_dwf_to_reported_total'], 
         color=colors[1], label=labels[4])
ax2.scatter(china_dwf['year'], china_dwf['frac_matched_dwf_to_reported_total'], 
            color=colors[1], s=22)
ax2.set_ylim(0., 0.64)

#
# Legends
handles, labels = ax.get_legend_handles_labels()
handles_add, labels_add = ax2.get_legend_handles_labels()
handles += handles_add
labels += labels_add
order = [2,0,3,1,4]
leg = plt.legend([handles[idx] for idx in order],
                 [labels[idx] for idx in order],
                 ncol=1, bbox_to_anchor=(0., 0.795),
                 loc='lower left', fontsize=9, frameon=True, facecolor='white' )

plt.xticks(range(2012, 2022, 1), fontsize=10)
plt.yticks(fontsize=10)
plt.xlabel('Year')
ax.set_ylabel('Number of Vessels (Bars)', fontsize=12)
ax2.set_ylabel('Fraction of Distant Water Fleet Size\nto The Reported Total (Line)', fontsize=12)

ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax2.spines['left'].set_visible(False)
ax2.spines['top'].set_visible(False)
ax2.spines['right'].set_visible(False)
ax2.spines['bottom'].set_visible(False)

ax.yaxis.grid(True, lw=0.4)
ax2.yaxis.grid(False)
ax.xaxis.grid(False)
ax.tick_params(axis='x', which='both', bottom=False, labelbottom=True, labelsize=12)
ax.tick_params(axis='y', which='both', labelsize=10)

ax.set_xlim(2011.5, 2021.5)
plt.title('Chinese Distant Water Fleet by Year', y=1.0, fontsize=18)
plt.show()
# -
# ## ANNEX: Combined Top 30 with Reported counts

rep = pd.read_csv('2016 Fleet_Update_20190812_overwrite.csv')

reported = {}
rep_chn = rep[rep['iso3'] == "CHN"]['>24'].sum()
reported['CHN'] = rep_chn
rep_rest = rep[rep['iso3'] != "CHN"]['>24'].sum()
reported['REST'] = rep_rest
for t in ['USA', 'KOR', 'RUS', 'TWN', 'IDN', 
          'JPN', 'NOR', 'ESP', 'TUR', 'PAN', 
          'ISL', 'GBR', 'ARG', 'ITA', 'IND', 
          'CAN', 'NLD', 'FRA', 'GRC', 'IRL', 
          'AUS', 'LBR', 'PER', 'DNK', 'SGP',
          'PRT', 'ZAF', 'BLZ', 'BHS']:
    reported[t] = rep[rep['iso3'] == t]['>24'].sum()
reported = pd.DataFrame.from_dict(reported, orient='index', columns=['reported'])

# +
#
# plot matched/unmatched record numbers by country for all years
colors = ['white', '#f8ba47','#204280','#d73b68']
columns={
    'unmatched_reg': 'Unmatched Registry', 
    'matched_reg_ais': 'Matched Between Registries and AIS',
    'unmatched_ais': 'Unmatched AIS'
}
fig = plt.figure(figsize=(12, 7.5), dpi=400, facecolor='#f7f7f7')
ax = fig.add_subplot(111, facecolor='white')

#
# Prepare for a diverging horizontal bar chart (Top 30, excluding China and invalid Flag code)
byctr = allyears[(allyears.flag != "CHN") & (allyears.flag.notnull())][:29]
byctr = byctr[["flag", "unmatched_reg", "matched_reg_ais", "unmatched_ais"]]

middles = byctr["unmatched_reg"]
longest = middles.max()
complete_longest = byctr["unmatched_reg"].max() + \
                    byctr[["matched_reg_ais", "unmatched_ais"]].sum(axis=1).max()
byctr.insert(0, '', (middles - longest).abs()) # Base transparent bars without name to exclude it from legends
byctr['total_ais_active'] = byctr['matched_reg_ais'] + byctr['unmatched_ais'] 
byctr = byctr.rename(columns=columns)

#
# Plot
byctr = byctr.set_index('flag').sort_values('total_ais_active', ascending=False)
byctr.drop('total_ais_active', axis=1)\
     .plot.bar(ax=ax, stacked=True, width=0.8, 
                color=colors, edgecolor='none', legend=True)
z = plt.axhline(longest, linewidth=1, linestyle='-', color='black', alpha=.5)
z.set_zorder(-1)


color_for_line = '#8abbc7'
reported['max_unmatched_reg'] = longest
reported[['max_unmatched_reg', 'reported']].iloc[2:]\
    .plot.bar(ax=ax, stacked=True, width=0.60,
               color='none', lw=2.5, linestyle='--', 
               edgecolor='none', legend=True)
for rect, ind in zip(ax.patches[-29:], reported['reported'][::-1]):
    rect.set_edgecolor(color_for_line)

#
# Ticks, labels, legends
plt.ylim(complete_longest * -0.01, 3550)#complete_longest * 1.05)
gap = 500
yvalues = range(longest % gap, complete_longest, gap)
ylabels = [str(abs(y-longest))[:-3] + ',' + str(abs(y-longest))[-3:] 
           if len(str(abs(y-longest))) > 3 else str(abs(y-longest)) for y in yvalues]
plt.yticks(yvalues, ylabels, fontsize=10)

#
# Legends
handles, labels = plt.gca().get_legend_handles_labels()
order = [2, 1, 0]
leg = plt.legend([handles[idx] for idx in order],
                 [labels[idx] for idx in order],
                 ncol=1, bbox_to_anchor=(0.98, 0.),
                 loc='lower right', fontsize=10, frameon=False )

ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
plt.xlabel(None)
ax.xaxis.grid(False)
ax.yaxis.grid(True, lw=0.4)
plt.tick_params(axis='x', which='both', bottom=False, labelbottom=True, 
                pad=-145, labelcolor='white', labelsize=8)
labels = ax.get_xticklabels()
#plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
[label.set_fontweight('bold') for label in labels]
[label.set_va('bottom') for label in labels]
plt.xticks(None)
plt.tick_params(axis='y', which='both', left=False, labelleft=True)

plt.title("Number of Vessel Identities by Top 30 Flag States (>24 m, 2012-2021)", 
          fontsize=16, pad=10)

#
#
# inset axes....
axins = ax.inset_axes([0.45, 0.7, 0.53, 0.27], facecolor='white')
#
# Prepare for a diverging horizontal bar chart
colors = ['white', '#f8ba47','#204280','#d73b68']
df_chn['label'] = "China"
df_non_chn['label'] = "Rest of\nthe World"
chn_vs_non_chn = pd.concat([df_chn, df_non_chn], sort=False)
chn_vs_non_chn = chn_vs_non_chn[["label", "unmatched_reg", "matched_reg_ais", "unmatched_ais"]]

middles = chn_vs_non_chn["unmatched_reg"]
longest = middles.max()
complete_longest = chn_vs_non_chn[["unmatched_reg", "matched_reg_ais", "unmatched_ais"]].sum(axis=1).max()
chn_vs_non_chn.insert(0, '', (middles - longest).abs())

columns = ['', 'label', 'Unmatched Registry', 
           'Matched Between Registries and AIS', 'Unmatched AIS']
chn_vs_non_chn.columns = columns

#
# Plot
chn_vs_non_chn[['', 'label', 'Unmatched Registry', 
                'Matched Between Registries and AIS',
                'Unmatched AIS']].sort_values('label', ascending=False)\
                .set_index('label')\
                .plot.barh(ax=axins, stacked=True, width=0.6, 
                           color=colors, edgecolor='none', legend=False)

# axins.spines['left'].set_visible(False)
# axins.spines['top'].set_visible(False)
# axins.spines['right'].set_visible(False)
# axins.spines['bottom'].set_visible(False)
for axis in ['top','bottom','left','right']:
    axins.spines[axis].set_linewidth(5)
    axins.spines[axis].set_color('#f7f7f7')
axins.yaxis.grid(False)
axins.xaxis.grid(True, lw=0.4)
axins.set_ylabel(None)

#
# Ticks, labels, and legends
chn_vs_non_chn = pd.concat([df_chn, df_non_chn], sort=False)
chn_vs_non_chn = chn_vs_non_chn[["label", "unmatched_reg", "matched_reg_ais", "unmatched_ais"]]
middles = chn_vs_non_chn["unmatched_reg"]
longest = middles.max()
longest_registry = chn_vs_non_chn["unmatched_reg"].max()
longest_ais = chn_vs_non_chn[["matched_reg_ais", "unmatched_ais"]].sum(axis=1).max()
complete_longest = longest_registry + longest_ais
axins.set_xlim(0 - complete_longest * 0.05, complete_longest * 1.05 +3000)
gap = 5000
xvalues = range(longest % gap, complete_longest, gap)
xlabels = [str(abs(x-longest))[:-3] + ',' + str(abs(x-longest))[-3:] 
           if abs(x-longest) != 0 else str(abs(x-longest)) for x in xvalues]
xlabels = ['' if '5,000' in x else x for x in xlabels]
axins.set_xticks(xvalues)
axins.set_xticklabels(xlabels) #, fontsize=16)
axins.axvline(longest, linewidth=0.5, linestyle='-', color='black', alpha=1)


reported['max_unmatched_reg'] = longest
reported[['max_unmatched_reg', 'reported']].iloc[:2][::-1]\
    .plot.barh(ax=axins, stacked=True, width=0.60,
               color='none', lw=2.5, linestyle='--', 
               edgecolor='none', legend=False)
for rect in axins.patches[-2:]:
    rect.set_edgecolor(color_for_line)

axins.tick_params(axis='y', which='both', left=False, labelbottom=False,
                  pad=-125, labelcolor='white', labelsize=9)
axins.tick_params(axis='x', which='both', bottom=False, labelbottom=True, zorder=5)
labels = axins.get_yticklabels()
[label.set_fontweight('bold') for label in labels]
[label.set_ha('left') for label in labels]


plt.show()

# +
# import rendered
# rendered.publish_to_github('./identity_data_stats.ipynb', 
#                            'vessel-identity-paper/', action='push')
# -


