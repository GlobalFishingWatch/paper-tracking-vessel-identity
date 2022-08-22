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

import pandas as pd
import plotly.graph_objects as go


source = [0, 0, 1, 1, 0]
target = [2, 3, 4, 5, 4]
value = [8, 2, 2, 8, 4]

link = dict(source = source, target = target, value = value)
data = go.Sankey(link = link)
print(data)

# + tags=[]
fig = go.Figure(data)
fig.show()
# -

fig.write_image("temp.png")

# data
label = ["ZERO", "ONE", "TWO", "THREE", "FOUR", "FIVE"]
source = [0, 0, 1, 1, 0]
target = [2, 3, 4, 5, 4]
value = [8, 2, 2, 8, 4]
# data to dict, dict to sankey
link = dict(source = source, target = target, value = value)
node = dict(label = label, pad=50, thickness=5)
data = go.Sankey(link = link, node=node)
# plot
fig = go.Figure(data)
fig.show()

q = f'''
WITH

all_hulls AS (
SELECT DISTINCT vessel_record_id
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
),

# Get the identities active in 2012 and rank by first_timestamp
ids_2012_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2012-12-31'
AND DATE(last_timestamp) >= '2012-01-01'
),

# Get the identities active in 2017 and rank by first_timestamp
ids_2017_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2017-12-31'
AND DATE(last_timestamp) >= '2017-01-01'
),

# Get the identities active in 2021, but unlike 2012 and 2017,
# rank by last_timestamp to get the most recent identities.
ids_2021_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY last_timestamp DESC) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2021-12-31'
AND DATE(last_timestamp) >= '2021-01-01'
),

hulls_with_flag_2012 AS (
    SELECT vessel_record_id, flag as flag_2012
    FROM ids_2012_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2017 AS (
    SELECT vessel_record_id, flag as flag_2017
    FROM ids_2017_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2021 AS (
    SELECT vessel_record_id, flag as flag_2021
    FROM ids_2021_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_joined AS (
    SELECT 
    vessel_record_id,
    IFNULL(flag_2012, 'UNK') as flag_2012,
    IFNULL(flag_2017, 'UNK') as flag_2017,
    IFNULL(flag_2021, 'UNK') as flag_2021,
    FROM all_hulls 
    LEFT JOIN hulls_with_flag_2012 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2017 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2021 USING(vessel_record_id)
)

SELECT
*
FROM hulls_with_flag_joined

'''



# +
q = f'''
WITH

all_hulls AS (
SELECT DISTINCT vessel_record_id
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
),

# Get the identities active in 2012 and rank by first_timestamp
ids_2012_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2012-12-31'
AND DATE(last_timestamp) >= '2012-01-01'
),

# Get the identities active in 2017 and rank by first_timestamp
ids_2017_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2017-12-31'
AND DATE(last_timestamp) >= '2017-01-01'
),

# Get the identities active in 2021, but unlike 2012 and 2017,
# rank by last_timestamp to get the most recent identities.
ids_2021_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY last_timestamp DESC) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2021-12-31'
AND DATE(last_timestamp) >= '2021-01-01'
),

hulls_with_flag_2012 AS (
    SELECT vessel_record_id, flag as flag_2012
    FROM ids_2012_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2017 AS (
    SELECT vessel_record_id, flag as flag_2017
    FROM ids_2017_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2021 AS (
    SELECT vessel_record_id, flag as flag_2021
    FROM ids_2021_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_joined AS (
    SELECT 
    vessel_record_id,
    IFNULL(flag_2012, 'UNK') as flag_2012,
    IFNULL(flag_2017, 'UNK') as flag_2017,
    IFNULL(flag_2021, 'UNK') as flag_2021,
    FROM all_hulls 
    LEFT JOIN hulls_with_flag_2012 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2017 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2021 USING(vessel_record_id)
),

count_2012_to_2017 AS (
    SELECT 
--    IF(flag_2012 IN ('UNK', 'PAN', 'CHN'), flag_2012, 'OTHER') as flag_2012, 
--    IF(flag_2017 IN ('UNK', 'PAN', 'CHN'), flag_2017, 'OTHER') as flag_2017,
    flag_2012, flag_2017,
    COUNT(*) as num_hulls
    FROM hulls_with_flag_joined 
    GROUP BY flag_2012, flag_2017
)

SELECT
*
FROM count_2012_to_2017
'''

df_2012_to_2017_all = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_2012_to_2017_all.groupby('flag_2017').sum().sort_values('num_hulls', ascending=False)[0:20]

# ---
# ---
# ---

flags_of_interest = ['RUS', 'EU', 'BLZ', 'NAM', 'NOR', 'CHN', 'FSM', 'ARG', 'FRO', 'CMR', 'MAR', 'NRU', 'TUR', 'ISL', 'NZL', 'UNK']

# +
q = f'''
WITH

all_hulls AS (
SELECT DISTINCT vessel_record_id
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
),

# Get the identities active in 2012 and rank by first_timestamp
ids_2012_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2012-12-31'
AND DATE(last_timestamp) >= '2012-01-01'
),

# Get the identities active in 2017 and rank by first_timestamp
ids_2017_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2017-12-31'
AND DATE(last_timestamp) >= '2017-01-01'
),

# Get the identities active in 2021, but unlike 2012 and 2017,
# rank by last_timestamp to get the most recent identities.
ids_2021_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY last_timestamp DESC) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2021-12-31'
AND DATE(last_timestamp) >= '2021-01-01'
),

hulls_with_flag_2012 AS (
    SELECT vessel_record_id, flag as flag_2012
    FROM ids_2012_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2017 AS (
    SELECT vessel_record_id, flag as flag_2017
    FROM ids_2017_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2021 AS (
    SELECT vessel_record_id, flag as flag_2021
    FROM ids_2021_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_joined AS (
    SELECT 
    vessel_record_id,
    IFNULL(flag_2012, 'UNK') as flag_2012,
    IFNULL(flag_2017, 'UNK') as flag_2017,
    IFNULL(flag_2021, 'UNK') as flag_2021,
    FROM all_hulls 
    LEFT JOIN hulls_with_flag_2012 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2017 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2021 USING(vessel_record_id)
),

hulls_with_flag_joined_eu_grouping AS (
SELECT
    * EXCEPT (flag_2012, flag_2017, flag_2021),
    IF (flag_2012 IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
    "EU", flag_2012) AS flag_2012,
    IF (flag_2017 IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
    "EU", flag_2017) AS flag_2017,
    IF (flag_2021 IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
    "EU", flag_2021) AS flag_2021,
FROM hulls_with_flag_joined
),

count_2012_to_2021 AS (
    SELECT 
    IF(flag_2012 IN UNNEST({flags_of_interest}), flag_2012, 'OTHER') as flag_2012, 
    IF(flag_2021 IN UNNEST({flags_of_interest}), flag_2021, 'OTHER') as flag_2021,
    COUNT(*) as num_hulls
    FROM hulls_with_flag_joined_eu_grouping 
    GROUP BY flag_2012, flag_2021
)

SELECT
*
FROM count_2012_to_2021
'''

df_2012_to_2021 = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_2012_to_2021

flags_2012 = list(set(df_2012_to_2021.flag_2012.unique().tolist()))
flags_2021 = list(set(df_2012_to_2021.flag_2021.unique().tolist()))
flags_2021_only = list(set(df_2012_to_2021.flag_2021.unique().tolist()).difference(set(flags_2012)))
label = flags_2012 + flags_2021

# +
dict_replace_2012 = {}
for i in range(len(flags_2012)):
    dict_replace_2012[flags_2012[i]] = i

dict_replace_2021 = {}
for i in range(len(flags_2021)):
    dict_replace_2021[flags_2021[i]] = i + len(flags_2012)

# Replace the flags with their index in the label list
# since plotly requires integers
sankey_2012_to_2021 = df_2012_to_2021.copy()
sankey_2012_to_2021.replace({'flag_2012': dict_replace_2012, 'flag_2021': dict_replace_2021}, inplace=True)
sankey_2012_to_2021
# -

# data
# label = ["ZERO", "ONE", "TWO", "THREE", "FOUR", "FIVE"]
source = sankey_2012_to_2021.flag_2012.to_list()
target = sankey_2012_to_2021.flag_2021.to_list()
value = sankey_2012_to_2021.num_hulls.to_list()
# data to dict, dict to sankey
link = dict(source = source, target = target, value = value)
node = dict(label = label, pad=50, thickness=5)
data = go.Sankey(link = link, node=node)
# plot
fig = go.Figure(data)
fig.show()



# ---
# ---
# ---

# flags_of_interest_fishing = ['RUS', 'EU', 'BLZ', 'NAM', 'NOR', 'CHN', 'FSM', 'ARG', 'FRO', 'CMR', 'MAR', 'NRU', 'TUR', 'ISL', 'NZL', 'UNK']
flags_of_interest_fishing = ['RUS', 'EU', 'UNK', 'ITF_FOC', 'ARG', 'NOR', 'NAM', 'FSM', 'CHN', 'TUR', 'CMR', 'NRU', 'MAR', 'ISL', 'NZL']

# +
q = f'''
CREATE TEMP FUNCTION process_flag(flag STRING) AS (
    CASE
        WHEN flag IS NULL THEN 'UNK'
        WHEN flag IN (
            'ATG', 'BHS', 'BRB', 'BLZ', 'BMU', 
            'KHM', 'CYM', 'COM', 'CYP', 'GNQ', 
            'FRO', 'GEO', 'GIB', 'HND', 'JAM', 
            'LBN', 'LBR', 'MLT', 'PMD', 'MHL', 
            'MUS', 'MDA', 'MNG', 'MMR', 'ANT', 
            'PRK', 'PAN', 'STP', 'VCT', 'LKA', 
            'TON', 'VUT') THEN 'ITF_FOC'
        WHEN flag IN (
            'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
            'CZE', 'DNK', 'EST', 'FIN', 'FRA',
            'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
            'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
            'POL', 'PRT', 'ROU', 'SVK', 'SVN',
            'ESP', 'SWE', 'GBR') THEN 'EU'
        ELSE flag
    END
);

WITH

reflagging_hulls AS (
SELECT DISTINCT vessel_record_id
FROM `world-fishing-827.vessel_identity.reflagging_flag_in_out_v20210701`
WHERE is_fishing
),

# Get the identities active in 2012 and rank by first_timestamp
ids_2012_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2012-12-31'
AND DATE(last_timestamp) >= '2012-01-01'
),

# Get the identities active in 2017 and rank by first_timestamp
ids_2017_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2017-12-31'
AND DATE(last_timestamp) >= '2017-01-01'
),

# Get the identities active in 2021, but unlike 2012 and 2017,
# rank by last_timestamp to get the most recent identities.
ids_2021_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY last_timestamp DESC) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2021-12-31'
AND DATE(last_timestamp) >= '2021-01-01'
),

hulls_with_flag_2012 AS (
    SELECT vessel_record_id, flag as flag_2012
    FROM ids_2012_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2017 AS (
    SELECT vessel_record_id, flag as flag_2017
    FROM ids_2017_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2021 AS (
    SELECT vessel_record_id, flag as flag_2021
    FROM ids_2021_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_joined AS (
    SELECT 
    vessel_record_id,
    process_flag(flag_2012) as flag_2012,
    process_flag(flag_2017) as flag_2017,
    process_flag(flag_2021) as flag_2021,
    FROM reflagging_hulls 
    LEFT JOIN hulls_with_flag_2012 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2017 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2021 USING(vessel_record_id)
),

count_2012_to_2021 AS (
    SELECT 
    IF(flag_2012 IN UNNEST({flags_of_interest_fishing}), flag_2012, 'OTHER') as flag_2012, 
    IF(flag_2021 IN UNNEST({flags_of_interest_fishing}), flag_2021, 'OTHER') as flag_2021,
--    flag_2012, flag_2021,
    COUNT(*) as num_hulls
    FROM hulls_with_flag_joined 
    GROUP BY flag_2012, flag_2021
)

SELECT
*
FROM count_2012_to_2021
'''

df_2012_to_2021_fishing = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_2012_to_2021_fishing

print(df_2012_to_2021_fishing.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False)[0:15].index.to_list())







# +
flags_2012 = df_2012_to_2021_fishing.groupby('flag_2012').sum('num_hulls').sort_values('num_hulls', ascending=False).index.tolist()
num_hulls_2012 = df_2012_to_2021_fishing.groupby('flag_2012').sum('num_hulls').sort_values('num_hulls', ascending=False).num_hulls.tolist()
flags_2021 = df_2012_to_2021_fishing.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False).index.tolist()
num_hulls_2021 = df_2012_to_2021_fishing.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False).num_hulls.tolist()

label = flags_2012 + flags_2021

# +
dict_replace_2012 = {}
for i in range(len(flags_2012)):
    dict_replace_2012[flags_2012[i]] = i

dict_replace_2021 = {}
for i in range(len(flags_2021)):
    dict_replace_2021[flags_2021[i]] = i + len(flags_2012)

# Replace the flags with their index in the label list
# since plotly requires integers
sankey_2012_to_2021 = df_2012_to_2021_fishing.copy()
sankey_2012_to_2021['flag_2012_orig'] = sankey_2012_to_2021.flag_2012
sankey_2012_to_2021['flag_2021_orig'] = sankey_2012_to_2021.flag_2021
sankey_2012_to_2021.replace({'flag_2012': dict_replace_2012, 'flag_2021': dict_replace_2021}, inplace=True)
sankey_2012_to_2021

# +
buffer = 0.00001

# node x positions
label_x = [buffer for i in range(len(flags_2012))] + [1 for i in range(len(flags_2021))]

# node y positions
label_y_2012 = []
step = 1/len(flags_2012)
for i in range(len(flags_2012)):
    if i == 0:
        label_y_2012.append(0)
    else:
        label_y_2012.append(label_y_2012[i-1] + step + (num_hulls_2012[i-1]/sum(num_hulls_2012)))

label_y_2012 = [l/max(label_y_2012) for l in label_y_2012]
label_y_2012[0] = buffer

label_y_2021 = []
step = 1/len(flags_2021)
for i in range(len(flags_2021)):
    if i == 0:
        label_y_2021.append(0)
    else:
        label_y_2021.append(label_y_2021[i-1] + step + (num_hulls_2021[i-1]/sum(num_hulls_2021)))

label_y_2021 = [l/max(label_y_2021) for l in label_y_2021]
label_y_2021[0] = buffer

#
label_y = label_y_2012 + label_y_2021

# +
colors_full = ['#204280', '#742980', '#ad2176', '#d73b68', '#ee6256', '#f68d4b', '#f8ba47', '#ebe55d']
colors_40 = ['#989abc', '#b79abe', '#d6a2bb', '#ebaeb4', '#f8bdad', '#fdceac', '#fde0b1', '#f5f3bf']
light_gray = '#e6e7eb'
light_gray_40 = '#f7f7f7'
dark_gray = '#363c4c'
med_gray = '#848b9b'

color_map_node = {'OTHER': dark_gray, 'UNK': light_gray}
i = 0
for l in label:
    if l not in color_map_node:
        color_map_node[l] = colors_full[i%len(colors_full)]
        i += 1

color_map_link = {'OTHER': med_gray, 'UNK': med_gray_40}
i = 0
for l in label:
    if l not in color_map_link:
        color_map_link[l] = colors_40[i%len(colors_40)]
        i += 1
        

# +
# data
# label = ["ZERO", "ONE", "TWO", "THREE", "FOUR", "FIVE"]
source = sankey_2012_to_2021.flag_2012.to_list()
target = sankey_2012_to_2021.flag_2021.to_list()
value = sankey_2012_to_2021.num_hulls.to_list()

# colors
color_node = [color_map_node[l] for l in label]
color_link = [color_map_link[s] for s in sankey_2012_to_2021.flag_2012_orig.to_list()]


# data to dict, dict to sankey
link = dict(source = source, target = target, value = value, color=color_link)
node = dict(label = label, color=color_node, pad=15, thickness=5, x=label_x, y=label_y)
data = go.Sankey(link = link, node=node, arrangement='fixed')
# plot
fig = go.Figure(data)

fig.update_layout(
    autosize=False,
    width=800,
    height=600,
    margin=dict(
        l=50,
        r=50,
        b=50,
        t=100,
        pad=4
    ),
    paper_bgcolor="white",
)

fig.show()
# -

# ---
# ---
# ---

flags_of_interest_supply = ['ITF_FOC', 'UNK', 'RUS', 'KOR', 'EU', 'CHL', 'SGP', 'COK', 'NOR', 'KNA', 'SLE', 'IDN', 'TUR', 'DMA', 'KIR']

# +
q = f'''
CREATE TEMP FUNCTION process_flag(flag STRING) AS (
    CASE
        WHEN flag IS NULL THEN 'UNK'
        WHEN flag IN (
            'ATG', 'BHS', 'BRB', 'BLZ', 'BMU', 
            'KHM', 'CYM', 'COM', 'CYP', 'GNQ', 
            'FRO', 'GEO', 'GIB', 'HND', 'JAM', 
            'LBN', 'LBR', 'MLT', 'PMD', 'MHL', 
            'MUS', 'MDA', 'MNG', 'MMR', 'ANT', 
            'PRK', 'PAN', 'STP', 'VCT', 'LKA', 
            'TON', 'VUT') THEN 'ITF_FOC'
        WHEN flag IN (
            'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
            'CZE', 'DNK', 'EST', 'FIN', 'FRA',
            'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
            'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
            'POL', 'PRT', 'ROU', 'SVK', 'SVN',
            'ESP', 'SWE', 'GBR') THEN 'EU'
        ELSE flag
    END
);

WITH

reflagging_hulls AS (
SELECT DISTINCT vessel_record_id
FROM `world-fishing-827.vessel_identity.reflagging_flag_in_out_v20210701`
WHERE is_bunker OR is_carrier
),

# Get the identities active in 2012 and rank by first_timestamp
ids_2012_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2012-12-31'
AND DATE(last_timestamp) >= '2012-01-01'
),

# Get the identities active in 2017 and rank by first_timestamp
ids_2017_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2017-12-31'
AND DATE(last_timestamp) >= '2017-01-01'
),

# Get the identities active in 2021, but unlike 2012 and 2017,
# rank by last_timestamp to get the most recent identities.
ids_2021_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY last_timestamp DESC) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2021-12-31'
AND DATE(last_timestamp) >= '2021-01-01'
),

hulls_with_flag_2012 AS (
    SELECT vessel_record_id, flag as flag_2012
    FROM ids_2012_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2017 AS (
    SELECT vessel_record_id, flag as flag_2017
    FROM ids_2017_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2021 AS (
    SELECT vessel_record_id, flag as flag_2021
    FROM ids_2021_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_joined AS (
    SELECT 
    vessel_record_id,
    process_flag(flag_2012) as flag_2012,
    process_flag(flag_2017) as flag_2017,
    process_flag(flag_2021) as flag_2021,
    FROM reflagging_hulls 
    LEFT JOIN hulls_with_flag_2012 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2017 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2021 USING(vessel_record_id)
),

count_2012_to_2021 AS (
    SELECT 
    IF(flag_2012 IN UNNEST({flags_of_interest_fishing}), flag_2012, 'OTHER') as flag_2012, 
    IF(flag_2021 IN UNNEST({flags_of_interest_fishing}), flag_2021, 'OTHER') as flag_2021,
--    flag_2012, flag_2021,
    COUNT(*) as num_hulls
    FROM hulls_with_flag_joined 
    GROUP BY flag_2012, flag_2021
)

SELECT
*
FROM count_2012_to_2021
'''

df_2012_to_2021_support = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_2012_to_2021_support

print(df_2012_to_2021_support.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False)[0:15].index.to_list())





# +
flags_2012 = df_2012_to_2021_support.groupby('flag_2012').sum('num_hulls').sort_values('num_hulls', ascending=False).index.tolist()
num_hulls_2012 = df_2012_to_2021_support.groupby('flag_2012').sum('num_hulls').sort_values('num_hulls', ascending=False).num_hulls.tolist()
flags_2021 = df_2012_to_2021_support.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False).index.tolist()
num_hulls_2021 = df_2012_to_2021_support.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False).num_hulls.tolist()

label = flags_2012 + flags_2021

# +
dict_replace_2012 = {}
for i in range(len(flags_2012)):
    dict_replace_2012[flags_2012[i]] = i

dict_replace_2021 = {}
for i in range(len(flags_2021)):
    dict_replace_2021[flags_2021[i]] = i + len(flags_2012)

# Replace the flags with their index in the label list
# since plotly requires integers
sankey_2012_to_2021 = df_2012_to_2021_support.copy()
sankey_2012_to_2021['flag_2012_orig'] = sankey_2012_to_2021.flag_2012
sankey_2012_to_2021['flag_2021_orig'] = sankey_2012_to_2021.flag_2021
sankey_2012_to_2021.replace({'flag_2012': dict_replace_2012, 'flag_2021': dict_replace_2021}, inplace=True)
sankey_2012_to_2021

# +
buffer = 0.00001

# node x positions
label_x = [buffer for i in range(len(flags_2012))] + [1 for i in range(len(flags_2021))]

# node y positions
label_y_2012 = []
step = 1/len(flags_2012)
for i in range(len(flags_2012)):
    if i == 0:
        label_y_2012.append(0)
    else:
        label_y_2012.append(label_y_2012[i-1] + step + (num_hulls_2012[i-1]/sum(num_hulls_2012)))

label_y_2012 = [l/max(label_y_2012) for l in label_y_2012]
label_y_2012[0] = buffer

label_y_2021 = []
step = 1/len(flags_2021)
for i in range(len(flags_2021)):
    if i == 0:
        label_y_2021.append(0)
    else:
        label_y_2021.append(label_y_2021[i-1] + step + (num_hulls_2021[i-1]/sum(num_hulls_2021)))

label_y_2021 = [l/max(label_y_2021) for l in label_y_2021]
label_y_2021[0] = buffer

#
label_y = label_y_2012 + label_y_2021

# +
colors_full = ['#204280', '#742980', '#ad2176', '#d73b68', '#ee6256', '#f68d4b', '#f8ba47', '#ebe55d']
colors_40 = ['#989abc', '#b79abe', '#d6a2bb', '#ebaeb4', '#f8bdad', '#fdceac', '#fde0b1', '#f5f3bf']
light_gray = '#e6e7eb'
light_gray_40 = '#f7f7f7'
dark_gray = '#363c4c'
med_gray = '#848b9b'

color_map_node = {'OTHER': dark_gray, 'UNK': light_gray}
i = 0
for l in label:
    if l not in color_map_node:
        color_map_node[l] = colors_full[i%len(colors_full)]
        i += 1

color_map_link = {'OTHER': med_gray, 'UNK': med_gray_40}
i = 0
for l in label:
    if l not in color_map_link:
        color_map_link[l] = colors_40[i%len(colors_40)]
        i += 1
        

# +
# data
# label = ["ZERO", "ONE", "TWO", "THREE", "FOUR", "FIVE"]
source = sankey_2012_to_2021.flag_2012.to_list()
target = sankey_2012_to_2021.flag_2021.to_list()
value = sankey_2012_to_2021.num_hulls.to_list()

# colors
color_node = [color_map_node[l] for l in label]
color_link = [color_map_link[s] for s in sankey_2012_to_2021.flag_2012_orig.to_list()]


# data to dict, dict to sankey
link = dict(source = source, target = target, value = value, color=color_link)
node = dict(label = label, color=color_node, pad=15, thickness=5, x=label_x, y=label_y)
data = go.Sankey(link = link, node=node, arrangement='fixed')
# plot
fig = go.Figure(data)

fig.update_layout(
    autosize=False,
    width=800,
    height=600,
    margin=dict(
        l=50,
        r=50,
        b=50,
        t=100,
        pad=4
    ),
    paper_bgcolor="white",
)

fig.show()
# -

# ---
# # ALL VESSELS

# ---

flags_of_interest_all_fishing = ['EU', 'UNK', 'USA', 'NOR', 'CHN', 'TWN', 'RUS', 'CAN', 'IDN', 'TUR', 'JPN', 'ITF_FOC', 'AUS', 'ARG', 'ISL']

# +
q = f'''
CREATE TEMP FUNCTION process_flag(flag STRING) AS (
    CASE
        WHEN flag IS NULL THEN 'UNK'
        WHEN flag IN (
            'ATG', 'BHS', 'BRB', 'BLZ', 'BMU', 
            'KHM', 'CYM', 'COM', 'CYP', 'GNQ', 
            'FRO', 'GEO', 'GIB', 'HND', 'JAM', 
            'LBN', 'LBR', 'MLT', 'PMD', 'MHL', 
            'MUS', 'MDA', 'MNG', 'MMR', 'ANT', 
            'PRK', 'PAN', 'STP', 'VCT', 'LKA', 
            'TON', 'VUT') THEN 'ITF_FOC'
        WHEN flag IN (
            'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
            'CZE', 'DNK', 'EST', 'FIN', 'FRA',
            'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
            'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
            'POL', 'PRT', 'ROU', 'SVK', 'SVN',
            'ESP', 'SWE', 'GBR') THEN 'EU'
        ELSE flag
    END
);

WITH

all_hulls AS (
SELECT DISTINCT vessel_record_id
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE is_fishing
),

# Get the identities active in 2012 and rank by first_timestamp
ids_2012_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2012-12-31'
AND DATE(last_timestamp) >= '2012-01-01'
),

# Get the identities active in 2017 and rank by first_timestamp
ids_2017_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2017-12-31'
AND DATE(last_timestamp) >= '2017-01-01'
),

# Get the identities active in 2021, but unlike 2012 and 2017,
# rank by last_timestamp to get the most recent identities.
ids_2021_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY last_timestamp DESC) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2021-12-31'
AND DATE(last_timestamp) >= '2021-01-01'
),

hulls_with_flag_2012 AS (
    SELECT vessel_record_id, flag as flag_2012
    FROM ids_2012_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2017 AS (
    SELECT vessel_record_id, flag as flag_2017
    FROM ids_2017_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2021 AS (
    SELECT vessel_record_id, flag as flag_2021
    FROM ids_2021_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_joined AS (
    SELECT 
    vessel_record_id,
    process_flag(flag_2012) as flag_2012,
    process_flag(flag_2017) as flag_2017,
    process_flag(flag_2021) as flag_2021,
    FROM all_hulls 
    LEFT JOIN hulls_with_flag_2012 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2017 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2021 USING(vessel_record_id)
),

count_2012_to_2021 AS (
    SELECT 
    IF(flag_2017 IN UNNEST({flags_of_interest_all_fishing}), flag_2017, 'OTHER') as flag_2012, 
    IF(flag_2021 IN UNNEST({flags_of_interest_all_fishing}), flag_2021, 'OTHER') as flag_2021,
--    flag_2012, flag_2021,
    COUNT(*) as num_hulls
    FROM hulls_with_flag_joined 
    GROUP BY flag_2017, flag_2021
)

SELECT
*
FROM count_2012_to_2021
'''

df_2012_to_2021_all_fishing = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_2012_to_2021_all_fishing

print(df_2012_to_2021_all_fishing.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False)[0:15].index.to_list())







# +
flags_2012 = df_2012_to_2021_all_fishing.groupby('flag_2012').sum('num_hulls').sort_values('num_hulls', ascending=False).index.tolist()
num_hulls_2012 = df_2012_to_2021_all_fishing.groupby('flag_2012').sum('num_hulls').sort_values('num_hulls', ascending=False).num_hulls.tolist()
flags_2021 = df_2012_to_2021_all_fishing.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False).index.tolist()
num_hulls_2021 = df_2012_to_2021_all_fishing.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False).num_hulls.tolist()

label = flags_2012 + flags_2021

# +
dict_replace_2012 = {}
for i in range(len(flags_2012)):
    dict_replace_2012[flags_2012[i]] = i

dict_replace_2021 = {}
for i in range(len(flags_2021)):
    dict_replace_2021[flags_2021[i]] = i + len(flags_2012)

# Replace the flags with their index in the label list
# since plotly requires integers
sankey_2012_to_2021 = df_2012_to_2021_all_fishing.copy()
sankey_2012_to_2021['flag_2012_orig'] = sankey_2012_to_2021.flag_2012
sankey_2012_to_2021['flag_2021_orig'] = sankey_2012_to_2021.flag_2021
sankey_2012_to_2021.replace({'flag_2012': dict_replace_2012, 'flag_2021': dict_replace_2021}, inplace=True)
sankey_2012_to_2021

# +
buffer = 0.00001

# node x positions
label_x = [buffer for i in range(len(flags_2012))] + [1 for i in range(len(flags_2021))]

# node y positions
label_y_2012 = []
step = 1/len(flags_2012)
for i in range(len(flags_2012)):
    if i == 0:
        label_y_2012.append(0)
    else:
        label_y_2012.append(label_y_2012[i-1] + step + (num_hulls_2012[i-1]/sum(num_hulls_2012)))

label_y_2012 = [l/max(label_y_2012) for l in label_y_2012]
label_y_2012[0] = buffer

label_y_2021 = []
step = 1/len(flags_2021)
for i in range(len(flags_2021)):
    if i == 0:
        label_y_2021.append(0)
    else:
        label_y_2021.append(label_y_2021[i-1] + step + (num_hulls_2021[i-1]/sum(num_hulls_2021)))

label_y_2021 = [l/max(label_y_2021) for l in label_y_2021]
label_y_2021[0] = buffer

#
label_y = label_y_2012 + label_y_2021

# +
colors_full = ['#204280', '#742980', '#ad2176', '#d73b68', '#ee6256', '#f68d4b', '#f8ba47', '#ebe55d']
colors_40 = ['#989abc', '#b79abe', '#d6a2bb', '#ebaeb4', '#f8bdad', '#fdceac', '#fde0b1', '#f5f3bf']
light_gray = '#e6e7eb'
light_gray_40 = '#f7f7f7'
dark_gray = '#363c4c'
med_gray = '#848b9b'

color_map_node = {'OTHER': dark_gray, 'UNK': light_gray}
i = 0
for l in label:
    if l not in color_map_node:
        color_map_node[l] = colors_full[i%len(colors_full)]
        i += 1

color_map_link = {'OTHER': med_gray, 'UNK': med_gray_40}
i = 0
for l in label:
    if l not in color_map_link:
        color_map_link[l] = colors_40[i%len(colors_40)]
        i += 1
        

# +
# data
# label = ["ZERO", "ONE", "TWO", "THREE", "FOUR", "FIVE"]
source = sankey_2012_to_2021.flag_2012.to_list()
target = sankey_2012_to_2021.flag_2021.to_list()
value = sankey_2012_to_2021.num_hulls.to_list()

# colors
color_node = [color_map_node[l] for l in label]
color_link = [color_map_link[s] for s in sankey_2012_to_2021.flag_2012_orig.to_list()]


# data to dict, dict to sankey
link = dict(source = source, target = target, value = value, color=color_link)
node = dict(label = label, color=color_node, pad=15, thickness=5, x=label_x, y=label_y)
data = go.Sankey(link = link, node=node, arrangement='fixed')
# plot
fig = go.Figure(data)

fig.update_layout(
    autosize=False,
    width=800,
    height=600,
    margin=dict(
        l=50,
        r=50,
        b=50,
        t=100,
        pad=4
    ),
    paper_bgcolor="white",
)

fig.show()
# -



















# ---
# ---
# ---

flags_of_interest_supply = ['PAN', 'RUS', 'EU', 'KOR', 'BHS', 'TGO', 'CHL', 'MNG', 'SLE', 'COK', 'KIR', 'LBR', 'PLW', 'KNA', 'NOR', 'UNK']

# +
q = f'''
WITH

reflagging_hulls AS (
SELECT DISTINCT vessel_record_id
FROM `world-fishing-827.vessel_identity.reflagging_flag_in_out_v20210701`
WHERE is_bunker OR is_carrier
),

# Get the identities active in 2012 and rank by first_timestamp
ids_2012_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2012-12-31'
AND DATE(last_timestamp) >= '2012-01-01'
),

# Get the identities active in 2017 and rank by first_timestamp
ids_2017_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2017-12-31'
AND DATE(last_timestamp) >= '2017-01-01'
),

# Get the identities active in 2021, but unlike 2012 and 2017,
# rank by last_timestamp to get the most recent identities.
ids_2021_ranked AS (
SELECT
*,
RANK() OVER (PARTITION BY vessel_record_id ORDER BY last_timestamp DESC) as rank
FROM `world-fishing-827.vessel_identity.identity_core_v20210701`
WHERE DATE(first_timestamp) <= '2021-12-31'
AND DATE(last_timestamp) >= '2021-01-01'
),

hulls_with_flag_2012 AS (
    SELECT vessel_record_id, flag as flag_2012
    FROM ids_2012_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2017 AS (
    SELECT vessel_record_id, flag as flag_2017
    FROM ids_2017_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_2021 AS (
    SELECT vessel_record_id, flag as flag_2021
    FROM ids_2021_ranked 
    WHERE rank = 1
    AND flag IS NOT NULL
),

hulls_with_flag_joined AS (
    SELECT 
    vessel_record_id,
    IFNULL(flag_2012, 'UNK') as flag_2012,
    IFNULL(flag_2017, 'UNK') as flag_2017,
    IFNULL(flag_2021, 'UNK') as flag_2021,
    FROM reflagging_hulls 
    LEFT JOIN hulls_with_flag_2012 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2017 USING(vessel_record_id)
    LEFT JOIN hulls_with_flag_2021 USING(vessel_record_id)
),

hulls_with_flag_joined_eu_grouping AS (
SELECT
    * EXCEPT (flag_2012, flag_2017, flag_2021),
    IF (flag_2012 IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
    "EU", flag_2012) AS flag_2012,
    IF (flag_2017 IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
    "EU", flag_2017) AS flag_2017,
    IF (flag_2021 IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
    "EU", flag_2021) AS flag_2021,
FROM hulls_with_flag_joined
),

count_2012_to_2021 AS (
    SELECT 
    IF(flag_2012 IN UNNEST({flags_of_interest_supply}), flag_2012, 'OTHER') as flag_2012, 
    IF(flag_2021 IN UNNEST({flags_of_interest_supply}), flag_2021, 'OTHER') as flag_2021,
    COUNT(*) as num_hulls
    FROM hulls_with_flag_joined_eu_grouping 
    GROUP BY flag_2012, flag_2021
)

SELECT
*
FROM count_2012_to_2021
'''

df_2012_to_2021_support = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
# -

df_2012_to_2021_support

# +
flags_2012 = df_2012_to_2021_support.groupby('flag_2012').sum('num_hulls').sort_values('num_hulls', ascending=False).index.tolist()
num_hulls_2012 = df_2012_to_2021_support.groupby('flag_2012').sum('num_hulls').sort_values('num_hulls', ascending=False).num_hulls.tolist()
flags_2021 = df_2012_to_2021_support.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False).index.tolist()
num_hulls_2021 = df_2012_to_2021_support.groupby('flag_2021').sum('num_hulls').sort_values('num_hulls', ascending=False).num_hulls.tolist()

label = flags_2012 + flags_2021

# +
dict_replace_2012 = {}
for i in range(len(flags_2012)):
    dict_replace_2012[flags_2012[i]] = i

dict_replace_2021 = {}
for i in range(len(flags_2021)):
    dict_replace_2021[flags_2021[i]] = i + len(flags_2012)

# Replace the flags with their index in the label list
# since plotly requires integers
sankey_2012_to_2021 = df_2012_to_2021_support.copy()
sankey_2012_to_2021['flag_2012_orig'] = sankey_2012_to_2021.flag_2012
sankey_2012_to_2021['flag_2021_orig'] = sankey_2012_to_2021.flag_2021
sankey_2012_to_2021.replace({'flag_2012': dict_replace_2012, 'flag_2021': dict_replace_2021}, inplace=True)
sankey_2012_to_2021

# +
buffer = 0.00001

# node x positions
label_x = [buffer for i in range(len(flags_2012))] + [1 for i in range(len(flags_2021))]

# node y positions
label_y_2012 = []
step = 1/len(flags_2012)
for i in range(len(flags_2012)):
    if i == 0:
        label_y_2012.append(0)
    else:
        label_y_2012.append(label_y_2012[i-1] + step + (num_hulls_2012[i-1]/sum(num_hulls_2012)))

label_y_2012 = [l/max(label_y_2012) for l in label_y_2012]
label_y_2012[0] = buffer

label_y_2021 = []
step = 1/len(flags_2021)
for i in range(len(flags_2021)):
    if i == 0:
        label_y_2021.append(0)
    else:
        label_y_2021.append(label_y_2021[i-1] + step + (num_hulls_2021[i-1]/sum(num_hulls_2021)))

label_y_2021 = [l/max(label_y_2021) for l in label_y_2021]
label_y_2021[0] = buffer

#
label_y = label_y_2012 + label_y_2021

# +
colors_full = ['#204280', '#742980', '#ad2176', '#d73b68', '#ee6256', '#f68d4b', '#f8ba47', '#ebe55d']
colors_40 = ['#989abc', '#b79abe', '#d6a2bb', '#ebaeb4', '#f8bdad', '#fdceac', '#fde0b1', '#f5f3bf']
light_gray = '#e6e7eb'
light_gray_40 = '#f7f7f7'
med_gray = '#848b9b'
med_gray_40 = '#e6e7eb'

color_map_node = {'OTHER': med_gray, 'UNK': light_gray}
i = 0
for l in label:
    if l not in color_map_node:
        color_map_node[l] = colors_full[i%len(colors_full)]
        i += 1

color_map_link = {'OTHER': med_gray_40, 'UNK': med_gray_40}
i = 0
for l in label:
    if l not in color_map_link:
        color_map_link[l] = colors_40[i%len(colors_40)]
        i += 1
        

# +
# data
# label = ["ZERO", "ONE", "TWO", "THREE", "FOUR", "FIVE"]
source = sankey_2012_to_2021.flag_2012.to_list()
target = sankey_2012_to_2021.flag_2021.to_list()
value = sankey_2012_to_2021.num_hulls.to_list()

# colors
color_node = [color_map_node[l] for l in label]
color_link = [color_map_link[s] for s in sankey_2012_to_2021.flag_2012_orig.to_list()]


# data to dict, dict to sankey
link = dict(source = source, target = target, value = value, color=color_link)
node = dict(label = label, color=color_node, pad=15, thickness=5, x=label_x, y=label_y)
data = go.Sankey(link = link, node=node, arrangement='fixed')
# plot
fig = go.Figure(data)

fig.update_layout(
    autosize=False,
    width=800,
    height=600,
    margin=dict(
        l=50,
        r=50,
        b=50,
        t=100,
        pad=4
    ),
    paper_bgcolor="white",
)

fig.show()
# -




