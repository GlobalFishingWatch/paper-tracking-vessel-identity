# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import matplotlib.pyplot as plt
import os
import pandas as pd
import numpy as np
import seaborn as sns
from math import pi
import pylab
from jinja2 import Template
from google.cloud import bigquery


pink = tuple(np.asarray([255, 69, 115]) / 255)
blue = tuple(np.asarray([0, 59, 118]) / 255)
yellow = tuple(np.asarray([235, 229, 93]) / 255)
orange = tuple(np.asarray([248, 186, 71]) / 255)
gray = tuple(np.asarray([60, 60, 59]) / 255)
green = tuple(np.asarray([56, 144, 136]) / 255)
bg_color = "#f7f7f7"
rfmos = [
    "CCSBT",
    "IATTC",
    "ICCAT",
    "IOTC",
    "WCPFC",
    "NPFC",
    "SPRFMO",
    "FFA",
    "SEAFO",
    "NEAFC",
    "NAFO",
    "GFCM",
    "CCAMLR",
    "SIOFA",
]
rfmos = sorted(rfmos)


# +
figures_folder = "./figures/"

if not os.path.exists(figures_folder):
    os.makedirs(figures_folder)
# -

from queries.config import (
    IDENTITY_CORE_TABLE,
    NN_REGISTRY_TABLE,
    ALL_VESSELS_TABLE,
    VESSEL_INFO_TABLE
)

training_mmsi = tuple(pd.read_csv('queries/vc_training_mmsi_v20210906.csv').id.astype(str).values)


# +
# Set to True if you'd like to run the query and save the table (overwriting if it already exists)
# Set to False if you'd like to just pull the information from an existing table
run_ownership_by_mmsi = True

# If this table needs to be run (or rerun), do that and save/overwrite to BQ.
if run_ownership_by_mmsi:
    with open("queries/vessel_info_for_comparison.sql.j2") as f:
        sql_template = Template(f.read())

    # Format the query according to the desired features
    q = sql_template.render(
        identity_core_table=IDENTITY_CORE_TABLE,
        nn_registry_table=NN_REGISTRY_TABLE,
        all_vessels_table=ALL_VESSELS_TABLE,
        vessel_info_table=VESSEL_INFO_TABLE,
        training_mmsi=training_mmsi,
    )
    # print(q)
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination=NN_REGISTRY_TABLE, write_disposition="WRITE_TRUNCATE"
    )
    # Start the query, passing in the extra configuration.
    query_job = client.query(q, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(NN_REGISTRY_TABLE))
# -

# - This query retrieves geartype, tonnage, engine power, and length information for fishing, carrier and bunker vessels for the purpose of comparing raw registry information, and aggregated registry information, with model predictions.
# - aggregated1 is derived from an aggregation across registries
# - aggregated2 is derived from an aggregation across Registries and identities
# - To make a fair analysis of using the model predictions, vessels used to train the model were filtered out.
# - udfs.determine_class() is used to find most fine-grained common class from 2 input classes. This is helpful to determine if two classes are within the same hierarchy as each other.
#

query = f"""
select *,
udfs.determine_class(CONCAT(vessel_class_aggregated1, '|', vessel_class_inferred)) AS combo_agg_aggregated1,
udfs.determine_class(CONCAT(vessel_class_aggregated2, '|', vessel_class_inferred)) AS combo_agg_aggregated2,
udfs.determine_class(CONCAT(vessel_class_registry, '|', vessel_class_inferred)) AS combo_agg_reg
from {NN_REGISTRY_TABLE}
"""
df = pd.read_gbq(query, project_id="world-fishing-827", dialect="standard")


# +
def get_reg_order(
    df,
    registries,
    metrics=[
        "vessel_class_registry",
        "length_m_registry",
        "tonnage_gt_registry",
        "engine_power_kw_registry",
    ],
):
    """Get the list of indexes of registries ordered by registry score

    Parameters
    ----------
    df : Dataframe
    registries : list of strings
        list of registries to sort
    metrics : list of strings
        metrics used to compute score

    Returns
    ------
    array of indexes sorted by registry score
    """
    registry_scores = []
    for r in registries:
        filtered = df[(df["registries_listed"] == r)]
        sum_by_metric = []
        for m in metrics:
            sum_by_metric.append(sum(filtered[m].notnull()))

        sum_reg = sum(np.asarray(sum_by_metric) / len(filtered))
        registry_scores.append(sum_reg)

    return np.argsort(registry_scores)[::-1]


def combine_bools(bools):
    """take a list of masks and logical OR them

    Parameters
    ----------
    bools : list of booleans

    Returns
    ------
    list of booleans
    """
    all_bool = bools[0]
    for b in bools[1:]:
        all_bool = np.asarray(all_bool) | np.asarray(b)
    return all_bool


def get_scores(
    df,
    registry,
    metrics,
    source,
    combined="combined",
    exclude_classes=["fishing", "non_fishing", "unclassified", None],
):

    """Compute scores for how much information is available for vessels for a given registry

    Parameters
    ----------
    df : Dataframe
    registry : string
        registry to compute scores for
    metrics : list of strings
        metrics used to compute score
    source : list of strings
        name of information source
    combined : string
        name of field to save combined score under
    exclude_classes : list of strings
        don't count these as valid information

    Returns
    ------
    scores : record
        scores computed for each metric and source
    """
    filtered = df[(df["registries_listed"] == registry)]
    assert len(filtered) == len(filtered.ssvid.unique())
    scores = {}
    scores["registry_name"] = registry
    for m in metrics:
        scores[m] = {}
        s_bools = []
        for s in source:
            s_bools.append(
                (filtered[m + s].notnull()) & (~filtered[m + s].isin(exclude_classes))
            )
            scores[m][s] = sum(s_bools[-1]) / len(filtered)
        scores[m][combined] = len(filtered[combine_bools(s_bools)]) / len(filtered)

    return scores


# +
df.registries_listed = df.registries_listed.fillna(value="unregistered")

# The information provided by TWN is added to IOTC
IOTC_ids = df[(df.registries_listed == "IOTC")].ssvid.unique()
df.loc[
    (df.registries_listed == "TWN") & ~df.ssvid.isin(IOTC_ids), "registries_listed"
] = "IOTC"
# -

# # Compute and plot information completeness scores by registry 
# Scores are computed for information derived from raw registries, from aggregated registries, from the vessel characterisation model, and all of these combined. The score indicates the completenes of information provided by each of these sources.

metrics = ["vessel_class_", "length_m_", "tonnage_gt_", "engine_power_kw_"]
# Add spaces for the display 
categories = ["          geartype", "length", "tonnage          ", "engine_power"]
sources = ["registry", "inferred", "aggregated2"]
reg_order = get_reg_order(df, rfmos)
scores_by_reg = []
for rfmo in np.asarray(rfmos)[reg_order]:
    reg_scores = get_scores(df, rfmo, metrics, sources)
    scores_by_reg.append(reg_scores)


# +
rows = 5
cols = 3
fig, ax = plt.subplots(
    nrows=rows, ncols=cols, figsize=(18, 23), subplot_kw=dict(polar=True)
)
fig.suptitle("Information available by registry, before and after aggregating,\nfrom the vessel characterisation model, and all combined", size=25)
for i, rs in enumerate(scores_by_reg):
    results = {}
    for s in ["registry", "inferred", "aggregated2", "combined"]:
        results[s] = [rs[m][s] for m in metrics]
        results[s] += results[s][:1]

    angles = [n / float(len(metrics)) * 2 * pi for n in range(len(metrics))]
    angles += angles[:1]

    ax = plt.subplot(rows, cols, i + 1, projection="polar")
    ax.set_title(rs["registry_name"], size=20)

    plt.xticks(angles[:-1], categories, color="grey", size=16)
    plt.ylim(-0.000000001, 1)

    ax.set_rlabel_position(20)
    ax.yaxis.set_tick_params(labelsize=7)

    ax.plot(
        angles,
        results["combined"],
        linewidth=4,
        linestyle="solid",
        label="Information available after combining all available sources",
        color=blue,
        alpha=0.6,
    )

    ax.plot(
        angles,
        results["inferred"],
        linewidth=1.8,
        linestyle="solid",
        label="Information provided by vessel characterisation model",
        color=yellow,
        alpha=0.85,
    )

    ax.plot(
        angles,
        results["aggregated2"],
        linewidth=1.8,
        linestyle="solid",
        label="Information available after aggregation",
        color=orange,
        alpha=0.85,
    )

    ax.plot(
        angles,
        results["registry"],
        linewidth=3,
        linestyle="solid",
        label="Raw information available by registry",
        color=pink,
        alpha=0.6,
    )

    ax.grid(color="gray", linestyle="-", linewidth=0.7)
    ax.grid(True)
    fig.patch.set_facecolor(bg_color)
plt.legend(bbox_to_anchor=(1.5, 1), loc="upper left", fontsize="14")

ax = plt.subplot(rows, cols, 15, projection="polar")
fig.delaxes(ax)
ax.set_visible(False)
fig.subplots_adjust(hspace=0.7)
plt.savefig(
    figures_folder + "information_availability_plot.png", dpi=1200, bbox_inches="tight"
)
plt.show()


# +
def exact_match(
    df,
    col1="vessel_class_inferred",
    col2="vessel_class_registry",
    res_col="exact_match_registry",
):
    """compare two dataframe columns for exact matches

    Parameters
    ----------
    df : dataframe
    col1, col2 : string
        name of dataframe columns to compare
    res_col : string
        name of dataframe column to store boolean result of comparison

    Returns
    ------
    df : dataframe
    """
    df[res_col] = df[col1] == df[col2]

    return df


def split_match(
    df,
    col1="vessel_class_inferred",
    col2="vessel_class_registry",
    res_col="split_match_registry",
    score_col="split_score_reg",
):
    """compare two dataframe columns for matches. col2 may be multiple geartypes separated by '|'
    If one of the geartypes matches col1, then that is a match. The score is 1 / number of geartypes in col2.

    Parameters
    ----------
    df : dataframe
    col1, col2 : string
        name of dataframe columns to compare
        col2 may have multiple geartypes split using '|'
    res_col : string
        name of dataframe column to store boolean result of comparison
    score_col : string
        name of dataframe column to store score of comparison
    Returns
    ------
    df : dataframe
    """
    split_match = []
    split_lens = []
    for index, row in df.iterrows():
        reg_list = set(row[col2].split("|"))

        u = set.intersection(reg_list, set([row[col1]]))

        split_lens.append(1.0 / len(reg_list))
        if len(u) >= 1:
            split_match.append(True)

        else:
            split_match.append(False)

    df[res_col] = split_match
    df[score_col] = split_lens
    return df


def hier_match(
    df,
    col1="vessel_class_inferred",
    col2="vessel_class_registry",
    agg="combo_agg_reg",
    res_col="hier_match_registry",
):
    """Check if two geartypes are in the same hierarchy as more general / more specific versions of each other

    Parameters
    ----------
    df : dataframe
    col1, col2 : string
        name of dataframe columns to compare
    agg : the geartype returned from udfs.determine_class
    res_col : string
        name of dataframe column to store boolean result of comparison
    Returns
    ------
    df : dataframe
    """
    hier_match = []
    for index, row in df.iterrows():
        same_hier = bool(row[agg] == row[col1]) or (row[agg] == row[col2])
        if same_hier:
            hier_match.append(True)
        else:
            hier_match.append(False)
    df[res_col] = hier_match
    return df


def branch_match(
    df,
    agg="combo_agg_reg",
    res_col="branch_match_registry",
):
    """Check if two geartypes are in the same hierarchy as different leaf nodes

    Parameters
    ----------
    df : dataframe
    agg : the geartype returned from udfs.determine_class
    res_col : string
        name of dataframe column to store boolean result of comparison
    Returns
    ------
    df : dataframe
    """
    branch_check = (
        (df[agg] == "bunker_or_tanker")
        | (df[agg] == "cargo_or_tanker")
        | (df[agg] == "cargo_or_reefer")
        | (df[agg] == "reefer")
        | (df[agg] == "seiners")
    )
    df[res_col] = branch_check
    return df


def fill_unclassified(df, columns, fill="unclassified"):
    """
    Parameters
    ----------
    df : dataframe
    columns : list of strings
        list of dataframe columns to fill null values for
    fill : string
        string to replace null values
    Returns
    ------
    df : dataframe
    """
    for column in columns:
        df[column] = df[column].fillna(fill)
        df[column] = df[column].replace("", fill)
    return df


# +
def assign_scores(
    df,
    source="registry",
    score_name="reg",
    exact_score=1.0,
    hier_score=0.5,
    hier_score_fishing=0.25,
):
    """
    Assign scores to geartype matches, according to if they are exact or within the same hierarchy.
    For ssvid appearing on multiple rows, the average score over all those rows is computed.

    Parameters
    ----------
    df : dataframe
    source : string
        source of information
    score_name : string
        string extension for score field
    exact_score : float
        score assigned to exact matches
    hier_score : float
        score assigned to same hierarchy matches
    hier_score_fishing : float
        score assigned when it is same hierarchy and one of the geartypes is 'fishing' or 'non_fishing'

    Returns
    ------
    df : dataframe
    """
    scores = []
    for index, row in df.iterrows():
        if row["exact_match_" + source] == True:
            scores.append(exact_score)
        elif row["split_match_" + source] == True:
            scores.append(row["split_score_" + score_name])
        elif (row["hier_match_" + source] == True) or (
            row["branch_match_" + source] == True
        ):
            if (
                (row["vessel_class_" + source] != "unclassified")
                and (row["vessel_class_" + source] != "fishing")
                and (row["vessel_class_" + source] != "non_fishing")
            ):
                scores.append(hier_score)
            elif (row["vessel_class_" + source] == "fishing") or (
                row["vessel_class_" + source] == "non_fishing"
            ):
                scores.append(hier_score_fishing)
            else:
                scores.append(0.0)
        else:
            scores.append(0.0)
    df["scores_" + score_name] = scores
    df["scores_" + score_name + "_b"] = scores

    # For ssvid appearing on multiple rows, the average score over all those rows is recorded
    dfg = df.groupby("ssvid")
    if source == "registry":
        dfg = df[~df.registries_listed.isin(remove_for_reg)].groupby("ssvid")
    for name, group in dfg:
        if len(group) > 1:
            df.loc[df.ssvid == name, "scores_" + score_name + "_b"] = sum(
                group["scores_" + score_name]
            ) / len(group)

    return df


def sort_for_radial(scores):
    """
    Find sequence to make the radial plot a smooth circle

    Parameters
    ----------
    scores : list of floats

    Returns
    ------
    array
    """
    sorted1 = np.argsort(scores)[::-1]
    half1 = sorted1[::2]
    half2 = sorted1[1::2][::-1]
    return np.concatenate((half1, half2), axis=0)


# -

# # Compute and plot scores by registry indicating how well the geartype information matches with the vessel characterisation model

# +
# Remove these registries when computing registry scores for the geartype matching with model
remove_for_reg = [
    "CORR",
    "REV",
    "CHINASPRFMO",
    "CHINAFISHING",
    "OWNER",
    "CARRIER",
    "BUNKER",
]

df = fill_unclassified(
    df,
    [
        "vessel_class_registry",
        "vessel_class_aggregated1",
        "vessel_class_aggregated2",
        "vessel_class_inferred",
    ],
)

# Remove vessels for which the model did not predict a geartype
df = df[df.vessel_class_inferred != "unclassified"].copy()


column_names = ["registry", "aggregated1", "aggregated2"]
score_names = ["reg", "aggregated1", "aggregated2"]

for (col, sc) in zip(column_names, score_names):
    df = exact_match(df, col2="vessel_class_" + col, res_col="exact_match_" + col)
    df = split_match(
        df,
        col2="vessel_class_" + col,
        res_col="split_match_" + col,
        score_col="split_score_" + sc,
    )
    df = hier_match(
        df,
        col2="vessel_class_" + col,
        agg="combo_agg_" + sc,
        res_col="hier_match_" + col,
    )
    df = branch_match(
        df,
        agg="combo_agg_" + sc,
        res_col="branch_match_" + col,
    )

    df = assign_scores(df, source=col, score_name=sc)


# +
min_vessels = 10


registry_scores = []
aggregated1_scores = []
aggregated2_scores = []
reg_names = []


# Normalise scores
for r in rfmos:

    df_r = df[(df["registries_listed"] == r)]
    assert len(df_r) == len(df_r.ssvid.unique())
    if len(df_r) > min_vessels:
        reg_names.append(r)

        max_score = len(df_r)
        registry_scores.append(sum(df_r["scores_reg_b"]) / max_score)
        aggregated1_scores.append(sum(df_r["scores_aggregated1_b"]) / max_score)
        aggregated2_scores.append(sum(df_r["scores_aggregated2_b"]) / max_score)


# Sort to look nice in plot
reg_order = sort_for_radial(registry_scores)

registry_scores = list(np.asarray(registry_scores)[reg_order])
aggregated1_scores = list(np.asarray(aggregated1_scores)[reg_order])
aggregated2_scores = list(np.asarray(aggregated2_scores)[reg_order])
reg_names = list(np.asarray(reg_names)[reg_order])

registry_scores += registry_scores[:1]
aggregated1_scores += aggregated1_scores[:1]
aggregated2_scores += aggregated2_scores[:1]


# Plot
angles = [n / float(len(reg_names)) * 2 * pi for n in range(len(reg_names))]
angles += angles[:1]


pylab.rcParams["xtick.major.pad"] = "11"

sns.set_style("white")
fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(4, 4), subplot_kw=dict(polar=True))


plt.xticks(angles[:-1], reg_names, size=11)


plt.ylim(-0.000000001, 1)
ax.set_rlabel_position(10)

ax.plot(
    angles,
    registry_scores,
    linewidth=2,
    linestyle="solid",
    label="Score by Pre-Aggregation Vessel Class\nAveraged over registries",
    color=pink,
    alpha=0.6,
)

ax.plot(
    angles,
    aggregated1_scores,
    linewidth=2,
    linestyle="solid",
    label="Score by Vessel Class\nDerived from Aggregation across Registry",
    color=orange,
    alpha=0.6,
)

ax.plot(
    angles,
    aggregated2_scores,
    linewidth=2,
    linestyle="solid",
    label="Score by Vessel Class Derived from\nAggregation across Registry and Identity",
    color=blue,
    alpha=0.6,
)
ax.axhline(y=0, color="k")
fig.patch.set_facecolor(bg_color)
lgd = plt.legend(bbox_to_anchor=(1.15, 1), loc="upper left")

plt.savefig(
    figures_folder + "geartype_matching_score_plot.png",
    bbox_extra_artists=(lgd,),
    dpi=1200,
    bbox_inches="tight",
)


# -
df[df.registries_listed == "NEAFC"][
    [
        "ssvid",
        "vessel_class_inferred",
        "vessel_class_registry",
        "vessel_class_aggregated1",
        "vessel_class_aggregated2",
    ]
]



