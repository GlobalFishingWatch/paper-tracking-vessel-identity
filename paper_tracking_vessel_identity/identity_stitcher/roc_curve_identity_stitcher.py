# ---
# jupyter:
#   jupytext:
#     formats: py:light
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

# # ROC Curve to Determine Thresholds for Identity Stitcher

import pandas as pd
import numpy as np
from sklearn.metrics import roc_curve, auc
import matplotlib.pyplot as plt
# %matplotlib inline
plt.style.use('bmh')

#
# To determine the optimal distance under which pair trawlers are mostly operating,
# we use the human annotated data sets of optical imagery. A total of 4,800 vessels
# were annotated to train the neural network for the detection on optical imagery.
# The annotated vessels include trawlers, lighting vessels, and other unknown vessels.
# Using a ROC curve, we determine the threshold in distance to the nearest vessel of
# a target vessel. 
#
q = """
WITH
  category AS (
    SELECT distance_gap_meter, imo=pair_imo AS match
    FROM vessel_identity_staging.identity_stitcher_core_v20210801
    WHERE imo IS NOT NULL AND pair_imo IS NOT NULL
    #  AND rank_dist = 1
    ORDER BY distance_gap_meter
  )
  
SELECT *
FROM category
"""
df = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Create the ROC curve and plot it to determine the optimal threshold
#'', ''
fpr, tpr, th = roc_curve(~df['match'], # This is reversed as the target is "below" the threshold not "above".
                         df['distance_gap_meter'])
#
# Calculate the area of the curve
roc_auc = auc(fpr, tpr)

fig = plt.figure(figsize=(6, 6), dpi=300, facecolor='white')
ax = fig.add_subplot(111)
ax.set_facecolor('white')
ax.plot(fpr, tpr, lw=1, color='#204280', alpha=0.9)

#
# Find the optimal threshold in distance
optimal_idx = np.argmax(tpr - fpr)
optimal_th = th[optimal_idx]

ax.scatter(fpr[optimal_idx], tpr[optimal_idx], color='#d73b68')
ax.text(fpr[optimal_idx]+0.02, tpr[optimal_idx]-0.04, 
        'Distance threshold = {} meters'.format(round(optimal_th, 1)), color='#ca0020', fontsize=14)
ax.plot([0,1],[0,1], "--", color='#204280', alpha=0.9)
ax.set_xlim([0,1])
ax.set_ylim([0,1])
ax.set_title("ROC Curve", fontsize=14)
ax.set_ylabel('True positive rate', fontsize=12)
ax.set_xlabel('False positive rate', fontsize=12)
# ax.grid(zorder=2)
ax.grid(b=True, which='major', color='k', linestyle='--', linewidth=0.1)
ax.legend(["AUC={:.3f}".format(roc_auc)], loc=4, facecolor='white')
plt.show()
# -


