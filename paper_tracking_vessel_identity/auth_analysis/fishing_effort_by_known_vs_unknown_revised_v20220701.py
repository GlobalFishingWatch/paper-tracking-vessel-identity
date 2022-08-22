# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.1
#   kernelspec:
#     display_name: pyseas
#     language: python
#     name: pyseas
# ---

# +
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import pyseas
from pyseas import maps, cm, styles, util
from pyseas.maps import scalebar, core, rasters, ticks
import imp
import matplotlib.colors as colors
from cartopy import crs
from cartopy.io.shapereader import Reader
import pyseas.maps as psm
import matplotlib.colors as mpcolors
from matplotlib.colors import LinearSegmentedColormap
import glob


# -

def reload():
    imp.reload(util)
    imp.reload(ticks)
    imp.reload(scalebar)
    imp.reload(cm)
    imp.reload(styles)
    imp.reload(rasters)
    imp.reload(core)
    imp.reload(maps)
    imp.reload(pyseas)


def make_raster(df, xyscale, cat):
    authorized_all = df[df["fishing_hours_all"].notnull()]
    authorized_known = df[df[cat].notnull()]
    authorized_unknown = authorized_all.copy()
    authorized_unknown[cat] = authorized_unknown[cat].fillna(0)
    authorized_unknown['fishing_hours_unknown'] = authorized_unknown.apply(
        lambda x: max (x["fishing_hours_all"] - x[cat], 0), axis=1)

    grid = pyseas.maps.rasters.df2raster(authorized_unknown, 'lon_bin', 'lat_bin', 
                                         'fishing_hours_unknown', 
                                         xyscale=xyscale, per_km2=True)
    grid2 = pyseas.maps.rasters.df2raster(authorized_all, 'lon_bin', 'lat_bin', 
                                         'fishing_hours_all', 
                                         xyscale=xyscale, per_km2=True)
    grid3 = np.divide(grid, grid2, out=np.zeros_like(grid), where=grid2!=0)
    return grid2, grid3


# # Known fishing effort

# ## All fishing effort in AIS

q = """
SELECT *
FROM `vessel_identity_staging.fishing_effort_all_5th_deg_v20220701`
"""
all_fishing = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
reload()
grid = pyseas.maps.rasters.df2raster(all_fishing, 'lon_bin', 'lat_bin', 'fishing_hours', 
                                     xyscale=5, per_km2=True)

with pyseas.context(pyseas.styles.dark, 
                    {'axes.labelsize' : 40}):
    fig_min_value = 0.005
    fig_max_value = 5 
    norm = mpcolors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(15, 15), dpi=300, facecolor='#f7f7f7')
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(
        grid, cmap='fishing', loc='bottom', norm = norm,
        projection = "global.default") 
    
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)

    ax1 = ax.inset_axes([0.4, -0.215, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='left', fontsize=14,
             s="Fishing hours by all vessels\nbroadcasting AIS (per $\mathregular{km^2}$)") #+ \
#                "\nin 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells")
    plt.title("Fishing Effort by All Vessels Broadcasting AIS (2021)", 
              fontsize=18)
    plt.show()
# -

# ## Fishing effort by identified vessels

q = """
SELECT *
FROM `vessel_identity_staging.fishing_effort_known_vs_all_5th_deg_v20220701`
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
grid_total, grid_ratio = make_raster(authorized, 5, 'fishing_hours_known_vessels')

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='#f7f7f7')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.003, vmax=0.3, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(cmap, norm1, norm2,
                                       xlabel='Hours fished by AIS-registry unmatched vessels\n' +
                                         '(as fraction of total fishing hours)',
                                       ylabel='Fishing\nhours\n(per $\mathregular{km^2}$)', fontsize=12,
                                       xformat='{x:.1f}', yformat='{x:.2f}',
                                       loc = (0.6, -0.17), aspect_ratio=3.0,
                                       ax=ax)
    
    ax1 = ax.inset_axes([0.35, -0.2, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=14,
             s="Ratio of fishing hours\nby AIS-registry unmatched vessels\n"
             "to those by all vessels broadcasting AIS\n"
             "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells")
#     ax1.text(0.58, -0.05, ha='right', fontsize=21,
#              s="Ratio of Fishing Hours\nby AIS-registry Unmatched Vessels\n"
#              "to Those by All Vessels Broadcasting AIS")
    
    plt.title("Fishing Effort by Identified vs. Unidentified Vessels (2021)", 
              fontsize=18)
    plt.show()

# ## Fishing effort by vessels with known vs. unknown authorization

q = """
SELECT *
FROM `vessel_identity_staging.fishing_effort_auth_vs_all_5th_deg_v20220701`
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
grid_total, grid_ratio = make_raster(authorized, 5, 'fishing_hours_authorized_vessels')

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='#f7f7f7')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.003, vmax=0.3, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(cmap, norm1, norm2,
                                       xlabel='Hours fished by authorization unknown vessels\n' +
                                         '(as fraction of total fishing hours)',
                                       ylabel='Fishing\nhours\n(per $\mathregular{km^2}$)', fontsize=12,
                                       xformat='{x:.1f}', yformat='{x:.2f}',
                                       loc = (0.6, -0.17), aspect_ratio=3.0,
                                       ax=ax)
    
    ax1 = ax.inset_axes([0.35, -0.2, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=14,
             s="Ratio of fishing hours\nby authorization unknown vessels\n"
             "to those by all vessels broadcasting AIS\n"
             "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells")
#     ax1.text(0.58, -0.05, ha='right', fontsize=21,
#              s="Ratio of Fishing Hours\nby AIS-registry Unmatched Vessels\n"
#              "to Those by All Vessels Broadcasting AIS")
    
    plt.title("Fishing Effort by Vessels with Known vs. Unknown Authorization (2021)", 
              fontsize=18)
    plt.show()

# # RFMO mapping

# ## FAO Major Fisheries Areas

q = """
SELECT *
FROM `vessel_identity_staging.fishing_effort_auth_vs_all_5th_deg_v20220701`
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
grid_total, grid_ratio = make_raster(authorized, 5, 'fishing_hours_authorized_vessels')

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='#f7f7f7')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.003, vmax=0.3, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(cmap, norm1, norm2,
#                                        xlabel='Ratio',
                                       ylabel='Fishing\nHours\n(per $\mathregular{km^2}$)', fontsize=12,
                                       xformat='{x:.1f}', yformat='{x:.2f}',
                                       loc = (0.6, -0.17), aspect_ratio=3.0,
                                       ax=ax)

    lw = 1
    al = 1
    color = '#848b9b'
    path = '../data/FAO_FISHERIES_AREA/'
    for fname in glob.glob(path + 'FSA_*_simp/FSA_*.shp'):
        ax.add_geometries(Reader(fname).geometries(),
                          crs.PlateCarree(), edgecolor=color, linewidth=lw, 
                          facecolor='none', alpha=al, zorder=0)
    
    tags = {'18': [150, 72],
            '21': [-51, 52],
            '27': [-40, 52],
            '31': [-68.5, 30.7],
            '34': [-21, 31.7],
            '37': [17, 29.7],
            '41': [-45, -49.5],
            '47': [2, -49.5],
            '48': [-26, -70],
            '51': [60, -44.5],
            '57': [110, -54.2],
            '58': [82, -64],
            '61': [157, 50],
            '67': [-150, 50],
            '71': [142, 16],
            '77': [-142, 35.5],
            '81': [-156, -59.3],
            '87': [-105, -59.3],
            '88': [-130, -71]}
    color = "#e6e7eb"
    for i, key in enumerate(tags):
        lon, lat = crs.EqualEarth().transform_point(tags[key][0], tags[key][1], crs.PlateCarree())
        ax.text(lon, lat, key, fontsize=15, fontweight='bold', color=color, alpha=0.7)
    
    
    ax1 = ax.inset_axes([0.35, -0.20, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=14,
             s="Ratio of Fishing Hours\nby AIS-registry Unmatched Vessels\n"
             "to Those by All Vessels Broadcasting AIS\n"
             "in 0.2" + u"\u00b0" + "x 0.2" + u"\u00b0" + "grid cells")
    
    plt.title("FAO Fishing Areas: Ratio of Fishing by Vessels with Unknown Authorization (2021)", 
              fontsize=18)
    plt.show()

# ## Averaged fishing hours with unknown authorization by FAO Major Fisheries Area

q = """
SELECT *
FROM `vessel_identity_staging.fishing_effort_auth_vs_all_ratio_by_fao_area_v20220701`
"""
fao_region = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='#f7f7f7')
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    

    lw = 1
    al = 1
    cmp = LinearSegmentedColormap.from_list("colormap", ["#2f87cf","#8f799a","#ea4753"])
    norm = mpcolors.Normalize(vmin=0.01, vmax=0.5, clip=True)

    ax0 = ax.inset_axes([0.45, -0.08, 0.35, 0.2 / 3], transform=ax.transAxes)
    
    x, y = np.mgrid[0:2, 0:81]
    Z_plot = np.array(255*(y-y.min())/(y.max()-y.min()), dtype=np.int)
    Z_color = cmp(Z_plot)
    ax0.imshow(Z_color[:, :, 0:3], origin='upper', interpolation='nearest', cmap=cmp)

    ax0.yaxis.set_visible(False)
    ax0.yaxis.set_ticks([])
    ax0.set_xticklabels([l * 0.005 if l != 80 else '>0.4' for l in ax0.get_xticks()])

    path = '../data/FAO_FISHERIES_AREA/'
    for fname in glob.glob(path + 'FSA_*_simp/FSA_*.shp'):
        fao_area = fname.split('/FSA_')[-1].split('.shp')[0]
        if fao_area in ('18', '58'):
            color = 'none'
        else:
            ratio = fao_region[fao_region['fao_area'] == fao_area]['ratio_auth_unknown_fishing'].iloc[0]
            color = cmp(norm(ratio))
            
        ax.add_geometries(Reader(fname).geometries(),
                          crs.PlateCarree(), edgecolor='#e6e7eb', linewidth=lw, 
                          facecolor=color, alpha=al, zorder=1)
    
    tags = {'18': [150, 72],
            '21': [-51, 52],
            '27': [-40, 52],
            '31': [-68.5, 30.7],
            '34': [-21, 31.7],
            '37': [14.4, 32.5],
            '41': [-45, -49.5],
            '47': [2, -49.5],
            '48': [-26, -70],
            '51': [60, -44.5],
            '57': [110, -54.2],
            '58': [82, -64],
            '61': [157, 50],
            '67': [-150, 50],
            '71': [142, 16],
            '77': [-142, 35.5],
            '81': [-156, -59.3],
            '87': [-105, -59.3],
            '88': [-130, -71]}
    color = "#e6e7eb"
    for i, key in enumerate(tags):
        lon, lat = crs.EqualEarth().transform_point(tags[key][0], tags[key][1], crs.PlateCarree())
        ax.text(lon, lat, key, fontsize=15, fontweight='bold', color=color, alpha=1)
    
    ax1 = ax.inset_axes([0.4, -0.25, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='left', fontsize=14,
             s="Ratio of Fishing Hours by" + \
             "\nVessels with Unknown Authorization" + \
             "\nto Those of All Vessels by FAO Major Fisheries Area")
    
    plt.title("FAO Fishing Areas: Fraction of Fishing by Vessels with Unknown Authorization (2021)", 
              fontsize=18)
    plt.show()

# ## Tuna-RFMOs

q = """
SELECT *
FROM `vessel_identity_staging.fishing_effort_auth_vs_all_5th_deg_trfmo_v20220701`
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
grid_total, grid_ratio = make_raster(authorized, 5, 'fishing_hours_authorized_vessels')

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=500, facecolor='#f7f7f7')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.005, vmax=0.5, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(cmap, norm1, norm2,
#                                        xlabel='Ratio of Fishing Hours by RFMO Authorization Matched Vessels to Those by All',
                                       ylabel='Fishing\nHours\n(per $\mathregular{km^2}$)', fontsize=12,
                                       xformat='{x:.1f}', yformat='{x:.2f}',
                                       loc = (0.6, -0.17), aspect_ratio=3.0,
                                       ax=ax)

    lw = 4
    al = 0.4
    colors = ['#ad2176', '#d73b68', '#8abbc7', '#f68d4b', '#ebe55d']
    fname = '../data/FAO_RFB/FAO_RFB_CCSBT_simplified/RFB_CCSBT.shp'
    ax.add_geometries(Reader(fname).geometries(),
                      crs.PlateCarree(), edgecolor=colors[0], linewidth=lw, #e6e7eb
                      facecolor='none', alpha=al+0.1)
    fname = '../data/FAO_RFB/FAO_RFB_IATTC_simplified/RFB_IATTC.shp'
    ax.add_geometries(Reader(fname).geometries(),
                      crs.PlateCarree(), edgecolor=colors[1], linewidth=lw,
                      facecolor='none', alpha=al)
    fname = '../data/FAO_RFB/FAO_RFB_ICCAT_simplified/RFB_ICCAT.shp'
    ax.add_geometries(Reader(fname).geometries(),
                      crs.PlateCarree(), edgecolor=colors[2], linewidth=lw,
                      facecolor='none', alpha=al)
    fname = '../data/FAO_RFB/FAO_RFB_IOTC_simplified/RFB_IOTC.shp'
    ax.add_geometries(Reader(fname).geometries(),
                      crs.PlateCarree(), edgecolor=colors[3], linewidth=lw, 
                      facecolor='none', alpha=al)
    fname = '../data/FAO_RFB/FAO_RFB_WCPFC_simplified/RFB_WCPFC.shp'
    ax.add_geometries(Reader(fname).geometries(),
                      crs.PlateCarree(), edgecolor=colors[4], linewidth=lw,
                      facecolor='none', alpha=al)
    
    tags = {'CCSBT': [-40, -43],
            'IATTC': [-145, 44],
            'ICCAT': [-53, 54],
            'IOTC': [81, 12],
            'WCPFC': [146, 46]}
    for i, key in enumerate(tags):
        lon, lat = crs.EqualEarth().transform_point(tags[key][0], tags[key][1], crs.PlateCarree())
        ax.text(lon, lat, key, fontsize=15, fontweight='bold', color=colors[i], alpha=0.8)
    
    
    ax1 = ax.inset_axes([0.35, -0.17, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=14,
             s="Ratio of Fishing Hours in High Seas\nby Vessels with Unknown RFMO Authorization\n"
             "to Those by All Vessels Broadcasting AIS")
    
    plt.title("Tuna RFMO Areas: Ratio of Fishing by Vessels with Unknown Authorization (2021)", 
              fontsize=18)
    plt.show()

# ## Squid RFMO

q = """
SELECT *
FROM `vessel_identity_staging.fishing_effort_auth_vs_all_5th_deg_squid_rfmo_v20220701`
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
grid_total, grid_ratio = make_raster(authorized, 5, 'fishing_hours_authorized_vessels')

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=500, facecolor='#f7f7f7')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.005, vmax=0.5, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(cmap, norm1, norm2,
#                                        xlabel='Ratio of Fishing Hours by RFMO Authorization Matched Vessels to Those by All',
                                       ylabel='Fishing\nHours\n(per $\mathregular{km^2}$)', fontsize=12,
                                       xformat='{x:.1f}', yformat='{x:.2f}',
                                       loc = (0.6, -0.17), aspect_ratio=3.0,
                                       ax=ax)

    lw = 4
    al = 0.4
    colors = ['#8abbc7', '#ebe55d']
    fname = '../data/FAO_RFB/FAO_RFB_NPFC/RFB_NPFC.shp'
    ax.add_geometries(Reader(fname).geometries(),
                      crs.PlateCarree(), edgecolor=colors[0], linewidth=lw, #e6e7eb
                      facecolor='none', alpha=al)
    fname = '../data/FAO_RFB/FAO_RFB_SPRFMO/RFB_SPRFMO.shp'
    ax.add_geometries(Reader(fname).geometries(),
                      crs.PlateCarree(), edgecolor=colors[1], linewidth=lw,
                      facecolor='none', alpha=al)
    
    tags = {'NPFC': [136, 46],
            'SPRFMO': [-145, -57]}
    for i, key in enumerate(tags):
        lon, lat = crs.EqualEarth().transform_point(tags[key][0], tags[key][1], crs.PlateCarree())
        ax.text(lon, lat, key, fontsize=15, fontweight='bold', color=colors[i], alpha=0.8)
        
    
    ax1 = ax.inset_axes([0.35, -0.17, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=14,
             s="Ratio of Fishing Hours in High Seas\nby Vessels with Unknown RFMO Authorization\n"
             "to Those by All Vessels Broadcasting AIS")
    
    plt.title("Squid Fishing Areas: Fishing by Vessels with Unknown Authorization (2021)", 
              fontsize=18)
    plt.show()

# # Country mapping

# ## Faroe Island

q = """
SELECT *
FROM scratch_jaeyoon.fishing_effort_auth_vs_all_50th_deg_fro
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
authorized_all = authorized[authorized["fishing_hours_all"].notnull()]
authorized_known = authorized[authorized["fishing_hours_authorized_vessels"].notnull()]
authorized_unknown = authorized_all.copy()
authorized_unknown["fishing_hours_authorized_vessels"] = authorized_unknown["fishing_hours_authorized_vessels"].fillna(0)
authorized_unknown['fishing_hours_authorized_unknown'] = authorized_unknown.apply(
    lambda x: max (x["fishing_hours_all"] - x["fishing_hours_authorized_vessels"], 0), axis=1)

grid = pyseas.maps.rasters.df2raster(authorized_unknown, 'lon_bin', 'lat_bin', 
                                     'fishing_hours_authorized_unknown', 
                                     xyscale=50, per_km2=True)
grid2 = pyseas.maps.rasters.df2raster(authorized_all, 'lon_bin', 'lat_bin', 
                                     'fishing_hours_all', 
                                     xyscale=50, per_km2=True)
grid3 = np.divide(grid, grid2, out=np.zeros_like(grid), where=grid2!=0)
# -

reload()
grid_ratio = grid3.copy()
grid_total = grid2.copy()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='white',
                              projection='country.faroe')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.1, vmax=10.0, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(cmap, norm1, norm2,
                                       xlabel='Ratio of Fishing Hours by Authorization Matched Vessels to Those by All',
                                       ylabel='Total Fishing Hours\n(per $\mathregular{km^2}$)', fontsize=8,
                                       xformat='{x:.1f}', yformat='{x:.2f}',
                                       loc = (0.6, -0.12), aspect_ratio=3.0,
                                       ax=ax)
    
    ax1 = ax.inset_axes([0.4, -0.13, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right',
             s="Ratio of fishing hours by Faroe Islands' authorization matched vessels\n"
             "to those by all vessels broadcasting AIS\n"
             "during 2021, in 0.02" + u"\u00b0" + "x 0.02" + u"\u00b0" + "grid cells")
    
    plt.title("Fishing Effort by Authorized Vessels to vs. Authorization Unknown Vessels (2021)" + 
              "\nFaroe Islands: 1,600 hours of fishing with unknown authorization, 1% of all fishing hours by its fleet",
              fontsize=14)
    plt.show()

# ## Norway

q = """
SELECT *
FROM vessel_identity_staging.fishing_effort_auth_vs_all_50th_deg_nor_v20211101
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
grid_total, grid_ratio = make_raster(authorized, 50, 'fishing_hours_authorized_vessels')

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='#f7f7f7',
                              projection='country.norway')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.1, vmax=10.0, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(
        cmap, norm1, norm2,
#         xlabel='Ratio of Fishing Hours by Authorization Matched Vessels to Those by All',
        ylabel='Fishing\nHours\n(per $\mathregular{km^2}$)', fontsize=12,
        xformat='{x:.1f}', yformat='{x:.2f}',
        loc = (0.6, -0.12), aspect_ratio=2.0,
        ax=ax)
    
    ax1 = ax.inset_axes([0.35, -0.17, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=14,
             s="Ratio of Fishing Hours\nby Vessels with Unknown National Authorization\n"
             "to Those by All Vessels Broadcasting AIS")
    
    plt.title("Norwegian Waters: Fishing by Vessels with Unknown Authorization (2020)",
              fontsize=18)
    plt.show()

# ## Iceland

q = """
SELECT *
FROM vessel_identity_staging.fishing_effort_auth_vs_all_50th_deg_isl_v20211101
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
grid_total, grid_ratio = make_raster(authorized, 50, 'fishing_hours_authorized_vessels')

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='#f7f7f7',
                              projection='country.iceland')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.1, vmax=10.0, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(cmap, norm1, norm2,
#                                        xlabel='Ratio of Fishing Hours by Authorization Matched Vessels to Those by All',
                                       ylabel='Fishing\nHours\n(per $\mathregular{km^2}$)', fontsize=12,
                                       xformat='{x:.1f}', yformat='{x:.2f}',
                                       loc = (0.6, -0.12), aspect_ratio=2.0,
                                       ax=ax)
    
    ax1 = ax.inset_axes([0.35, -0.17, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right', fontsize=14,
             s="Ratio of Fishing Hours\nby Vessels with Unknown National Authorization\n"
             "to Those by All Vessels Broadcasting AIS")
    
    plt.title("Icelandic Waters: Fishing by Vessels with Unknown Authorization (2020)",
              fontsize=18)
    plt.show()

# ## Peru

q = """
SELECT *
FROM scratch_jaeyoon.fishing_effort_auth_vs_all_50th_deg_per
"""
authorized = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
grid_total, grid_ratio = make_raster(authorized, 50)

reload()
cmap = psm.bivariate.TransparencyBivariateColormap(cm.misc.blue_orange)
with pyseas.context(psm.styles.dark):
    fig, ax = psm.create_maps(1, 1, figsize=(15, 15), dpi=300, facecolor='white',
                              projection='country.peru_tight')
    norm1 = mpcolors.Normalize(vmin=0.01, vmax=1.0, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.01, vmax=0.5, clip=True)
    psm.add_bivariate_raster(grid_ratio, grid_total, cmap, norm1, norm2, ax=ax)
    psm.add_land(ax)
    psm.add_countries(ax)
    psm.add_eezs(ax)
    
    cb_ax = psm.add_bivariate_colorbox(cmap, norm1, norm2,
                                       xlabel='Ratio of Fishing Hours by Authorization Matched Vessels to Those by All',
                                       ylabel='Total Fishing Hours\n(per $\mathregular{km^2}$)', fontsize=8,
                                       xformat='{x:.1f}', yformat='{x:.2f}',
                                       loc = (0.6, -0.1), aspect_ratio=3.0,
                                       ax=ax)
    
    ax1 = ax.inset_axes([0.4, -0.12, 0.2, 0.2 / 3], transform=ax.transAxes)
    ax1.axis('off')
    ax1.text(0.5, 0.5, ha='right',
             s="Ratio of fishing hours\nby authorization matched vessels\n"
             "to those by all vessels broadcasting AIS\n"
             "during 2021, in 0.02" + u"\u00b0" + "x 0.02" + u"\u00b0" + "grid cells")
    
    plt.title("Fishing Effort by Authorized Vessels to vs. Authorization Unknown Vessels (2021)" + 
              "\nPeru: 24,000 hours of fishing with unknown authorization, 35% of all fishing hours by its fleet",
              fontsize=14)
    plt.show()







# +
from matplotlib.colors import LinearSegmentedColormap
xx, yy = np.mgrid[0:100, 0:100]
Z1_plot = np.array((xx-xx.min())/(xx.max()-xx.min()))
Z2_plot = np.array(255*(yy-yy.min())/(yy.max()-yy.min()), dtype=np.int)

colors = [(255 / 255., 69 / 255., 115 / 255.), (1, 1, 1), (0, 255 / 255., 195 / 255.)]

cm = LinearSegmentedColormap.from_list(
    'contrast', colors, N=256)
Z2_color = cm(Z2_plot)
background = (10 / 255., 23 / 255., 56 / 255.)  # "#0a1738"
Z_color = np.zeros_like(Z2_color)
Z2_color[:, :, 0] * Z1_plot
# background * (1 - Z1_plot)
Z_color[:, :, 0] = Z2_color[:, :, 0] * Z1_plot + background[0] * (1 - Z1_plot)
Z_color[:, :, 1] = Z2_color[:, :, 1] * Z1_plot + background[1] * (1 - Z1_plot)
Z_color[:, :, 2] = Z2_color[:, :, 2] * Z1_plot + background[2] * (1 - Z1_plot)

fig = plt.figure(figsize=(5,5))
ax = fig.add_subplot(111)
ax.imshow(Z_color[:, :, 0:3], origin='lower', interpolation='nearest', cmap=cm)
ax.set_xlabel('test')
ax.set_yscale('log')

ax.set_yticklabels(ax.get_yticks())
print([item.get_text() for item in ax.get_yticklabels()])

ax.set_yticklabels(["", "", "ab", "cd", "", ""])
# -


