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
#     display_name: pyseas
#     language: python
#     name: pyseas
# ---

# # Ports of Identity Changes 

# +
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import pyseas
from pyseas import maps#, cm, styles, util
import pyseas.props
# from pyseas.contrib import plot_tracks
# from pyseas.maps import scalebar, core, rasters, ticks
import imp
# import matplotlib.colors as colors
from cartopy import crs
from mpl_toolkits.axes_grid1.inset_locator import inset_axes


# -

#
# Pyseas reload
def reload():
#     imp.reload(util)
#     imp.reload(ticks)
#     imp.reload(scalebar)
    imp.reload(pyseas.props)
#     imp.reload(cm)
#     imp.reload(styles)
#     imp.reload(rasters)
#     imp.reload(core)
    imp.reload(maps)
#     imp.reload(plot_tracks)
    imp.reload(pyseas)


# +
#
# Colors and the function that adds pie charts in the map
colors_for_data = ['#d73b68', '#204280']
text_color = '#363c4c'

def plot_pie_inset(data, ilon, ilat, ax, width, colors, lw):
    ax_sub= inset_axes(ax, width=width, height=width, loc=10, 
                       bbox_to_anchor=(ilon, ilat),
                       bbox_transform=ax.transData, 
                       borderpad=0)
    wedges,texts= ax_sub.pie(data, colors=colors)
    for w in wedges:
        w.set_edgecolor('black')
        w.set_linewidth(lw)
        w.set_alpha(0.6)
        w.set_zorder(1)

    ax_sub.set_aspect("equal")


# -

#
# Function that creates a pyseas map with identity changes at ports
def plot_identity_change_ports(df, max_count, position_adjust, title):
    """
    df: DataFrame, Data for identity changes at ports
    max_count: Int, Number of top ports to be displayed
    position_adjust: Dict, Adjustment mapping for each port information
    title: Str, Title of the figure
    
    Return: None
    """
    
    with pyseas.context(pyseas.styles.light):
        
        fig = plt.figure(figsize=(10, 10), dpi=500)
        ax = maps.create_map()
        maps.add_land(ax)
        maps.add_countries(ax)

        for n in range(max_count):
            temp = df.iloc[n]
            if temp.port_label == "Outside ports":
                lonr, latr =  crs.EqualEarth().transform_point(
                    0 + position_adjust[temp.port_label][0], 
                    0 + position_adjust[temp.port_label][1], 
                    crs.PlateCarree())
                plot_pie_inset([temp.foreign_both, temp.total - temp.foreign_both],
                               lonr, latr, ax, np.log(temp.total) * 0.07, colors_for_data, 0)


                lonr, latr =  crs.EqualEarth().transform_point(
                    0 + position_adjust[temp.port_label][0] + position_adjust[temp.port_label][2], 
                    0 + position_adjust[temp.port_label][1] + position_adjust[temp.port_label][3] - 5.5, 
                    crs.PlateCarree())
                ax.text(lonr, latr, "Reflagging taking place\noutside any port (>10 km)",
                         color=text_color, fontsize=6, #fontweight='bold', 
                         zorder=3) #, horizontalalignment='center')
                
            else:
                lonr, latr =  crs.EqualEarth().transform_point(
                    temp.lon + position_adjust[temp.port_label][0], 
                    temp.lat + position_adjust[temp.port_label][1], 
                    crs.PlateCarree())
                plot_pie_inset([temp.foreign_both, temp.total - temp.foreign_both],
                               lonr, latr, ax, np.log(temp.total) * 0.07, colors_for_data, 0)


                lonr, latr =  crs.EqualEarth().transform_point(
                    temp.lon + position_adjust[temp.port_label][0] + position_adjust[temp.port_label][2], 
                    temp.lat + position_adjust[temp.port_label][1] + position_adjust[temp.port_label][3] - 5.5, 
                    crs.PlateCarree())
                ax.text(lonr, latr, temp.port_label.title() + ", " + temp.port_iso3, 
                         color=text_color, fontsize=6, #fontweight='bold', 
                         zorder=3) #, horizontalalignment='center')

        #
        # A dummy pie chart for the legend
        llon, llat = 5, -55
        lonr, latr =  crs.EqualEarth().transform_point(
            llon + position_adjust['100'][0], 
            llat + position_adjust['100'][1], 
            crs.PlateCarree())
        plot_pie_inset([1], lonr, latr, ax, np.log(100) * 0.07, ['none'], 0.5)
        lonr, latr =  crs.EqualEarth().transform_point(
            llon + position_adjust['100'][0] + position_adjust['100'][2], 
            llat + position_adjust['100'][1] + position_adjust['100'][3],
            crs.PlateCarree())
        ax.text(lonr, latr, '100 Events', color=text_color, fontsize=6, zorder=3)
        lonr, latr =  crs.EqualEarth().transform_point(
            llon + position_adjust['10'][0], 
            llat + position_adjust['10'][1],
            crs.PlateCarree())
        plot_pie_inset([1], lonr, latr, ax, np.log(10) * 0.07, ['none'], 0.5)
        lonr, latr =  crs.EqualEarth().transform_point(
            llon + position_adjust['10'][0] + position_adjust['10'][2], 
            llat + position_adjust['10'][1] + position_adjust['10'][3],
            crs.PlateCarree())
        ax.text(lonr, latr, '10 Events', color=text_color, fontsize=6, zorder=3)

        #
        # A dummy color patch for the legend
        llon, llat = 20, -52
        lonr, latr =  crs.EqualEarth().transform_point(llon, llat, crs.PlateCarree())
        plot_pie_inset([1], lonr, latr, ax, np.log(4) * 0.07, [colors_for_data[0]], 0)
        llon, llat = 23.5, -52.7
        lonr, latr =  crs.EqualEarth().transform_point(llon, llat, crs.PlateCarree())
        ax.text(lonr, latr, 'Foreign-to-foreign reflagging with respect to port State', 
                color=text_color, fontsize=8, zorder=3)
        llon, llat = 21, -58
        lonr, latr =  crs.EqualEarth().transform_point(llon, llat, crs.PlateCarree())
        plot_pie_inset([1], lonr, latr, ax, np.log(4) * 0.07, [colors_for_data[1]], 0)
        llon, llat = 25, -58.9
        lonr, latr =  crs.EqualEarth().transform_point(llon, llat, crs.PlateCarree())
        ax.text(lonr, latr, 'Domestic-to-foreign or foreign-to-domestic reflagging', 
                color=text_color, fontsize=8, zorder=3)


        ax.set_title(title, pad=5, fontsize=9)

        maps.add_figure_background(fig)
    #     maps.add_eezs()
    #             gl = maps.add_gridlines()
    #             maps.add_logo(ax, name='black_logo.png', loc='lower left', alpha=1, scale=0.6)

    #             plt.savefig('./{}_{}.png'.format(name, i),  
    #                         dpi=250, facecolor=plt.rcParams['pyseas.fig.background'],
    #                         bbox_inches = 'tight')
        plt.show()
        


#
# Get all vessels' ports of identity change
q = """
SELECT *
FROM `vessel_identity.identity_change_ports_all_v20220701`
ORDER BY total DESC#, port_iso3 IN ("CHN", "ESH") DESC
"""
df_all = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Map ports of identity change for fishing + support vessels
reload()

position_adjust = {
    'LAS PALMAS': [0, 1, 5.5, 1.5],
    'ZHOUSHAN': [0, 0, 2, 1],
    "BUSAN": [1, 3, 7, 10],
    'KAOHSIUNG': [1, -1, 2, 1],
    'SINGAPORE': [0, 0, 2, 1],
    'SKAGEN HAVN': [1.7, 0.5, -46, 5.5],
    'WALVIS BAY': [0, 0, 0, 0],
    'PORT LOUIS': [0, 0, 0, 0.5],
    'ALESUND': [-1.5, 2.9, -20, 11.],
    'TALLINN': [-2.3, 0, 0.9, -0.5],
    'SAINT PETERSBURG': [1.5, 4.5, 4.5, 4],
    'REYKJAVIK': [2, 0.7, -38, 11.3],
    'SHIMIZU': [0.5, 2.5, 1.5, 3],
    'PUERTO MONTT': [0, 0, 0, 1],
    'MONTEVIDEO': [0, 0, 0, 1],
    'CANGAS': [0, 0, 3, 1],
    'DALIAN': [0, 0, -30, 5],
    'POHNPEI': [0, 0, -3, 1.],
    'NOUADHIBOU': [0, 0, 3, 3.5],
    'IJMUIDEN': [-1, -1.3, 2, 0.5],
    'KIRKENES': [4, 1.5, 5, 5],
    'KILKEEL': [0, -2.3, -25, 1],
    'TUZLA': [0, -1, 2, 1.5],
    'FUZHOU': [-1.5, -1, -29, 5.5],
    'CALLAO': [0, 0, 3, 4],
    'MANTA': [0, 0, 3, 4],
    'GENERAL SANTOS': [0, 0, -8, 1.5],
    'SUVA': [-2, 0, -14, 9],
    'BAYOU LA BATRE': [0, 0, 4, 3.5],
    'ADMIRALTEISKY': [-1, -0.3, 3, 3.3],
    'GALVESTON': [-0.5, -0.5, 1, 1.5],
    'KLAIPEDA': [-1, -1.8, 2.5, 1.6],
    'GUANGZHOU': [0, 0, -34, 4.5],
    'CAPE TOWN': [0, 0, 0, 0.5],
    'PORT LYTTELTON': [0, 0, -24, 2.5],
    'KODIAK': [0, 0, 0, 0],
    'CHIMBOTE': [0, 1, 3, 4.5],
    'ABIDJAN': [0, 0, 0, 2],
    'DAKAR': [0, 0, 4, 3],
    'HAFNARFJORDUR': [-4, 0, -44, 3.5],
    'THYBORON': [-0.2, -2.4, -36, 6.1],
    'GDANSK': [-2, -2.8, 0.5, 2.4],
    'PERAMA': [0, 0, 0, 1],
    'STELLENDAM': [6.5, 0.5, 1.5, 1],
    'WEIHAI': [-5, -4, -29, 5.5],
    'VLADIVOSTOK': [-2, 1, -37, 5.5],
    'FREDERIKSHAVN': [1.8, -5, 1, 0.8],
    'Outside ports': [-140, 10, 4, -2],
    '100': [0, 0, -14.5, 6.5],
    '10': [0.3, -2.9, -12.3, 3.7],
}

plot_identity_change_ports(df_all, 40, position_adjust, 
    'Foreign Flags Involved When Fishing and Support Vessels Reflag at Top 40 Ports (2012-2021)')
# -

#
# Get fishing vessels' ports of identity change
q = """
SELECT *
FROM `vessel_identity.identity_change_ports_fishing_v20220701`
"""
df_fishing = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Map ports of identity change for fishing vessels
reload()

position_adjust = {
    'LAS PALMAS': [0, 1, 5.5, 1.5],
    'ZHOUSHAN': [0, 0, 2, 1],
    "BUSAN": [1, 3, 7, 10],
    'KAOHSIUNG': [1, -1, 2, 1],
    'SINGAPORE': [0, 0, 2, 1],
    'SKAGEN HAVN': [1.7, 0.5, -46, 5.5],
    'WALVIS BAY': [0, 0, 0, 0],
    'PORT LOUIS': [0, 0, 0, 0.5],
    'ALESUND': [-1.5, 2.9, -20, 11.],
    'TALLINN': [-2.3, 0, 0.9, -0.5],
    'SAINT PETERSBURG': [1.5, 4.5, 4.5, 4],
    'REYKJAVIK': [2, 0.7, -38, 11.3],
    'SHIMIZU': [0.5, 2.5, 1.5, 3],
    'PUERTO MONTT': [0, 0, 0, 1],
    'MONTEVIDEO': [0, 0, 0, 1],
    'CANGAS': [0, 0, 3, 1],
    'DALIAN': [0, 0, -27, 5],
    'POHNPEI': [0, 0, -3, 1.],
    'NOUADHIBOU': [0, 0, 3, 3.5],
    'IJMUIDEN': [-1, -1.3, 2, 0.5],
    'KIRKENES': [4, 1.5, 5, 5],
    'KILKEEL': [0, -2.3, -25, 1],
    'TUZLA': [0, -1, 2, 1.5],
    'FUZHOU': [-1.5, -1, -29, 5.5],
    'CALLAO': [0, 0, 3, 4],
    'MANTA': [0, 0, 3, 4],
    'GENERAL SANTOS': [0, 0, -8, 1.5],
    'SUVA': [-2, 0, -14, 9],
    'BAYOU LA BATRE': [0, 0, 4, 3.5],
    'ADMIRALTEISKY': [-1, -0.3, 3, 3.3],
    'GALVESTON': [-0.5, -0.5, 1, 1.5],
    'KLAIPEDA': [-1, -1.8, 2.5, 1.6],
    'GUANGZHOU': [0, 0, -34, 4.5],
    'CAPE TOWN': [0, 0, 0, 0.5],
    'PORT LYTTELTON': [0, 0, -24, 2.5],
    'KODIAK': [0, 0, 0, 0],
    'CHIMBOTE': [0, 1, 3, 4.5],
    'ABIDJAN': [0, 0, 0, 2],
    'DAKAR': [0, 0, 4, 3],
    'HAFNARFJORDUR': [-4, 0, -44, 3.5],
    'THYBORON': [-0.2, -2.4, -36, 6.1],
    'GDANSK': [-2, -2.8, 0.5, 2.4],
    'PERAMA': [0, 0, 0, 1],
    'STELLENDAM': [6.5, 0.5, 1.5, 1],
    'WEIHAI': [-5, -4, -29, 5.5],
    'VLADIVOSTOK': [-2, 1, -37, 5.5],
    'FREDERIKSHAVN': [1.8, -5, 2, 2],
    'DAKHLA': [0, 0, 3, 3.5],
    'SCHEVENINGEN': [0, 0, 0, 2],
    'MOANA': [0, 0, 0, 2],
    'AKUREYRI': [0, 0, 0, 2],
    'YALOVA': [0, 0, 0, 2],
    'AGADIR': [0, 0, 2, 4],
    'HANSTHOLM': [0, 0, 0, 2],
    'MARIN': [0, 0, 0, 2],
    'LA CORUNA': [0, 0, 0, 2],
    'KLAKSVIK': [0, 0, 0, 2],
    'Outside ports': [-150, 10, 4, -2],
    '100': [0, 0, -14.5, 6.5],
    '10': [0.3, -2.9, -12.3, 3.7],
}

plot_identity_change_ports(df_fishing, 30, position_adjust, 
    'Foreign Flags Involved When Fishing Vessels Reflag at Top 30 Ports (2012-2021)')

# -

#
# Get support vessels' ports of identity change
q = """
SELECT *
FROM `vessel_identity.identity_change_ports_support_v20220701`
"""
df_support = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
#
# Map ports of identity change for support vessels
reload()

position_adjust = {
    'LAS PALMAS': [0, 1, 5.5, 1.5],
    'ZHOUSHAN': [0, 0, 1.8, 0.8],
    "BUSAN": [3, 3, 7, 10],
    'KAOHSIUNG': [1, -1, 2, 1],
    'SINGAPORE': [0, 0, 2, 1],
    'SKAGEN HAVN': [1.7, 0.5, -46, 5.5],
    'WALVIS BAY': [0, 0, 0, 0],
    'PORT LOUIS': [0, 0, 0, 0.5],
    'ALESUND': [-1.5, 2.9, -20, 11.],
    'TALLINN': [-2.3, 0, 1.1, 0.5],
    'SAINT PETERSBURG': [1.5, 4.5, 4.5, 4],
    'REYKJAVIK': [2, 0.7, -38, 11.3],
    'SHIMIZU': [2.5, 4, 1.5, 4],
    'PUERTO MONTT': [0, 0, 0, 1],
    'MONTEVIDEO': [0, 0, 0, 1],
    'CANGAS': [0, 0, 3, 1],
    'DALIAN': [0, 0, -27, 5],
    'POHNPEI': [0, 0, -3, 1.],
    'NOUADHIBOU': [0, 0, 3, 3.5],
    'IJMUIDEN': [-1, -1.3, 2, 0.5],
    'KIRKENES': [4, 1.5, 5, 5],
    'KILKEEL': [0, -2.3, -25, 1],
    'TUZLA': [0, -1, 2, 1.5],
    'FUZHOU': [-1.5, -1, -29, 5.5],
    'CALLAO': [0, 0, 3, 4],
    'MANTA': [0, 0, 3, 4],
    'GENERAL SANTOS': [0, 0, -8, 1.5],
    'SUVA': [-2, 0, -14, 9],
    'BAYOU LA BATRE': [0, 0, 4, 3.5],
    'ADMIRALTEISKY': [-1, -0.3, 3.3, 4],
    'GALVESTON': [-0.5, -0.5, 1, 1.5],
    'KLAIPEDA': [-1, -1.8, 2.5, 1.6],
    'GUANGZHOU': [0, 0, -34, 4.5],
    'CAPE TOWN': [0, 0, 0, 0.5],
    'PORT LYTTELTON': [0, 0, -24, 2.5],
    'KODIAK': [0, 0, 0, 0],
    'CHIMBOTE': [0, 1, 3, 4.5],
    'ABIDJAN': [0, 0, 0, 2],
    'DAKAR': [0, 0, 2, 3],
    'HAFNARFJORDUR': [-4, 0, -44, 3.5],
    'THYBORON': [-0.2, -2.4, -36, 6.1],
    'GDANSK': [-2, -2.8, 0.5, 2.4],
    'PERAMA': [0, 0, 0, 0.3],
    'STELLENDAM': [6.5, 0.5, 1.5, 1],
    'WEIHAI': [-2, 0, -27, 5],
    'VLADIVOSTOK': [-2, 1, -37, 5.5],
    'FREDERIKSHAVN': [1.8, -5, 2, 2],
    'PANAMA CITY': [0, 0, 3, 3.5],
    'BUKHTA GAYDAMAK': [0, 0, 0, 2],
    'BANG KHO LAEM': [0, 0, 0, 2],
    'PUERTO BOLIVAR': [0, 0, 0, 2],
    'GDYNIA': [0, 0, 0, 2],
    'CARAMINAL': [0, 0, 2, 4],
    'MASAN': [2., -2.3, -2, 0.9],
    'HIROSHIMA': [2, -0.5, 0, 2.8],
    'ROTTERDAM': [0, 0, 2, 2],
    'ANTWERP': [-2, -2, 0.5, 0.7],
    'FUJAIRAH': [0, 0, -25, 2],
    'ONOMICHI-ITOZAKI': [6, 1, 0, 4.5],
    'Outside ports': [-150, 10, 4, -2],
    '100': [0, 0, -14.5, 6.5],
    '10': [0.3, -2.9, -12.3, 3.7],
}

plot_identity_change_ports(df_support, 30, position_adjust, 
    'Foreign Flags Involved When Support Vessels Reflag at Top 30 Ports (2012-2021)')
# -


# +
# import rendered
# rendered.publish_to_github('./ports_of_identity_changes.ipynb', 
#                            'vessel-identity-paper/', action='push')
# -







