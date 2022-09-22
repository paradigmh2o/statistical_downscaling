# -*- coding: utf-8 -*-
"""
Created on Wed Sept 21 15:27:42 2022

@author: Benjamin Bowes

This script reads predownloaded netCDF data for user specified GCMs and RCPs.
Data is clipped to a user specified bounding box(es) (lat/long).

Optionally the LOCA grid cells can be pulled from Cal Adapt and saved for GIS applications.
"""

import requests
import os
import xarray as xr
from tqdm import tqdm
import numpy as np
from geojson import dump
import pandas as pd

# parentlink = "http://albers.cnr.berkeley.edu/data/scripps/loca/met/ACCESS1-0/rcp45/pr/"  # weblink for the data
# parentlink = "http://albers.cnr.berkeley.edu/data/scripps/loca/met/"  # weblink for the data
outpth = r"C:\Users\Ben\Box\Projects\County of San Diego\TO4_SpringValley\3_Data\WeatherData\CalAdapt_FutureClimate\Raw_GCM_Data"
deskpth = r"C:\Users\Ben\Desktop\SpringValley_loca_test\timeseries"

gcms = ["ACCESS1-0", "CCSM4", "CESM1-BGC", "CMCC-CMS", "CNRM-CM5",
        "CanESM2", "GFDL-CM3", "HadGEM2-CC", "HadGEM2-ES", "MIROC5"]
# gcms = ["ACCESS1-0"]
rcps = ['historical', 'rcp45', 'rcp85']

# get loca grid
# TODO find way to get total 'count' of features and put that in page size
response = requests.get('https://api.cal-adapt.org/api/locagrid/?format=geojson&pagesize=26517')
if response.ok:
    data = response.json()

    # # save geojson if needed
    # with open(os.path.join(deskpth, 'loca_grid3.geojson'), 'w') as f:
    #     dump(data, f)

    # get coordinates for Spring Valley grid cells
    SV_names = ['-117.03125, 32.78125', '-116.96875, 32.78125', '-117.03125, 32.71875', '-116.96875, 32.71875',
                '-117.09375, 32.65625', '-117.03125, 32.65625', '-116.96875, 32.65625']

    geom, cell_name = [], []
    for feat in data['features']:
        if feat['properties']['name'] in SV_names:
            print(feat['properties']['name'], ':', feat['geometry']['coordinates'][0][:4])
            geom.append(feat['geometry']['coordinates'][0][:4])
            cell_name.append(feat['properties']['name'])


# # Spring Valley lat/lon bounding box
# min_lon = -117.2
# min_lat = 32.574
# max_lon = -116.94
# max_lat = 32.82

# loop over GCMs
dfs = []
for gcm in gcms:
    # make GCM folder
    folder = os.path.join(outpth, gcm)
    # if not os.path.isdir(folder):
    #     os.makedirs(folder)

    # loop over RCPs
    for rcp in rcps:
        subfolder = os.path.join(folder, rcp)
        # if not os.path.isdir(subfolder):
        #     os.makedirs(subfolder)

        files = []
        files += [x for x in os.listdir(os.path.join(folder, subfolder)) if x.endswith('.nc')]
        files.sort()

        # loop over Spring Valley grid cells
        for cell, name in zip(geom, cell_name):
            print(name, cell)
            min_lon = cell[3][0]
            min_lat = cell[3][1]
            max_lon = cell[1][0]
            max_lat = cell[1][1]

            # Clip and merge files into single time series
            print(gcm, rcp, name)

            # extract cell from file
            vals, years = [], []
            for index, file in enumerate(tqdm(files)):
                path = os.path.join(folder, subfolder, file)
                ds = xr.open_dataset(path).load()
                ds['lon'] = ds['lon'] - 360  #  the original longitude values are greater than 180, convert the values to the range (-180, 180)
                mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
                mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
                cropped_ds = ds.where(mask_lon & mask_lat, drop=True)  # get area of interest

                # get total value for area of interest (each file is one year so this is annual total)
                # convert kg/m^2/s to inches/yr
                # grid cells are 40584038.409m^2, 25.4mm/inch
                # assuming density of water 1000 [kg m-3], value in [kg m-2] is identical to the value in [mm]
                fut_vals = np.sum(cropped_ds.pr.values) * 60 * 60 * 24 / 25.4
                year = cropped_ds.time.values[0].astype('datetime64[Y]').astype('int') + 1970

                vals.append(fut_vals)
                years.append(year)

                df = pd.DataFrame(vals, index=years, columns=['Total (in/yr)'])
                df['SV_GridName'] = name
                df['GCM'] = gcm
                df['RCP'] = rcp

                dfs.append(df)

out_df = pd.concat(dfs, axis=0)
out_df.to_csv(os.path.join(deskpth, "yearly_totals.csv"))
