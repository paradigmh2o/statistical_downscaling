# -*- coding: utf-8 -*-
"""
January 2, 2024

@author: Benjamin Bowes

This script merges NLDAS forcing data (in netCDF format) into a single time series
Optional: clip data to a user specified bounding box (lat/long)
"""

import os
import xarray as xr
from tqdm import tqdm

base_dir = r"F:\Project\CityofSanDiego\NLDAS"
in_dir = os.path.join(base_dir, "Raw")
out_dir = os.path.join(base_dir, "Merged")

if not os.path.exists(out_dir):
    os.mkdir(out_dir)

# Clip files?
clip = False

# lat/lon bounding box
min_lon = -117.6
min_lat = 32.53
max_lon = -116.01
max_lat = 33.58

# Clip and merge files into single time series
files = []
files += [x for x in os.listdir(in_dir) if x.endswith('.nc4')]
files.sort()

for index, file in enumerate(tqdm(files)):
    path = os.path.join(in_dir, file)
    ds = xr.open_dataset(path).load()

    # clip
    if clip:
        mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
        mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
        cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
    else:
        cropped_ds = ds

    if index == 0:
        fds = cropped_ds  # initialize merged array
    else:
        fds = xr.merge([fds, cropped_ds])

# save merged files
fds.to_netcdf(os.path.join(out_dir, 'SD_NLDAS_Merged.nc4'.format()))
