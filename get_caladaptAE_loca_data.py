# -*- coding: utf-8 -*-
"""
Benjmain Bowes, March 21, 2024

This script pulls CMIP6 LOCA downscaled daily precip. from the Cal-Adapt Analytics Engine AWS S3 bucket.
Data is clipped to a project area and saved as netCDF files by GCM and SSP.

https://analytics.cal-adapt.org/faq/
"""

import os
import xarray as xr
import pandas as pd

raw_dir = r"C:\Users\Ben\Box\Projects\County of San Diego\TO9_SLR RAD phase 2\3_Data\CalAdapt"

"""read csv of all data; from https://analytics.cal-adapt.org/faq/"""
s3_df = pd.read_csv(os.path.join(raw_dir, 'cae-zarr.csv'))
s3_df = s3_df.loc[(s3_df['activity_id'] == 'LOCA2') & (s3_df['institution_id'] == 'UCSD') & (s3_df['member_id'] == 'r1i1p1f1') & (s3_df['table_id'] == 'day') & (s3_df['variable_id'] == 'pr')]

# lat/lon bounding box
min_lon = -117.40625
min_lat = 33.15625
max_lon = -116.46875
max_lat = 33.40625

# loop over unique paths
for data_path in s3_df['path'].unique():
    print(data_path)

    gcm = s3_df['source_id'].loc[s3_df['path'] == data_path].iloc[0]
    ssp = s3_df['experiment_id'].loc[s3_df['path'] == data_path].iloc[0]

    out_dir = os.path.join(raw_dir, "precip")
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    # get xarray statewide dataset
    ds = xr.open_zarr(data_path, storage_options={'anon': True})

    # # create csv of grid points if needed
    # lat_lons = []
    # for i in ds['lat'].values:
    #     for j in ds['lon'].values:
    #         lat_lons.append([i, j])
    # lat_lon_df = pd.DataFrame(lat_lons, columns=['lat', 'lon'])
    # lat_lon_df.to_csv(os.path.join(raw_dir, 'LOCA_Grid_Points.csv'))

    # ds['lon'] = ds['lon'] - 360  # the original longitude values are greater than 180, convert the values to the range (-180, 180)
    mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
    mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
    cropped_ds = ds.where(mask_lon & mask_lat, drop=True)

    cropped_ds.to_netcdf(os.path.join(out_dir, '{}_{}.nc'.format(gcm, ssp)))
