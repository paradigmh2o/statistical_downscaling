# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:27:42 2022

@author: Zhao

This script pulls all netCDF data for user specified GCMs and RCPs.
The retrieved data is LOCA downscaled daily precipitation data.
One netCDF file per year.
Optional: clip data to a user specified bounding box (lat/long);
merge all files for a given GCM and RCP into a single time series
"""

from bs4 import BeautifulSoup
import urllib
import requests
import os
import xarray as xr
from tqdm import tqdm

parentlink = "http://albers.cnr.berkeley.edu/data/scripps/loca/met/"  # weblink for the data
outpth = r"C:\Users\Ben\Box\Projects\County of San Diego\TO4_SpringValley\3_Data\WeatherData\CalAdapt_FutureClimate\Raw_GCM_Data"

# gcms = ["ACCESS1-0", "ACCESS1-3", "CCSM4", "CESM1-BGC", "CESM1-CAM5", "CMCC-CM", "CMCC-CMS", "CNRM-CM5",
#         "CSIRO-Mk3-6-0", "CanESM2", "EC-EARTH", "FGOALS-g2", "GFDL-CM3", "GFDL-ESM2G", "GFDL-ESM2M", "GISS-E2-H",
#         "GISS-E2-R", "HadGEM2-AO", "HadGEM2-CC", "HadGEM2-ES", "IPSL-CM5A-LR", "IPSL-CM5A-MR", "MIROC-ESM",
#         "MIROC-ESM-CHEM", "MIROC5", "MPI-ESM-LR", "MPI-ESM-MR", "MRI-CGCM3", "NorESM1-M", "bcc-csm1-1",
#         "bcc-csm1-1-m", "inmcm4"]  # TODO get GCM list automatically from repository
rcps = ['historical', 'rcp45', 'rcp85']
gcms = ["GISS-E2-R", "HadGEM2-AO", "HadGEM2-CC", "HadGEM2-ES", "IPSL-CM5A-LR", "IPSL-CM5A-MR", "MIROC-ESM",
        "MIROC-ESM-CHEM", "MIROC5", "MPI-ESM-LR", "MPI-ESM-MR", "MRI-CGCM3", "NorESM1-M", "bcc-csm1-1",
        "bcc-csm1-1-m", "inmcm4"]  # 10 selected GCMs

# Spring Valley lat/lon bounding box
min_lon = -117.6
min_lat = 32.53
max_lon = -116.01
max_lat = 33.58

# loop over GCMs
for gcm in gcms:
    # make GCM folder
    folder = os.path.join(outpth, gcm)
    if not os.path.isdir(folder):
        os.makedirs(folder)

    # loop over RCPs
    for rcp in rcps:
        subfolder = os.path.join(folder, rcp)
        if not os.path.isdir(subfolder):
            os.makedirs(subfolder)

        # build URL
        temp_link = parentlink + "{}/{}/pr/".format(gcm, rcp)

        # get list of all files
        r = requests.get(temp_link)
        data = r.text
        soup = BeautifulSoup(data)

        # Download NetCDF data
        for link in soup.find_all('a'):
            if link.get('href').endswith('.nc'):
                filename = os.path.join(folder, subfolder, link.get('href'))
                print('Downloading: ', link.get('href'))
                fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
                urllib.request.urlretrieve(fulllink, filename)  # download specified dataset

        # # Clip and merge files into single time series
        # print(gcm, rcp)
        # files = []
        # files += [x for x in os.listdir(os.path.join(folder, subfolder)) if x.endswith('.nc')]
        # files.sort()
        # for index, file in enumerate(tqdm(files)):
        #     path = os.path.join(folder, subfolder, file)
        #     ds = xr.open_dataset(path).load()
        #     ds['lon'] = ds['lon'] - 360  #  the original longitude values are greater than 180, convert the values to the range (-180, 180)
        #     mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
        #     mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
        #     cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
        #     if index == 0:
        #         fds = cropped_ds
        #     else:
        #         fds = xr.merge([fds, cropped_ds])
        #
        # #
        # # ds = xarray.open_mfdataset(r'C:\Projects\SpringValley\NC_Data\ACCESS1-0_rcp45\*.nc')
        # fds.to_netcdf(os.path.join(outpth, "Merged_TS_Raw", '{}_{}_Merged.nc'.format(gcm, rcp)))
