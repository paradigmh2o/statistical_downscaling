# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:27:42 2022

@author: Zhao

This script pulls all netCDF data for user specified GCMs and RCPs.
The retrieved data is LOCA downscaled daily precip., relative humidity, wind speed, and solar radiation
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

# parentlink = "http://albers.cnr.berkeley.edu/data/scripps/loca/met/"  # weblink for the data
parentlink = "http://albers.cnr.berkeley.edu/data/scripps/loca/"  # weblink for the data
base_dir = r"C:\Users\{}\Box\Projects\City of San Diego\IDEAs\ClimateChange\CalAdapt\netCDF\Daily".format(os.getlogin())
# outpth = os.path.join(base_dir, "Raw")
# mrg_dir = os.path.join(base_dir, "Merged")
#
# if not os.path.exists(mrg_dir):
#     os.makedirs(mrg_dir)

# met_vars = ['solards', 'wspeed', 'rel_humid']
met_vars = ['tasmin', 'tasmax']
rcps = ['rcp85']  # 'historical', 'rcp45',
gcms = ["ACCESS1-0", "CCSM4", "CESM1-BGC", "CMCC-CMS", "CNRM-CM5",
        "CanESM2", "GFDL-CM3", "HadGEM2-CC", "HadGEM2-ES", "MIROC5"]  # CCTAG GCM subset
# sel_gcms = ["HadGEM2-ES", "CNRM-CM5", "CanESM2", "MIROC5"]  # 4 priority GCMs
sel_gcms = ["CanESM2"]  # 4 priority GCMs TODO CanESM2_rcp85 tasmin and tasmax

# lat/lon bounding box
min_lon = -117.258
min_lat = 32.565
max_lon = -116.802
max_lat = 32.950

# loop over GCMs
# for gcm in gcms:
for gcm in sel_gcms:
    # loop over RCPs
    for rcp in rcps:
        print(gcm, rcp)

        # # Get precip for all GCMs/RCPs
        # subfolder = os.path.join(base_dir, "precip", "Raw", gcm, rcp)
        # mrg_dir = os.path.join(base_dir, "precip", "Merge_Clip")
        # for i in [subfolder, mrg_dir]:
        #     if not os.path.exists(i):
        #         os.makedirs(i)

        # temp_link = parentlink + "met/{}/{}/pr/".format(gcm, rcp)  # build URL
        #
        # # get list of all files
        # r = requests.get(temp_link)
        # data = r.text
        # soup = BeautifulSoup(data)
        #
        # # Download NetCDF data
        # for link in soup.find_all('a'):
        #     if link.get('href').endswith('.nc'):
        #         filename = os.path.join(subfolder, link.get('href'))
        #         print('Downloading: ', link.get('href'))
        #         fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
        #         urllib.request.urlretrieve(fulllink, filename)  # download specified dataset
        #
        # # Clip and merge files into single time series
        # files = []
        # files += [x for x in os.listdir(subfolder) if x.endswith('.nc')]
        # files.sort()
        # for index, file in enumerate(tqdm(files)):
        #     path = os.path.join(subfolder, file)
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
        # fds.to_netcdf(os.path.join(mrg_dir, '{}_{}_MergeClip.nc'.format(gcm, rcp)))

        # get non-precip data for 4 priority GCMs
        if gcm in sel_gcms:
            for met_var in met_vars:  # loop over non-precip variables
                print(met_var)
                subfolder = os.path.join(base_dir, met_var, "Raw", gcm, rcp)
                mrg_dir = os.path.join(base_dir, met_var, "Merge_Clip")
                for i in [subfolder, mrg_dir]:
                    if not os.path.exists(i):
                        os.makedirs(i)

                if met_var == 'rel_humid':
                    # temp_link = parentlink + "{}/{}/{}/".format(met_var, gcm, rcp)  # build URL
                    #
                    # # get list of all files
                    # r = requests.get(temp_link)
                    # data = r.text
                    # soup = BeautifulSoup(data)
                    #
                    # # Download NetCDF data
                    # for link in soup.find_all('a'):
                    #     if link.get('href').endswith('.nc') and 'monthly' not in link.get('href'):
                    #         filename = os.path.join(subfolder, link.get('href'))
                    #         print('Downloading: ', link.get('href'))
                    #         fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
                    #         urllib.request.urlretrieve(fulllink, filename)  # download specified dataset

                    # Clip and merge files into single time series
                    for i in ['min', 'max']:
                        files = []
                        files += [x for x in os.listdir(subfolder) if x.endswith('.nc') and i in x]
                        files.sort()
                        for index, file in enumerate(tqdm(files)):
                            path = os.path.join(subfolder, file)
                            ds = xr.open_dataset(path).load()
                            ds['lon'] = ds['lon'] - 360  # the original longitude values are greater than 180, convert the values to the range (-180, 180)
                            mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
                            mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
                            cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
                            if index == 0:
                                fds = cropped_ds
                            else:
                                fds = xr.merge([fds, cropped_ds])

                        fds.to_netcdf(os.path.join(mrg_dir, '{}_{}_{}_MergeClip.nc'.format(gcm, rcp, i)))

                if met_var in ['tasmin', 'tasmax']:
                    temp_link = parentlink + "met/{}/{}/{}/".format(gcm, rcp, met_var)  # build URL

                    # get list of all files
                    r = requests.get(temp_link)
                    data = r.text
                    soup = BeautifulSoup(data)

                    # Download NetCDF data
                    for link in soup.find_all('a'):
                        if link.get('href').endswith('.nc') and rcp in link.get('href'):
                            filename = os.path.join(subfolder, link.get('href'))
                            print('Downloading: ', link.get('href'))
                            fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
                            urllib.request.urlretrieve(fulllink, filename)  # download specified dataset

                    # Clip and merge files into single time series
                    files = []
                    files += [x for x in os.listdir(subfolder) if x.endswith('.nc')]
                    files.sort()
                    for index, file in enumerate(tqdm(files)):
                        path = os.path.join(subfolder, file)
                        ds = xr.open_dataset(path).load()
                        ds['lon'] = ds['lon'] - 360  # the original longitude values are greater than 180, convert the values to the range (-180, 180)
                        mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
                        mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
                        cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
                        if index == 0:
                            fds = cropped_ds
                        else:
                            fds = xr.merge([fds, cropped_ds])

                    fds.to_netcdf(os.path.join(mrg_dir, '{}_{}_MergeClip.nc'.format(gcm, rcp)))

                else:
                    # temp_link = parentlink + "{}/{}/".format(met_var, gcm)  # build URL
                    #
                    # # get list of all files
                    # r = requests.get(temp_link)
                    # data = r.text
                    # soup = BeautifulSoup(data)
                    #
                    # # Download NetCDF data
                    # for link in soup.find_all('a'):
                    #     if link.get('href').endswith('.nc') and rcp in link.get('href'):
                    #         filename = os.path.join(subfolder, link.get('href'))
                    #         print('Downloading: ', link.get('href'))
                    #         fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
                    #         urllib.request.urlretrieve(fulllink, filename)  # download specified dataset

                    # Clip and merge files into single time series
                    files = []
                    files += [x for x in os.listdir(subfolder) if x.endswith('.nc')]
                    files.sort()
                    for index, file in enumerate(tqdm(files)):
                        path = os.path.join(subfolder, file)
                        ds = xr.open_dataset(path).load()
                        ds['lon'] = ds['lon'] - 360  #  the original longitude values are greater than 180, convert the values to the range (-180, 180)
                        mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
                        mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
                        cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
                        if index == 0:
                            fds = cropped_ds
                        else:
                            fds = xr.merge([fds, cropped_ds])

                    fds.to_netcdf(os.path.join(mrg_dir, '{}_{}_MergeClip.nc'.format(gcm, rcp)))
