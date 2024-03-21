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
from shutil import copyfileobj
import certifi
import ssl
import xarray as xr
from tqdm import tqdm

context = ssl._create_unverified_context()
# context = ssl.create_default_context(cafile=certifi.where())

# parentlink = "http://albers.cnr.berkeley.edu/data/scripps/loca/met/"  # weblink for the data
# parentlink = "http://albers.cnr.berkeley.edu/data/scripps/loca/"  # weblink for the data
parentlink = "https://cirrus.ucsd.edu/~pierce/LOCA2/CONUS_regions_split/"  # weblink for the data
raw_dir = r"C:\Users\Ben\Box\Data Library\Tabular Datasets\CMIP6_LOCA"
clip_dir = r"C:\Users\{}\Box\Projects\Barge Design Solutions\3_Data\LOCA\netCDF".format(os.getlogin())
# outpth = os.path.join(base_dir, "Raw")
# mrg_dir = os.path.join(base_dir, "Merged")
#
# if not os.path.exists(mrg_dir):
#     os.makedirs(mrg_dir)

# met_vars = ['solards', 'wspeed', 'rel_humid', 'tasmin', 'tasmax']
ssps = ['historical']  # , 'ssp245', 'ssp370', 'ssp585'
# gcms = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'AWI-CM-1-1-MR', 'BCC-CSM2-MR', 'CESM2-LENS',
#         'CNRM-CM6-1-HR', 'CNRM-CM6-1', 'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg',
#         'EC-Earth3', 'FGOALS-g3', 'GFDL-CM4', 'GFDL-ESM4', 'HadGEM3-GC31-LL', 'HadGEM3-GC31-MM',
#         'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KACE-1-0-G', 'MIROC6', 'MPI-ESM1-2-HR',
#         'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 'NorESM2-MM', 'TaiESM1']
# sel_gcms = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'AWI-CM-1-1-MR', 'BCC-CSM2-MR', 'CESM2-LENS',
#             'CNRM-CM6-1-HR', 'CNRM-CM6-1', 'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg',
#             'EC-Earth3', 'FGOALS-g3', 'GFDL-CM4', 'GFDL-ESM4', 'HadGEM3-GC31-LL', 'HadGEM3-GC31-MM',
#             'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KACE-1-0-G', 'MIROC6', 'MPI-ESM1-2-HR',
#             'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 'NorESM2-MM', 'TaiESM1']
sel_gcms = ['ACCESS-CM2']
# # sel_gcms = ["HadGEM2-ES", "CNRM-CM5", "CanESM2", "MIROC5"]  # 4 priority GCMs
# sel_gcms = ["CanESM2"]  # 4 priority GCMs TODO CanESM2_rcp85 tasmin and tasmax
region = 'cent'

# lat/lon bounding box
min_lon = -117.258
min_lat = 32.565
max_lon = -116.802
max_lat = 32.950

# loop over GCMs
# for gcm in gcms:
for gcm in sel_gcms:
    # loop over RCPs
    for ssp in ssps:
        print(gcm, ssp)

        # Get precip for all GCMs/RCPs
        subfolder = os.path.join(raw_dir, region, "precip", gcm, ssp)  # TODO add ensemble member folder (e.g., r1i1p1f1)?
        mrg_dir = os.path.join(clip_dir, "precip", "Merge_Clip")
        for i in [subfolder, mrg_dir]:
            if not os.path.exists(i):
                os.makedirs(i)

        """Example precip link
        https://cirrus.ucsd.edu/~pierce/LOCA2/CONUS_regions_split/ACCESS-CM2/cent/0p0625deg/r1i1p1f1/ssp245/pr/"""
        temp_link = parentlink + "{}/{}/0p0625deg/r1i1p1f1/{}/pr/".format(gcm, region, ssp)  # build URL

        # get list of all files
        r = requests.get(temp_link)
        data = r.text
        soup = BeautifulSoup(data)

        # Download NetCDF data
        for link in soup.find_all('a'):
            # print(str(link).split('"')[1][:-1])
            # if link.get('href').endswith('.nc'):
            if link.get('href').endswith('.nc') and 'monthly' not in link.get('href'):
                filename = os.path.join(subfolder, link.get('href'))
                print('Downloading: ', link.get('href'))
                fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
                # urllib.request.urlretrieve(fulllink, filename)  # download specified dataset
                with urllib.request.urlopen(fulllink, context=context) as in_stream, open(filename, 'wb') as out_file:
                    copyfileobj(in_stream, out_file)

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
        # fds.to_netcdf(os.path.join(mrg_dir, '{}_{}_MergeClip.nc'.format(gcm, ssp)))
        #
        # # get non-precip data for 4 priority GCMs
        # if gcm in sel_gcms:
        #     for met_var in met_vars:  # loop over non-precip variables
        #         print(met_var)
        #         subfolder = os.path.join(clip_dir, met_var, "Raw", gcm, ssp)
        #         mrg_dir = os.path.join(clip_dir, met_var, "Merge_Clip")
        #         for i in [subfolder, mrg_dir]:
        #             if not os.path.exists(i):
        #                 os.makedirs(i)
        #
        #         if met_var == 'rel_humid':
        #             # temp_link = parentlink + "{}/{}/{}/".format(met_var, gcm, rcp)  # build URL
        #             #
        #             # # get list of all files
        #             # r = requests.get(temp_link)
        #             # data = r.text
        #             # soup = BeautifulSoup(data)
        #             #
        #             # # Download NetCDF data
        #             # for link in soup.find_all('a'):
        #             #     if link.get('href').endswith('.nc') and 'monthly' not in link.get('href'):
        #             #         filename = os.path.join(subfolder, link.get('href'))
        #             #         print('Downloading: ', link.get('href'))
        #             #         fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
        #             #         urllib.request.urlretrieve(fulllink, filename)  # download specified dataset
        #
        #             # Clip and merge files into single time series
        #             for i in ['min', 'max']:
        #                 files = []
        #                 files += [x for x in os.listdir(subfolder) if x.endswith('.nc') and i in x]
        #                 files.sort()
        #                 for index, file in enumerate(tqdm(files)):
        #                     path = os.path.join(subfolder, file)
        #                     ds = xr.open_dataset(path).load()
        #                     ds['lon'] = ds['lon'] - 360  # the original longitude values are greater than 180, convert the values to the range (-180, 180)
        #                     mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
        #                     mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
        #                     cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
        #                     if index == 0:
        #                         fds = cropped_ds
        #                     else:
        #                         fds = xr.merge([fds, cropped_ds])
        #
        #                 fds.to_netcdf(os.path.join(mrg_dir, '{}_{}_{}_MergeClip.nc'.format(gcm, ssp, i)))
        #
        #         if met_var in ['tasmin', 'tasmax']:
        #             temp_link = parentlink + "met/{}/{}/{}/".format(gcm, ssp, met_var)  # build URL
        #
        #             # get list of all files
        #             r = requests.get(temp_link)
        #             data = r.text
        #             soup = BeautifulSoup(data)
        #
        #             # Download NetCDF data
        #             for link in soup.find_all('a'):
        #                 if link.get('href').endswith('.nc') and ssp in link.get('href'):
        #                     filename = os.path.join(subfolder, link.get('href'))
        #                     print('Downloading: ', link.get('href'))
        #                     fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
        #                     urllib.request.urlretrieve(fulllink, filename)  # download specified dataset
        #
        #             # Clip and merge files into single time series
        #             files = []
        #             files += [x for x in os.listdir(subfolder) if x.endswith('.nc')]
        #             files.sort()
        #             for index, file in enumerate(tqdm(files)):
        #                 path = os.path.join(subfolder, file)
        #                 ds = xr.open_dataset(path).load()
        #                 ds['lon'] = ds['lon'] - 360  # the original longitude values are greater than 180, convert the values to the range (-180, 180)
        #                 mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
        #                 mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
        #                 cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
        #                 if index == 0:
        #                     fds = cropped_ds
        #                 else:
        #                     fds = xr.merge([fds, cropped_ds])
        #
        #             fds.to_netcdf(os.path.join(mrg_dir, '{}_{}_MergeClip.nc'.format(gcm, ssp)))
        #
        #         else:
        #             # temp_link = parentlink + "{}/{}/".format(met_var, gcm)  # build URL
        #             #
        #             # # get list of all files
        #             # r = requests.get(temp_link)
        #             # data = r.text
        #             # soup = BeautifulSoup(data)
        #             #
        #             # # Download NetCDF data
        #             # for link in soup.find_all('a'):
        #             #     if link.get('href').endswith('.nc') and rcp in link.get('href'):
        #             #         filename = os.path.join(subfolder, link.get('href'))
        #             #         print('Downloading: ', link.get('href'))
        #             #         fulllink = urllib.parse.urljoin(temp_link, link.get('href'))
        #             #         urllib.request.urlretrieve(fulllink, filename)  # download specified dataset
        #
        #             # Clip and merge files into single time series
        #             files = []
        #             files += [x for x in os.listdir(subfolder) if x.endswith('.nc')]
        #             files.sort()
        #             for index, file in enumerate(tqdm(files)):
        #                 path = os.path.join(subfolder, file)
        #                 ds = xr.open_dataset(path).load()
        #                 ds['lon'] = ds['lon'] - 360  #  the original longitude values are greater than 180, convert the values to the range (-180, 180)
        #                 mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
        #                 mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
        #                 cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
        #                 if index == 0:
        #                     fds = cropped_ds
        #                 else:
        #                     fds = xr.merge([fds, cropped_ds])
        #
        #             fds.to_netcdf(os.path.join(mrg_dir, '{}_{}_MergeClip.nc'.format(gcm, ssp)))
