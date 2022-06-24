# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:27:42 2022

@author: Zhao
"""

from bs4 import BeautifulSoup
import urllib
import requests
import os
import glob
import xarray as xr
from tqdm import tqdm

"""User Input"""
parentlink = "http://albers.cnr.berkeley.edu/data/scripps/loca/met/ACCESS1-0/rcp45/pr/" #weblink for the data
folder = os.path.join(r'C:\Projects\SpringValley\NC_Data', parentlink.split('/')[-4]+'_'+parentlink.split('/')[-3]) #folder directory for store NetCDF data
if not os.path.isdir(folder):
    os.makedirs(folder)

#lat/lon range of the area of interest    
min_lon = -117.6
min_lat = 32.53
max_lon = -116.01
max_lat = 33.58 

"""Download NetCDF data"""
r  = requests.get(parentlink)
data = r.text
soup = BeautifulSoup(data)
    
for link in soup.find_all('a'):
    if link.get('href').endswith('.nc'):
        filename = os.path.join(folder,link.get('href'))
        print ('Downloading: '+filename)
        fulllink = urllib.parse.urljoin(parentlink, link.get('href'))
        urllib.request.urlretrieve(fulllink, filename)

"""Clip and merge file"""
files = []
files += [x for x in os.listdir(folder) if x.endswith('.nc')]
files.sort()
for index, file in enumerate(tqdm(files)):
    path = os.path.join(folder, file)
    ds = xr.open_dataset(path).load()
    ds['lon'] = ds['lon'] - 360 #the original longitude values are greater than 180, conert the values to the range (-180, 180)
    mask_lon = (ds.lon >= min_lon) & (ds.lon <= max_lon)
    mask_lat = (ds.lat >= min_lat) & (ds.lat <= max_lat)
    cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
    if index == 0:
        fds = cropped_ds
    else:
        fds = xr.merge([fds, cropped_ds])
#ds = xarray.open_mfdataset(r'C:\Projects\SpringValley\NC_Data\ACCESS1-0_rcp45\*.nc')
ds.to_netcdf(os.path.join(folder, parentlink.split('/')[-4]+'_'+parentlink.split('/')[-3]+'_Merge.nc'))