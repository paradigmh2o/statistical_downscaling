"""
Benjamin Bowes, 1-12-2024

Utility script to flatten a netCDF file into a csv with datetime index and Lat-Long columns
"""

import os
import pandas as pd
import datetime
import numpy as np
import itertools
import xarray
import xarray as xr
import pyet

base_dir = r"C:\Users\{}\Box\Projects\City of San Diego\IDEAs\ClimateChange\CalAdapt\netCDF\Daily".format(os.getlogin())
out_dir = r"C:\Users\{}\Box\Projects\City of San Diego\IDEAs\ClimateChange\LSPC_Weather".format(os.getlogin())

gcms = ["HadGEM2-ES", "CNRM-CM5", "CanESM2", "MIROC5"]   # 4 priority GCMs
rcps = ['historical', 'rcp45', 'rcp85']
met_vars = ['solards', 'wspeed', 'tasmin', 'tasmax', 'rel_humid']

elev_df = pd.read_csv(os.path.join(base_dir, "LOCA_Grid_Elevation.csv"))
air_template = os.path.join(base_dir, 'temp.air')

# Loop over GCMs and RCPs
for gcm, rcp in itertools.product(gcms, rcps):
    print(gcm, rcp)
    out_path = os.path.join(out_dir, gcm, rcp)

    # read in appropriate variables and flatten netCDF to df
    tmin = xr.open_dataset(os.path.join(base_dir, 'tasmin', "Merge_Clip", "{}_{}_MergeClip.nc".format(gcm, rcp))).load()
    tmin['tasmin'] = tmin['tasmin'] - 273.15  # convert K to C

    # get all lat and lon combinations
    lat_lons = []
    for i in tmin['lat'].values:
        for j in tmin['lon'].values:
            # print(lat, lon)
            lat_lons.append("{}_{}".format(i, j))

    tmin_df = pd.DataFrame(tmin.tasmin.values.reshape(tmin.tasmin.shape[0], tmin.tasmin.shape[1] * tmin.tasmin.shape[2]),
                           index=tmin.time.values)

    tmax = xr.open_dataset(os.path.join(base_dir, 'tasmax', "Merge_Clip", "{}_{}_MergeClip.nc".format(gcm, rcp))).load()
    tmax['tasmax'] = tmax['tasmax'] - 273.15  # convert K to C
    tmax_df = pd.DataFrame(tmax.tasmax.values.reshape(tmax.tasmax.shape[0], tmax.tasmax.shape[1] * tmax.tasmax.shape[2]),
                           index=tmax.time.values)

    tmean = (tmax['tasmax'] + tmin['tasmin']) / 2  # calculate average temp
    tmean_df = pd.DataFrame(tmean.values.reshape(tmean.shape[0], tmean.shape[1] * tmean.shape[2]),
                            index=tmean.time.values)

    srad = xr.open_dataset(os.path.join(base_dir, 'solards', "Merge_Clip",
                                        "{}_{}_MergeClip.nc".format(gcm, rcp))).load()
    srad['rsds'] = srad['rsds'] / (1000000 / 86400)  # convert W m-2 to MJ m-2: 1W = 1J/s
    srad_df = pd.DataFrame(srad.rsds.values.reshape(srad.rsds.shape[0], srad.rsds.shape[1] * srad.rsds.shape[2]),
                           index=srad.time.values)

    windsp = xr.open_dataset(os.path.join(base_dir, 'wspeed', "Merge_Clip",
                                          "{}_{}_MergeClip.nc".format(gcm, rcp))).load()
    windsp_df = pd.DataFrame(windsp.magnitude.values.reshape(windsp.magnitude.shape[0],
                                                             windsp.magnitude.shape[1] * windsp.magnitude.shape[2]),
                             index=windsp.time.values)

    rhmin = xr.open_dataset(os.path.join(base_dir, 'rel_humid', "Merge_Clip",
                                         "{}_{}_min_MergeClip.nc".format(gcm, rcp))).load()
    rhmin_df = pd.DataFrame(rhmin.rel_humid_min.values.reshape(rhmin.rel_humid_min.shape[0],
                                                               rhmin.rel_humid_min.shape[1] * rhmin.rel_humid_min.shape[2]),
                            index=rhmin.time.values)

    rhmax = xr.open_dataset(os.path.join(base_dir, 'rel_humid', "Merge_Clip",
                                         "{}_{}_max_MergeClip.nc".format(gcm, rcp))).load()
    rhmax_df = pd.DataFrame(rhmax.rel_humid_max.values.reshape(rhmax.rel_humid_max.shape[0],
                                                               rhmax.rel_humid_max.shape[1] * rhmax.rel_humid_max.shape[2]),
                            index=rhmax.time.values)

    for met_df in [tmin_df, tmax_df, tmean_df, srad_df, windsp_df, rhmin_df, rhmax_df]:
        # print(met_df.index[0], met_df.index[-1])
        met_df.columns = lat_lons

    # loop over LOCA grid cells and calculate PM PET
    for lat_lon in lat_lons:
        if lat_lon == '32.59375_-117.21875':
            continue
        print(lat_lon)
        lat = float(lat_lon.split('_')[0])
        lat_rad = lat * np.pi / 180  # radians
        lon = float(lat_lon.split('_')[1])

        # mean grid cell elevation
        elev = elev_df['elev_mean'].loc[(elev_df['lat'] == lat) & (elev_df['lon'] == lon)].iloc[0]

        grid_id = elev_df['GRIDID'].loc[(elev_df['lat'] == lat) & (elev_df['lon'] == lon)].iloc[0]

        # Calculate PET in mm/day
        pet_pm = pyet.pm(tmean_df[lat_lon], windsp_df[lat_lon], rn=srad_df[lat_lon], elevation=elev, lat=lat_rad,
                         tmax=tmax_df[lat_lon], tmin=tmin_df[lat_lon],
                         rhmax=rhmax_df[lat_lon], rhmin=rhmin_df[lat_lon])
        pet_pm = pet_pm.to_frame()
        pet_pm['Penman_Monteith'] = pet_pm['Penman_Monteith'] / 24 / 25.4  # mm/day to in/hr
        pet_hr = pet_pm.resample('H').ffill()  # TODO distribute ET over daylight hours only
        pet_hr.dropna(axis=0, inplace=True)

        # # resample to hourly
        # fut_start = pet_pm.index[0] - datetime.timedelta(hours=12)
        # fut_end = pet_pm.index[-1] + datetime.timedelta(hours=11)
        # dt_idx = pd.DataFrame(index=pd.date_range(fut_start, fut_end, freq='H'))
        #
        # pet_pm.set_index(pet_pm.index - datetime.timedelta(hours=12), inplace=True)
        # pet_hr = pet_pm.resample('H').ffill()
        # #
        # # pet_hr = pd.DataFrame(index=dt_idx)
        # # pet_hr['PET'] =
        # pet_hr2 = pd.concat([pet_hr, dt_idx], axis=1)
        # # pet_hr = pet_pm.set_index(dt_idx)

        # write to air file
        with open(air_template, "r") as f:
            header_row = f.read().replace('x58y75', str(grid_id))
        np.savetxt(os.path.join(out_path, '{}_{}_{}_LOCA.air'.format(grid_id, gcm, rcp)),
                   np.asarray([header_row]), fmt='%s')

        pet_hr['STA'] = grid_id
        pet_hr['DTTM'] = pet_hr.index
        pet_hr['Year'] = pet_hr['DTTM'].dt.year
        pet_hr['Month'] = pet_hr['DTTM'].dt.month
        pet_hr['Day'] = pet_hr['DTTM'].dt.day
        pet_hr['Hour'] = pet_hr['DTTM'].dt.hour
        pet_hr['Minute'] = pet_hr['DTTM'].dt.minute
        pet_hr = pet_hr[['STA', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Penman_Monteith']]
        pet_hr.to_csv(os.path.join(out_path, '{}_{}_{}_LOCA.air'.format(grid_id, gcm, rcp)),
                      mode='a', sep=' ', header=False, index=False)
