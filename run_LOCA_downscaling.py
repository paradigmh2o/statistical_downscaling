"""
Benjamin Bowes, Jan. 23, 2024

Script to downscale future climate data
"""

import os
import pandas as pd
import datetime
import itertools
import pickle
from downscale import *

base_dir = r"C:\Users\{}\Box\Projects\City of San Diego\IDEAs\ClimateChange".format(os.getlogin())
fut_in_dir = os.path.join(base_dir, r"CalAdapt\netCDF\Daily\precip\Merge_Clip")
fut_out_dir = os.path.join(base_dir, r"CalAdapt\netCDF\Hourly_Downscaled")
pcarr_dir = os.path.join(base_dir, r"CalAdapt\netCDF\CorrelationZones")
obs_pth = os.path.join(base_dir, r"NLDAS\Merged\SD_NLDAS_Merged.nc4")

# make dirs
for i in [fut_out_dir, pcarr_dir]:
    if not os.path.exists(i):
        os.mkdir(i)

# Loop over files to be downscaled
rcps = ['rcp45', 'rcp85']
# rcps = ['rcp45']
gcms = ["ACCESS1-0", "CCSM4", "CESM1-BGC", "CMCC-CMS", "CNRM-CM5",
        "CanESM2", "GFDL-CM3", "HadGEM2-CC", "HadGEM2-ES", "MIROC5"]  # CCTAG GCM subset
# gcms = ["CNRM-CM5", "CanESM2", "GFDL-CM3", "HadGEM2-CC", "HadGEM2-ES", "MIROC5"]  # CCTAG GCM subset
# gcms = ["HadGEM2-ES"]  # CCTAG GCM subset

# Opening observed data for testing
# obs = xr.open_dataset(r"C:\Users\Ben\Box\Projects\LA County\Climate Change\5_Model\DownScaling_scripts\FromHamided'sComputer\ClimateChange\Downscaling_Handoff_Data\lac_nldas.nc").load()
obs = xr.open_dataset(obs_pth).load()

for gcm, rcp in itertools.product(gcms, rcps):
# for gcm, rcp in zip(['CCSM4', 'CMCC-CMS', 'CESM1-BGC'], ['rcp85', 'rcp45', 'rcp85']):
    # print(gcm, rcp)
    file = "{}_{}_Merged.nc".format(gcm, rcp)
    file_name = file.split('_Merged.nc')[0]
    ds_name = file_name + '_LOCA_hr'
    print(file_name)

    # Opening modeled future data for testing
    # mod_fut_files = glob('/media/eric/T71/data/wmms-cc/meteo/BiasCorrected_CalAdapt/RCP45/*.nc')
    # mod_fut = xr.open_dataset(mod_fut_files[5])
    mod_fut = xr.open_dataset(os.path.join(fut_in_dir, file)).load()
    # print(mod_fut.pr.units)

    # convert modeled values to the same units as the observed data (kg m-2 s-1 to kg m-2)
    # assuming the density of water is 1000 kg m-3, then kg m-2 is equivalent to mm
    mod_fut['pr'] = mod_fut['pr'] * 86400
    # mod_fut['pr'] = mod_fut['pr'].fillna(0)

    # get all lat and lon combinations
    lat_lons = []
    for lat in mod_fut['lat'].values:
        for lon in mod_fut['lon'].values:
            # print(lat, lon)
            lat_lons.append("{}_{}".format(lat, lon))

    # Formatting the lat/lon coordinates for modeled future data, extracting lat/lon coordinates into np array
    latvals = mod_fut.lat[(mod_fut.lat <= obs.lat.max()) & (mod_fut.lat >= obs.lat.min())].values
    lonvals = mod_fut.lon[(mod_fut.lon <= obs.lon.max()) & (mod_fut.lon >= obs.lon.min())].values

    # Observed and modeled future data may be on different grids.
    # This interpolates the observed data to the same grid as the modeled future data.
    obsinterp = obs.interp(lat=latvals, lon=lonvals, kwargs={'bounds_error': False, 'fill_value': np.nan})

    # save obsinterp for QA
    # obsinterp.to_netcdf(r"C:\Users\Ben\Box\Projects\City of San Diego\IDEAs\ClimateChange\NLDAS\Merged\NLDAS_LOCA_interp2.nc")

    # Identifying which grid cells are correlated with each other
    pcarr = identify_correlation_zones(obsinterp, precip_name='APCP')

    with open(os.path.join(pcarr_dir, '{}_pcarrs.pk'.format(file_name)), 'wb') as outfile:
        pickle.dump(pcarr, outfile)

    with open(os.path.join(pcarr_dir, '{}_pcarrs.pk'.format(file_name)), 'rb') as infile:
        pcarr = pickle.load(infile)

    # Preprocess and initialize
    mod_fut_vals, mod_fut_timestamps, mod_fut_seasons, mod_fut_doy, obs_vals, obs_daily_vals, obs_daily_timestamps, obs_daily_doy, obs_day = preprocess(obsinterp, mod_fut, precip_name='APCP')

    # Downscale
    dsfut = loca(mod_fut_vals, mod_fut_timestamps, mod_fut_seasons, mod_fut_doy, obs_vals, obs_daily_vals,
                 obs_daily_timestamps, obs_daily_doy, obs_day, pcarr)

    # create time stamp for downscaled values
    fut_start = pd.to_datetime(mod_fut.time[0].values) - datetime.timedelta(hours=12)
    fut_end = pd.to_datetime(mod_fut.time[-1].values) + datetime.timedelta(hours=11)
    dt_idx = pd.date_range(fut_start, fut_end, freq='H')

    dsfut_df = pd.DataFrame(dsfut.reshape(dsfut.shape[0], dsfut.shape[1] * dsfut.shape[2]), index=dt_idx)
    dsfut_df = dsfut_df / 25.4  # convert mm to inches
    dsfut_df.columns = lat_lons

    # check for empty cells
    for lat_lon in lat_lons:
        if dsfut_df[lat_lon].sum() == 0:
            print(lat_lon, dsfut_df[lat_lon].sum())

    # create downscaled netCDF
    dsfut_nc = xr.DataArray(dsfut,
                            coords={'time': np.asarray(dt_idx), 'lat': mod_fut.lat.values, 'lon': mod_fut.lon.values},
                            dims=["time", "lat", "lon"],
                            attrs=dict(description="pre", units="kg m-2"))

    # Save downscaled values
    dsfut_nc.to_netcdf(os.path.join(fut_out_dir, '{}.nc4'.format(ds_name)))
    dsfut_df.to_csv(os.path.join(fut_out_dir, '{}.csv'.format(ds_name)))
