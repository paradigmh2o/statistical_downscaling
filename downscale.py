import xarray as xr 
import numpy as np
import sys
from scipy.stats import pearsonr
from tqdm import tqdm 
from numba import njit, objmode


def identify_correlation_zones(obs, precip_name='pr'):
    # read the observed precip values into a numpy array
    obs_vals = obs[precip_name].values
    # read the observed precip timestamps into a numpy array
    obs_timestamps = obs.time.values

    # resample observed precip to daily timestep
    obs_daily = obs.resample(time='1D').sum()
    # read daily observed precip into a numpy array
    obs_daily_vals = obs_daily.values()
    # read daily observed precip timestamps into a numpy array
    obs_daily_timestamps = obs_daily.time.values

    obs_len, obs_maxlat, obs_maxlon = obs_vals.shape

    min_len = np.inf

    # iterating through all four seasons
    for i,ssn in enumerate(('DJF','MAM','JJA','SON')):
        # Identify which indices in the array fall within the given season
        obs_season_inds = (obs.time.dt.season==ssn).values
        # Identify the timeseries length for each season, so we can preallocate an array with the right dimensions
        season_ts_len = np.sum(obs_season_inds)
        if season_ts_len < min_len:
            min_len = season_ts_len
            
    # Preallocate array 
    obs_seasons = np.zeros([4, min_len, obs_maxlat, obs_maxlon])
    obs_seasons[:] = np.nan
    
    # Splitting timeseries into the four seasons, putting each season into a different part of the array
    for i, ssn in enumerate(('DJF','MAM','JJA','SON')):
        obs_season_inds = (obs.time.dt.season==ssn).values
        counter = 0
        for j,ind in enumerate(obs_season_inds):
            if ind:
                counter += 1
            if counter > min_len:
                obs_season_inds[j] = False
        obs_seasons[i,:,:,:] = obs_vals[obs_season_inds,:,:]

    obs_lons = np.arange(obs_maxlon)
    obs_lats = np.arange(obs_maxlat)
    
    # Preallocate array to store correlation
    all_pcarrs = np.zeros([4, obs_maxlat, obs_maxlon, obs_maxlat, obs_maxlon])
    all_pcarrs[:] = np.nan
    
    # For reach season, check which cells are correlated to each other
    tlen = len(obs_lats)*len(obs_lons)*4
    with tqdm(total=tlen) as pbar:
        for i in range(4):
            for obs_lat in obs_lats:
                for obs_lon in obs_lons:
                    obs_ts = obs_seasons[i,:,obs_lat,obs_lon]
                    if ~np.isnan(obs_ts[25]):
                        for comp_lat in obs_lats:
                            for comp_lon in obs_lons:
                                comp_ts = obs_seasons[i,:,comp_lat,comp_lon]
                                if ~np.isnan(comp_ts[25]):
                                    pc,pval = pearsonr(obs_ts,comp_ts)
                                    all_pcarrs[i,obs_lat,obs_lon,comp_lat,comp_lon] = pc
                    pbar.update(1)
    return all_pcarrs


def dt2cal(dt):

    # allocate output 
    out = np.empty(dt.shape + (3,), dtype="u4")
    # decompose calendar floors
    Y, M, D, h, m, s = [dt.astype(f"M8[{x}]") for x in "YMDhms"]
    
    return Y,M,D


# extracting all the necessary numpy arrays and datetimes from xarray objects
def preprocess(obs,mod_fut, precip_name='pr',):
    obs = obs.interp(coords={'lon':mod_fut.lon.values, 'lat':mod_fut.lat.values},method='linear')
    # obs_vals = obs.pr.values
    obs_vals = obs[precip_name].values
    obs_timestamps = obs.time.values

    obs_year,obs_month,obs_day = dt2cal(obs_timestamps)
    obs_daily = obs.resample(time='1D').sum()
    # obs_daily_vals = obs_daily.pr.values
    obs_daily_vals = obs_daily[precip_name].values
    obs_daily_timestamps = obs_daily.time.values
    obs_daily_doy = obs_daily.time.dt.dayofyear.values

    mod_fut_vals = mod_fut.pr.values
    mod_fut_timestamps = mod_fut.time.values
    mod_fut_doy = mod_fut.time.dt.dayofyear.values
    mod_fut_seasons = mod_fut.time.dt.season.values
    
    return(mod_fut_vals,mod_fut_timestamps,mod_fut_seasons,mod_fut_doy,obs_vals,obs_daily_vals,obs_daily_timestamps,obs_daily_doy,obs_day)


@njit
def loca(mod_fut_vals,mod_fut_timestamps,mod_fut_seasons,mod_fut_doy,obs_vals,obs_daily_vals,obs_daily_timestamps,obs_daily_doy,obs_day,pcarr):

    # Get the inputs into the correct data type, because numba is very sensitive about data types
    mod_fut_vals = mod_fut_vals.astype(np.float64)
    obs_daily_vals = obs_daily_vals.astype(np.float64)

    # Getting maximum indices for latitude and longitude
    garb,coarse_latmax,coarse_lonmax = mod_fut_vals.shape
 
    downscaled_fut = np.zeros((len(mod_fut_timestamps)*24, coarse_latmax, coarse_lonmax),dtype=np.float64)
    downscaled_fut[:] = np.nan

    currentcell = 1
    coarse_lats = np.arange(coarse_latmax)
    coarse_lons = np.arange(coarse_lonmax)

    numcells = len(coarse_lats)*len(coarse_lons)

    for lat in coarse_lats:
        for lon in coarse_lons:
            currentcell += 1
            if (~np.isnan(mod_fut_vals[25,lat,lon])) & (np.sum(np.isfinite(pcarr[1,lat,lon,:,:])) > 0):

                mod_fut_len = len(mod_fut_timestamps)
                mod_fut_ts_inds = np.arange(mod_fut_len)
                for t in mod_fut_ts_inds:

                    # for the given time step, determine the season
                    season = mod_fut_seasons[t]
                    if season == 'DJF':
                        s = 0
                    elif season == 'MAM':
                        s = 1
                    elif season == 'JJA':
                        s = 2
                    elif season == 'SON':
                        s = 3
                    # for the given time step and downscale location (DL) lat/lon, find which grid cells are correlated
                    mask = pcarr[s,lat,lon,:,:]
                    masklats,masklons = np.where(mask>.98)
                    masklats = masklats
                    masklons = masklons

                    #mask_inds = np.arange(len(masklats))


                    # find the modeled future depths at that timestep and DL lat/lon
                    if t == len(mod_fut_timestamps) - 1:
                        depths = mod_fut_vals[t-1:t+1,lat,lon]
                        depths = np.append(depths,np.float64(0))
                    elif t == 0:
                        depths = mod_fut_vals[0:3,lat,lon]
                    else:
                        depths = mod_fut_vals[t-1:t+2,lat,lon]


                    # if the modeled future depth for the given timestep/DL location is nonzero:
                    if depths[1] > 0:
                        # find the day of year for that timestep
                        fut_doy = mod_fut_doy[t]
                        # find the indices of the observed days that are within 30 days of the timestep's DOY.
                        if fut_doy < 30:
                            diff = 30-fut_doy
                            anstart = 365-diff
                            anend = fut_doy + 30
                            all_analog_day_inds = (obs_daily_doy > anstart) | (obs_daily_doy < anend)
                        elif fut_doy > 335:
                            diff = 365 - fut_doy
                            anend = 30-diff
                            anstart = fut_doy - 30
                            all_analog_day_inds = (obs_daily_doy > anstart) | (obs_daily_doy < anend)
                        else:
                            anstart = fut_doy - 30
                            anend = fut_doy + 30
                            all_analog_day_inds = (obs_daily_doy > anstart) & (obs_daily_doy < anend)

                        # from the indices determined above, find the actual timestamps for the observed days within 30 days of the timestep
                        all_analog_day_timestamps = obs_daily_timestamps[all_analog_day_inds]

                        # find the modeled future depths for the given timestep, at ALL CORRELATED LOCATIONS
                        if t == len(mod_fut_timestamps) - 1:
                            comp_fut_depths = np.zeros((3,coarse_latmax,coarse_lonmax))
                            comp_fut_depths[:] = np.nan
                            comp_fut_depths[0:2,:,:] = mod_fut_vals[t-1:t+1,:,:]
                            comp_fut_depths[2,:,:] = 0
                        elif t == 0:
                            comp_fut_depths = mod_fut_vals[0:3,:,:]
                        else:
                            comp_fut_depths = mod_fut_vals[t-1:t+2,:,:]
                        
                        # find the observed depths using the indices determined above, at ALL LOCATIONS
                        comp_analog_depths = obs_daily_vals[all_analog_day_inds,:,:]

                        # find out how many analog days there are to test ()
                        num_analog_days = len(comp_analog_depths)


                        # this variable stores the "rmse to beat"
                        best_rmse=np.inf

                        # iterate through every analog day that we found above
                        for i,analog_day in enumerate(range(1,num_analog_days-1)):
                            # this will store the rmse for ONE DAY, across ALL CORRELATED LOCATIONS
                            tot_rmse = 0
                            # iterate through EVERY CORRELATED LOCATION

                            for m,masklat in enumerate(masklats):
                                masklon = masklons[m]

                                # extract the observed depths for ONE OF THE CORRELATED LOCATIONS
                                analog_samp = comp_analog_depths[analog_day-1:analog_day+2,masklat,masklon]

                                fut_depths = comp_fut_depths[:,masklat,masklon]
                                # calculate rmse for ONE DAY, and for ONE CORRELATED LOCATION
                                diffs = analog_samp - fut_depths
                                diffs[0] = diffs[0]/4
                                diffs[2] = diffs[2]/4
                                samp_rmse = np.sqrt(np.sum(diffs**2)/len(masklats))
                                # add the rmse for THIS LOCATION to the rmse for ALL LOCATIONS
                                tot_rmse += samp_rmse

                            # comparing rmse for ALL LOCATIONS against best rmse. if it's better, reset best rmse and store this timestamp
                            if (tot_rmse < best_rmse) & (comp_analog_depths[analog_day,lat,lon]>0):
                                best_rmse = tot_rmse
                                best_timestamp = all_analog_day_timestamps[i+1]

                        if best_rmse!=np.inf:
                            obs_match_inds = np.where(obs_day == best_timestamp)[0]

                            analog_ts = obs_vals[obs_match_inds,lat,lon]

                            mod_depth = depths[1]
                            analog_depth = np.sum(analog_ts)
                            if analog_depth == 0:
                                print(lat)
                                print(lon)
                                print('-------------------')
                                print('HOURLY ANALOG DEPTH')
                                print(analog_depth)
                                print('-------------------')
                                print('DAILY ANALOG DEPTH')
                                print(analog_samp[1])
                                print('-------------------')
                                print('MODEL ANALOG DEPTH')
                                print(mod_depth)
                                print('-------------------')



                            scaleratio = mod_depth/analog_depth
                            analog_ts = analog_ts*scaleratio
                        else:  # BB added
                            analog_ts = np.zeros(24)  # BB added
                    else:
                        analog_ts = np.zeros(24)

                    ds_start = t*24
                    ds_end = ds_start+24
                    downscaled_fut[ds_start:ds_end,lat,lon] = analog_ts
            with objmode():
                sys.stdout.write('Cells: {}/{} \r'.format(currentcell,numcells))
                sys.stdout.flush()
    return(downscaled_fut)

