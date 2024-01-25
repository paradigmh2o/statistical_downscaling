# downscale-tools
Statistical downscaling tools and utilities

![GCM_Downscaling](https://github.com/paradigmh2o/statistical_downscaling/blob/4f5aa3820f6a42a04e514c0b1a854531fcddf3f5/GCM_Downscaling.png)

## Utilities
- get_cal_adapt_data.py: pulls netCDF files of LOCA downscaled daily meteorological variables for multiple GCMs/RCPs
  - Note that downloading the non-precip data needed for creating air files is very slow. All raw files for 10 GCMs have been predownloaded to box ("Box\Data Library\Tabular Datasets\CalAdapt_FutureClimate") and can be clipped to a project area.
- cal_adapt_annual_precip.py: processes netCDF daily precip into yearly totals for area(s) of interest
- cal_adapt_daily_precip.py: processes netCDF daily precip for area(s) of interest into tabular time series
