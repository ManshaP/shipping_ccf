import xarray as xr
import pandas as pd
import glob
path='/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/reanalysis_data/ERA5/regression_vars/coarsened/'

year=2014
files=glob.glob(path+f'n_*{year}.nc')
for f in files: 
    ds = xr.open_mfdataset(f)
    # Convert the 'date' coordinate to a pandas DatetimeIndex
    dates = pd.to_datetime(ds['date'].values)

    # Change the year to the desired new year, e.g., 2024
    new_dates = dates.map(lambda date: date.replace(year=year))

    # Update the 'date' coordinate in the DataArray
    ds['date'] = new_dates
    ds.to_netcdf(f[:-7]+f'corr_{year}.nc')
    
for year in [2015, 2016, 2017]:
    files=glob.glob(path+f'n_*{year}.nc')
    for f in files: 
        ds = xr.open_mfdataset(f)
        ds.to_netcdf(f[:-7]+f'corr_{year}.nc')

year=2018
files=glob.glob(path+f'n_*{year}.nc')
for f in files: 
    # continue
    ds = xr.open_mfdataset(f)
    ds = ds.isel(date=slice(1,None))
    ds.to_netcdf(f[:-7]+f'corr_{year}.nc')

    year=2019
files=glob.glob(path+f'n_*{year}.nc')
for f in files: 
    ds = xr.open_mfdataset(f)
    ds = ds.isel(date=slice(1,None))
    ds.to_netcdf(f[:-7]+f'corr_{year}.nc')

year=2021
files=glob.glob(path+f'n_*{year}.nc')
for f in files: 
    ds = xr.open_mfdataset(f)
    ds.isel(date=slice(2,None))
    dates = pd.to_datetime(ds['date'].values)

    # Identify the dates that are not 29 February
    non_leap_dates = dates[(dates.month != 2) | (dates.day != 29)]

    # Filter the DataArray to exclude 29 February
    ds = ds.sel(date=non_leap_dates)

    # Change the year to the desired new year, e.g., 2024
    new_dates = non_leap_dates.map(lambda date: date.replace(year=year))

    # Update the 'date' coordinate in the DataArray
    ds['date'] = new_dates
    ds.to_netcdf(f[:-7]+f'corr_{year}.nc')