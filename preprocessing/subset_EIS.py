import xarray as xr
import numpy as np

for year in ['2014','2015','2016','2021']:
    print(year)
    ds = xr.open_dataset(f'/gws/nopw/j04/eo_shared_data_vol1/satellite/modistracks/reanalysisx/globEIS_{year}')

    # Adjust longitude values from 0-360 to -180-180
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))

    # Sort the data by the new longitude values
    ds = ds.sortby('longitude').sortby('latitude')

    # Subset the data to the desired longitude and latitude range
    ds_subset = ds.sel(longitude=slice(-90, 10), latitude=slice(-50, 50))
    
    ds_subset.to_netcdf(f'/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/reanalysis_data/ERA5/regression_vars/EIS_{year}.nc')
    
    
    