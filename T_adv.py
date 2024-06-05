import glob
import numpy as np
import pandas as pd
import datetime
import xarray as xr
import dask
dask.config.set({'array.slicing.split_large_chunks': True})
years= [   
            '2021',
            ]
for year in years:
    print(year)
    era_filepath = '/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/reanalysis_data/ERA5/regression_vars/'
    u10 = xr.open_dataset(era_filepath + '10m_u_component_of_wind_'+ year+'.nc', chunks={'latitude':20, 'longitude':20, 'time':5})
    v10 = xr.open_dataset(era_filepath + '10m_v_component_of_wind_'+ year+'.nc', chunks={'latitude':20, 'longitude':20, 'time':5})
    sst = xr.open_dataset(era_filepath + 'sea_surface_temperature_'+ year+'.nc', chunks={'latitude':20, 'longitude':20, 'time':5})

    u10 = u10.coarsen({'latitude':10,'longitude':10},boundary='pad').mean()
    v10 = v10.coarsen({'latitude':10,'longitude':10},boundary='pad').mean()
    sst = sst.coarsen({'latitude':10,'longitude':10},boundary='pad').mean()

    u10=u10.chunk(chunks='auto')
    v10=v10.chunk(chunks='auto')
    sst=sst.chunk(chunks='auto')

    sst_lo = sst.differentiate('longitude')
    sst_la = sst.differentiate('latitude')

    Re= 6371000
    colats = np.cos(sst_lo.latitude/180*np.pi)

    Tadv =  - 1/(Re*colats) * sst_lo.sst * u10.u10
    Tadv -= 1/Re * sst_la.sst * v10.v10

    Tadv.to_netcdf(era_filepath+'T_adv'+year+'.nc')
