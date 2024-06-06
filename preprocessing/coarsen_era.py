import glob
import numpy as np
import pandas as pd
import datetime
import xarray as xr
from multiprocessing import Pool
from os import path

# import dask.dataframe as dd
# from dask.distributed import Client
# import pickle

def myround(x, base=5):
    return base * round(x/base)
def date(x):
    return x.date()

def coarsen_year(year): 
    era_filepath = '/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/reanalysis_data/ERA5/regression_vars/T_adv'
    savepath = '/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/reanalysis_data/ERA5/regression_vars/'
    dtypes = {'latitude':np.float32, 'longitude':np.float32, 'r_eff':np.float32, 'npoints':np.float32, 'LWP':np.float32, 'COT':np.float32,
                        'CTH':np.float32, 'signal':np.float32,  'particle':np.int32, 'latitude_1':np.float32, 'longitude_1':np.float32,
                        'r_eff_1':np.float32, 'LWP_1':np.float32, 'COT_1':np.float32, 'CTH_1':np.float32, 'npoints_1':np.float32, 'latitude_3':np.float32,
                        'longitude_3':np.float32, 'r_eff_3':np.float32, 'LWP_3':np.float32, 'COT_3':np.float32, 'CTH_3':np.float32, 'npoints_3':np.float32,
                        'terra':np.bool_, 'ocean':np.bool_,'Nd':np.float32, 'Nd_1':np.float32 , 'Nd_3':np.float32,  'hours_diff':np.int32, 'chil':np.bool_, 'azor':np.bool_, 'cver':np.bool_, 'ango':np.bool_}
    colu = ['longitude',  'latitude', 'ocean','overpass' ]


    print(year)
    files = glob.glob('/gws/nopw/j04/eo_shared_data_vol1/satellite/modistracks/{}/inc_EIS_par_??'.format(year))
    h5 = pd.read_parquet(files,
                    columns=colu,
                    # dtype=dtypes,
                    # blocksize=200e6
                        )
    metpaths = glob.glob(era_filepath + '*{}*.nc'.format(year))
    metpaths.reverse()
    for ran_path in metpaths:
        var = ran_path.split('/')[-1]
        print(var)
        if path.isfile(era_filepath +'/coarsened/'+ var):
            print('already exists')
            continue
        era = xr.open_dataset(ran_path)
        era_int = era.interp(time=xr.DataArray(h5.index, dims='obs'), longitude=xr.DataArray(h5.longitude, dims='obs'), latitude=xr.DataArray(h5.latitude, dims='obs'))
        era_int = era_int.to_dataframe()
        era_int['latitude'] = era_int.latitude.apply(myround, )
        era_int['longitude'] = era_int.longitude.apply(myround, )
        era_int['time'] = era_int.time.apply(date).astype(np.datetime64)
        erpr = era_int.groupby([era_int['latitude'], era_int['longitude'], era_int['time']]).mean().to_xarray()
        erpr.to_netcdf(savepath +'/coarsened/'+ var)
    return 0
    
# with Pool(4) as p:
    # p.map(coarsen_year, ['2015','2016','2021',]) #'2014','2017','2018','2019'
coarsen_year('2016')