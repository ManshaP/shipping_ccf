import glob
import numpy as np
import sys
import pandas as pd
import xarray as xr
# from multiprocessing import Pool
from os import path
# import dask.dataframe as dd
# from dask.distributed import Client



def myround(x, base=5):
    return base * round(x/base)

def albedo(COT):
    a=COT/((4/3 /0.15) + COT)
    return a

# files_n = glob.glob('/gws/nopw/j04/eo_shared_data_vol1/satellite/modistracks/nnull/20??/null_??_par')
year=sys.argv[1]
nullarg=sys.argv[2]
null = (nullarg=='null')
if null: 
    files = glob.glob('/gws/nopw/j04/eo_shared_data_vol1/satellite/modistracks/nnull/20??/null_??_par')
    savepath = '/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/reanalysis_data/ERA5/regression_vars/coarsened/n_'

else:
    files = glob.glob(f'/gws/nopw/j04/eo_shared_data_vol1/satellite/modistracks/{year}/inc_EIS_par_??')
    savepath = '/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/reanalysis_data/ERA5/regression_vars/coarsened/'

for var in ['r_eff', 'Nd', 'COT', 'LWP', 'npoints', 'signal']:
    print(f"working on {var}")

    for var_suffix in ['', '_1', '_3']:
        var_name = var + var_suffix
        print(var_name)

        if path.isfile(savepath+var_name+'_'+year+'.nc'):
            print('This file already exists')
            continue
        if var == 'signal' and var_suffix != '':
            continue

        colu = [var+var_suffix, 'ocean', 'longitude', 'latitude', 'overpass']
        h5 = pd.read_parquet(files,
                                columns=colu,)
                                # dtype=dtypes,
                                # blocksize=200e6)
        h5['la'] = h5.latitude.apply(myround, )
        h5['lo'] = h5.longitude.apply(myround, )
        h5['date'] = h5.overpass.dt.date

        if var == 'COT':
            h5['albedo'] = h5[var+var_suffix].apply(albedo)

        filtered_data = h5[h5.ocean]
        diff_mean = filtered_data.groupby(['la', 'lo', 'date'])[var_name].mean().to_xarray()
        diff_mean['date'] = pd.to_datetime(diff_mean.date)
        diff_mean.to_netcdf(savepath+var_name+'_'+year+'.nc')
        
        if var_name=='COT': 
            diff_mean = filtered_data.groupby(['la', 'lo', 'date'])[var_name].count().to_xarray()
            diff_mean['date'] = pd.to_datetime(diff_mean.date)
            diff_mean.to_netcdf(savepath+'count_'+var_name+'_'+year+'.nc')
        