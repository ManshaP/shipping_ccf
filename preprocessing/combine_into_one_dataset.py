import xarray as xr
import glob
import pandas as pd
import os
from collections import defaultdict
import re

cor_files = glob.glob('/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/reanalysis_data/ERA5/regression_vars/coarsened/*.nc')

# List of file paths
file_paths = cor_files

# Function to extract the full variable name (from filename) and (optionally) pressure level
def extract_var_and_level(filename):
    match = re.match(r'(.+?)_(\d{4})(?:_(\d+hPa))?\.nc', filename)
    if not match:
        raise ValueError(f"Filename {filename} doesn't match the expected pattern")
    var = match.group(1)
    year = match.group(2)
    level = match.group(3)
    return var, level, year

# Function to standardize coordinate names
def standardize_coords(ds):
    coord_map = {
        'la': 'latitude',
        'lo': 'longitude',
        'date': 'time'
    }
    ds = ds.rename({old: new for old, new in coord_map.items() if old in ds.coords})
    return ds

# Organize file paths by variable and level
files_by_var_and_level = defaultdict(list)

for fp in file_paths:
    var, level, year = extract_var_and_level(os.path.basename(fp))
    key = (var, level)
    files_by_var_and_level[key].append(fp)

# Create a dictionary to store the datasets
datasets = []

for (var, level), files in files_by_var_and_level.items():
    # Sort the files to ensure they are in chronological order
    files.sort()
    # Open and concatenate files for the current variable and level
    ds = xr.open_mfdataset(files, combine='by_coords')
    if var=='EIS': ds=ds.drop_vars('level')
    # Standardize coordinate names
    ds = standardize_coords(ds)
    # Rename the variables to match the filename
    for var_name in ds.data_vars:
        new_var_name = f"{var}_{level}" if level else var
        ds = ds.rename({var_name: new_var_name})
    # Append the dataset to the list
    datasets.append(ds)

# Merge all datasets into a single dataset
combined_ds = xr.merge(datasets)

# Display the combined dataset
print(combined_ds)

# Save the combined dataset to a NetCDF file if needed
output_file = '/gws/nopw/j04/eo_shared_data_vol2/scratch/pete_nut/shipping_ccf_combined_dataset.nc'
combined_ds.to_netcdf(output_file)

