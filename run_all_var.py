import xarray as xr
import os
import pandas as pd
import re
import numpy as np
import datetime
from multiprocessing import Pool
import time

PRESSURE_VARS_all = [
    "SPFH_P0_L100_GLC0", # specific_humidity  # 报错
    "TMP_P0_L100_GLC0", # temperature
    "UGRD_P0_L100_GLC0", # u_component_of_wind
    "VGRD_P0_L100_GLC0", # v_component_of_wind  # 报错
    # "DPT_P0_L100_GLC0",
    # "RH_P0_L100_GLC0",
    "CLWMR_P0_L100_GLC0", # Cloud mixing ratio
    "RWMR_P0_L100_GLC0", # Rain mixing ratio
    "SNMR_P0_L100_GLC0", # Snow mixing ratio
    # "GRLE_P0_L100_GLC0",
    # "CIMIXR_P0_L100_GLC0",
    # "VVEL_P0_L100_GLC0",
    # "ABSV_P0_L100_GLC0",
    "HGT_P0_L100_GLC0", # Geopotential Height (nearest grid point)  # 报错
]

SURFACE_VARS_all = [
    "UGRD_P0_L103_GLC0", # 10m_u_component_of_wind
    "VGRD_P0_L103_GLC0", # 10m_v_component_of_wind
    "TMP_P0_L103_GLC0", # 2m_temperature
    "MSLMA_P0_L101_GLC0", # mean_sea_level_pressure
    "PRES_P0_L1_GLC0",
    "DPT_P0_L103_GLC0", # 报错
    "TCDC_P0_L10_GLC0",
    "VIL_P0_L10_GLC0",  # 报错
    "APCP_P8_L1_GLC0_acc", 
    "PRATE_P0_L1_GLC0",
]


root_dir = "/blob/kmsw0eastau/data/hrrr/grib2/hrrr"
all_date_dir = os.listdir(root_dir)
all_date_dir =  sorted(all_date_dir, key=lambda x: datetime.datetime.strptime(x, '%Y%m%d'))

all_date_dir = all_date_dir[400:500]
for time_index in range(len(all_date_dir)):
    with open(f"./run_day/{all_date_dir[time_index]}.sh", "w") as file:  
        star_index = 0
        for var in PRESSURE_VARS_all+SURFACE_VARS_all:
            # 写入您想要添加到 Shell 脚本中的命令  
            star_index += 1
            if star_index % 4 == 0:
                file.write(f"python /home/msrai4scipde/grib2_zarr/grib2_onevar.py -s {all_date_dir[time_index]} -v {var}\n")  
            else:
                file.write(f"python /home/msrai4scipde/grib2_zarr/grib2_onevar.py -s {all_date_dir[time_index]} -v {var} &\n")  
        print(all_date_dir[time_index], "done.")


