import xarray as xr
import os
import pandas as pd
import re
import numpy as np
import datetime
from multiprocessing import Pool
import time

def rename_coords(
    ds: xr.Dataset, product: str = "prs", leadtime: int = 0, issue_time: str = "20200101T00"
) -> xr.Dataset:
    """
    Rename coordinates and dimensions in an xarray.Dataset for better readability and consistency.

    Args:
        ds: Input xarray.Dataset.
        product: Model product (e.g., 'sfc', 'subh', 'prs').
        leadtime: Forecast lead time in hours.
        issue_time: Issue time of the forecast in the format 'YYYYMMDDTHH'.

    Returns:
        xarray.Dataset with renamed coordinates and dimensions.
    """
    issue_time = pd.to_datetime(issue_time)
    int_leadtime = int(leadtime)
    if leadtime > 0 and product == "subh":
        leadtime_list = [int_leadtime - pd.Timedelta(f"{int(x)}min") for x in [45, 30, 15, 0]]
    else:
        leadtime_list = int_leadtime
    if "forecast_time0" not in ds:
        ds = ds.assign_coords({"lead_time": leadtime_list})
        # ds = ds.expand_dims(dim="lead_time", axis=0)
    else:
        ds = ds.rename({"forecast_time0": "lead_time"})
        ds["lead_time"] = leadtime_list
    ds = ds.rename(
        {
            "xgrid_0": "x",
            "ygrid_0": "y",
        }
    )
    ds = ds.rename({"gridlat_0": "lat", "gridlon_0": "lon"})
    # convert lontitude into 0~360 instead of -180 ~ 180.
    ds["lon"] = (ds["lon"] + 360) % 360
    ds = ds.assign_coords({"time": issue_time})

    return ds

def process_day(root_dir="/blob/kmsw0eastau/data/hrrr/grib2/hrrr", out_root_dir="/blob/kmsw0eastau/data/hrrr/zarr",
                day_name="20190101", issue_times=["00", "01"], leadtimes=["00", "01"], 
                PRESSURE_VARS=["SPFH_P0_L100_GLC0", "TMP_P0_L100_GLC0"], 
                SURFACE_VARS = [ "UGRD_P0_L103_GLC0", "VGRD_P0_L103_GLC0", "TMP_P0_L103_GLC0"], 
                LV_SELECTION={"lv_HTGL1": 10.0}):    
    '''
    Read date from daily grib2 file and save them .
    
    Args:
        input_dir: daily grib2 file directory.
        issue_time: Issue time of the forecast in the format 'YYYYMMDDTHH'.
        leadtime: Forecast lead time in hours.
        PRESSURE_VARS: atomsphare variables.
        SURFACE_VARS: surface variables.
        LV_SELECTION: There are two same variables but on different surface hight, one is 10m and the other is 80m here we choose 10m.
    
    Return:
        xarray.
    '''
    t1_pro = time.time()
    input_dir = os.path.join(root_dir, day_name)
    output_dir = os.path.join(out_root_dir, day_name + ".zarr")
    os.makedirs(os.path.dirname(output_dir), exist_ok=True)
    print(f"begin to read data {day_name}")
    for var_name in SURFACE_VARS+PRESSURE_VARS:
        print(var_name)
        t1_read = time.time()
        this_day_ds_list = []
        for i in issue_times:
            this_hour_ds_list = []
            for j in leadtimes:
                file_name = 'hrrr.t' + i + 'z.wrfprsf' + j + '.grib2'
                print("file_name", file_name)
                ds = xr.open_dataset(os.path.join(input_dir, file_name), engine="pynio")
                ds = rename_coords(ds, product="prs", leadtime=int(j), issue_time=f'{day_name}T{i}')
                if int(j) >= 2 and ("APCP_P8_L1_GLC0_acc" in var_name):
                    if len(ds[[f"APCP_P8_L1_GLC0_acc{int(j)}h"]].coords) != 3:
                        ds = ds[[f"APCP_P8_L1_GLC0_acc{int(j)}h"]]
                    else:
                        ds = ds[[f"APCP_P8_L1_GLC0_acc{int(j)}h"]].sel(LV_SELECTION)
                else:
                    if len(ds[[var_name]].coords) != 3:
                        ds = ds[[var_name]]
                    else:
                        ds = ds[[var_name]].sel(LV_SELECTION)
                this_hour_ds_list.append(ds)
            print("concat leadtime", var_name)
            t1_lead = time.time()
            this_hour_ds = xr.concat(this_hour_ds_list, dim='lead_time')
            del this_hour_ds_list
            t2_lead = time.time()
            print(t2_lead - t1_lead)
            this_day_ds_list.append(this_hour_ds)
            del this_hour_ds
        print("concat time", var_name)
        t1_time = time.time()
        this_day_ds = xr.concat(this_day_ds_list, dim='time')
        del this_day_ds_list
        t2_time = time.time()
        print("saving data", var_name, "time used in concat time", t2_time-t1_time, "day_name", day_name)
        t2_read = time.time()
        this_day_ds.to_zarr(output_dir, mode='a')  
        del this_day_ds
        t3_read = time.time()
        print("read data lead_time:", t2_read-t1_read, "save time", t3_read-t2_read)
    t2_pro = time.time()
    print("day:", day_name, "time per day for reading and saving:", t2_pro-t1_pro)



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


# There are two same variables but on different surface hight, one is 10m and the other is 80m here we choose 10m
LV_SELECTION = {"lv_HTGL1": 10.0}  # select surface 10m

issue_times = ["00", "01", "02", "03", "04", "05", "06", "07", 
              "08", "09", "10", "11", "12", "13", "14", "15",
              "16", "17", "18", "19", "20", "21", "22", "23"]
leadtimes = ["00", "01", "02", "03", "04", "05", "06"]

root_dir = "/blob/kmsw0eastau/data/hrrr/grib2/hrrr"
out_root_dir = "/blob/kmsw0eastau/data/hrrr/zarr"
# test
# root_dir = "/home/msrai4scipde/xuerui_hrrr2zarr/grib2_data"
# out_root_dir = "/home/msrai4scipde/xuerui_hrrr2zarr/zarr_data"

all_date_dir = os.listdir(root_dir)
all_date_dir =  sorted(all_date_dir, key=lambda x: datetime.datetime.strptime(x, '%Y%m%d'))
# all_date_dir = [all_date_dir[0]]
num_proc = 40
    
def main(all_date_dir_):
    if num_proc == 1:
        for day in all_date_dir_:
            process_day(root_dir, out_root_dir, day, issue_times, leadtimes, PRESSURE_VARS, SURFACE_VARS, LV_SELECTION)
    else:
        pool = Pool(num_proc)
        results = []
        for day in all_date_dir_:
            result = pool.apply_async(
                process_day,
                args=(root_dir, out_root_dir, day, issue_times, leadtimes, PRESSURE_VARS, SURFACE_VARS, LV_SELECTION),
            )
            results.append(result)
        pool.close()
        pool.join()

import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--start_date",
        default="20190101",
        type=str,
        help="Start date in YYYY-MM-DD.Includes entire month.",
    )
    parser.add_argument(
        "-v",
        "--var",
        default="UGRD_P0_L103_GLC0",
        type=str,
        help="name of variables include pressure level and surface level var.",
    )
    args = parser.parse_args()
    if args.var in PRESSURE_VARS_all:
        PRESSURE_VARS = [args.var]
        SURFACE_VARS = []
    elif args.var in SURFACE_VARS_all:
        PRESSURE_VARS = []
        SURFACE_VARS = [args.var]
    else:
        print("wrong name of variable")
        assert 1==2
    leadtimes = leadtimes[:4]
    print("issue_times", issue_times, "leadtimes", leadtimes)
    star_date = all_date_dir.index(args.start_date)
    all_date_dir = all_date_dir[star_date:star_date+1]
    print("all_date_dir", all_date_dir, "num_proc", num_proc)
    print("num of PRESSURE_VARS", len(PRESSURE_VARS), "num of SURFACE_VARS", len(SURFACE_VARS))
    t1 = time.time()
    main(all_date_dir_=all_date_dir)
    t2 = time.time()
    print("total time:", t2-t1)
    

    