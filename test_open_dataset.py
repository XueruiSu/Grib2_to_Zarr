import xarray as xr
import os
import time

file_name = "hrrr.t00z.wrfprsf00.grib2"
input_dir = "/blob/kmsw0eastau/data/hrrr/grib2/hrrr/20190110"

# file_name = "hrrr.t00z.wrfprsf00.grib2"
# input_dir = "/home/azureuser/cloudfiles/code/grib2/20190203"

print(".", input_dir)
t1 = time.time()
ds = xr.open_dataset(os.path.join(input_dir, file_name), engine="pynio")
t2 = time.time()
print("time:", t2 - t1)




