import os
import datetime

root_dir = "/blob/kmsw0eastau/data/hrrr/grib2/hrrr"
all_date_dir = os.listdir(root_dir)
all_date_dir =  sorted(all_date_dir, key=lambda x: datetime.datetime.strptime(x, '%Y%m%d'))

# len: 731, 600(big)+131(big)


# with open(f"./run_all_vars_big_1.sh", "w") as file:  
#     for time_index in range(len(all_date_dir[:200])):
#         file.write(f"bash ./run_day/{all_date_dir[time_index]}.sh \n")  
#         print(all_date_dir[time_index], "done.")

# with open(f"./run_all_vars_big_2.sh", "w") as file:  
#     for time_index in range(len(all_date_dir[200:400])):
#         file.write(f"bash ./run_day/{all_date_dir[time_index+200]}.sh \n")  
#         print(all_date_dir[time_index+200], "done.")

with open(f"./run_all_vars_big_3.sh", "w") as file:  
    for time_index in range(len(all_date_dir[400:500])):
        file.write(f"bash ./run_day/{all_date_dir[time_index+400]}.sh \n")  
        print(all_date_dir[time_index+400], "done.")

# with open(f"./run_all_vars_big_4_on_small.sh", "w") as file:  
#     for time_index in range(len(all_date_dir[500:600])):
#         file.write(f"bash ./run_day/{all_date_dir[time_index+500]}.sh \n")  
#         print(all_date_dir[time_index+500], "done.")

# with open(f"./run_all_vars_small_1.sh", "w") as file:  
#     for time_index in range(len(all_date_dir[600:])):
#         file.write(f"bash ./run_day/{all_date_dir[time_index+600]}.sh \n")  
#         print(all_date_dir[time_index+600], "done.")


