#!/usr/bin/env python3

from subprocess import Popen, PIPE
from datetime import datetime

idx_file = 1
path = "../data"

cmd_parse_data = "unzip -cq {0}/trip_data_{1}_head.csv.zip | cut -d , -f 6,7".format(path, idx_file)
cmd_parse_fare = "unzip -cq {0}/trip_fare_{1}_head.csv.zip | cut -d , -f 7,10,11".format(path, idx_file)

format_time = '%Y-%m-%d %H:%M:%S'

print(cmd_parse_data)
print(cmd_parse_fare)

pipe_data = Popen(cmd_parse_data, shell=True, stdout=PIPE)
pipe_fare = Popen(cmd_parse_fare, shell=True, stdout=PIPE)

# Skip the first line
#next(pipe_data.stdout)
#next(pipe_fare.stdout)

for (line_data, line_fare) in zip(pipe_data.stdout, pipe_fare.stdout):
    try:
        pickup_dropoff = line_data.strip().decode("utf-8").split(',')
        fares_str = line_fare.strip().decode("utf-8").split(',')
    
        trip_time = datetime.strptime(pickup_dropoff[1], format_time) - datetime.strptime(pickup_dropoff[0], format_time)
        fares = [float(fare) for fare in fares_str]
        print(trip_time.total_seconds())
        print(fares)
    except:
        continue
