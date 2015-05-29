#!/usr/bin/env python3

from subprocess import Popen, PIPE
from datetime import datetime
import numpy as np

def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        next(cr)
        return cr
    return start

def parse_file(idx_file, path, accumulator):
    cmd_parse_data = "unzip -cq {0}/trip_data_{1}_head.csv.zip | cut -d , -f 6,7".format(path, idx_file)
    cmd_parse_fare = "unzip -cq {0}/trip_fare_{1}_head.csv.zip | cut -d , -f 7,10,11".format(path, idx_file)

    format_time = '%Y-%m-%d %H:%M:%S'

    pipe_data = Popen(cmd_parse_data, shell=True, stdout=PIPE)
    pipe_fare = Popen(cmd_parse_fare, shell=True, stdout=PIPE)

    for (line_data, line_fare) in zip(pipe_data.stdout, pipe_fare.stdout):
        try:
            pickup_dropoff = line_data.strip().decode("utf-8").split(',')
            fares_str = line_fare.strip().decode("utf-8").split(',')
        
            trip_time = datetime.strptime(pickup_dropoff[1], format_time) - datetime.strptime(pickup_dropoff[0], format_time)
            fares = [float(fare) for fare in fares_str]
            #print((trip_time.total_seconds(), fares))
            accumulator.send((trip_time.total_seconds(), fares))
        except:
            continue
    
    accumulator.close()        
    return
       
@coroutine     
def accumulate_lines(count_trip_time, mat_reg1_XX_XY, mat_reg2_XX_XY, idx_file):
    try:
        while True:
            time_fares = (yield)
            print(time_fares)
    except GeneratorExit:
        print("Processing data/fare {0} completed!".format(idx_file))
    return

idx_file = 1
path = "../data"
mat_reg1_XX_XY = np.zeros((2, 3))
mat_reg2_XX_XY = np.zeros((3, 4))
count_trip_time = dict()

# Hook everything up
parse_file(idx_file, path, accumulate_lines(count_trip_time, mat_reg1_XX_XY, mat_reg2_XX_XY, idx_file))
