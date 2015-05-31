#!/usr/bin/env python3

from subprocess import Popen, PIPE
from datetime import datetime
import numpy as np
import pandas as pd
from pandas import Series
import time
from multiprocessing import Pool

def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        next(cr)
        return cr
    return start

def parse_file(idx_file, path, accumulator):
    cmd_parse_data = "unzip -cq {0}/trip_data_{1}.csv.zip | cut -d , -f 6,7".format(path, idx_file)
    cmd_parse_fare = "unzip -cq {0}/trip_fare_{1}.csv.zip | cut -d , -f 7,10,11".format(path, idx_file)

    format_time = '%Y-%m-%d %H:%M:%S'

    pipe_data = Popen(cmd_parse_data, shell=True, stdout=PIPE)
    pipe_fare = Popen(cmd_parse_fare, shell=True, stdout=PIPE)

    for (line_data, line_fare) in zip(pipe_data.stdout, pipe_fare.stdout):
        try:
            pickup_dropoff = line_data.strip().decode("utf-8").split(',')
            fares_str = line_fare.strip().decode("utf-8").split(',')
        
            trip_time = datetime.strptime(pickup_dropoff[1], format_time) - datetime.strptime(pickup_dropoff[0], format_time)
            fares = [float(fare) for fare in fares_str]
            
            accumulator.send((trip_time.total_seconds(), fares[2] - fares[1], fares[0]))
        except:
            continue
    
    accumulator.close()        
    return
       
@coroutine     
def accumulate_lines(count_fare, mat_reg1_XX_XY, mat_reg2_XX_XY, idx_file, verbose):
    try:
        while True:
            time_fares = (yield)
            #print(time_fares)
            
            # Update the count of occurence for the total fare less toll
            count_fare[time_fares[1]] = count_fare.get(time_fares[1], 0) + 1
            
            # Update the sufficient statistics
            mat_reg1_XX_XY[0, 0] += time_fares[0] ** 2
            mat_reg1_XX_XY[0, 1] += time_fares[0]
            mat_reg1_XX_XY[1, 1] += 1
            mat_reg1_XX_XY[0, 2] += time_fares[0] * time_fares[1]
            mat_reg1_XX_XY[1, 2] += time_fares[1]
            
            mat_reg2_XX_XY[0, 1] += time_fares[0] * time_fares[2]
            mat_reg2_XX_XY[1, 1] += time_fares[2] ** 2
            mat_reg2_XX_XY[1, 2] += time_fares[2]
            mat_reg2_XX_XY[1, 3] += time_fares[2] * time_fares[1]

    except GeneratorExit:
        mat_reg1_XX_XY[1, 0] = mat_reg1_XX_XY[0, 1]
        
        mat_reg2_XX_XY[0, 0] = mat_reg1_XX_XY[0, 0]
        mat_reg2_XX_XY[0, 2] = mat_reg1_XX_XY[0, 1]
        mat_reg2_XX_XY[1, 0] = mat_reg2_XX_XY[0, 1]
        mat_reg2_XX_XY[2, 0] = mat_reg2_XX_XY[0, 2]
        mat_reg2_XX_XY[2, 1] = mat_reg2_XX_XY[1, 2]
        mat_reg2_XX_XY[2, 2] = mat_reg1_XX_XY[1, 1]
        
        mat_reg2_XX_XY[0, 3] = mat_reg1_XX_XY[0, 2]
        mat_reg2_XX_XY[2, 3] = mat_reg1_XX_XY[1, 2]
        
        
        if (verbose):
            print("Processing data/fare {0} completed!".format(idx_file))
    return

def analyze_file(arg_list):
    idx_file = arg_list[0]
    path = arg_list[1]
    verbose = arg_list[2]
    mat_reg1_XX_XY = np.zeros((2, 3))
    mat_reg2_XX_XY = np.zeros((3, 4))
    count_fare = dict()

    # Hook everything up
    parse_file(idx_file, path, accumulate_lines(count_fare, mat_reg1_XX_XY, mat_reg2_XX_XY, idx_file, verbose))

    # Return a pandas Series
    count_fare = Series(count_fare)
     
    return([count_fare, mat_reg1_XX_XY, mat_reg2_XX_XY])


if __name__ == "__main__":
    path = "../data"
    idxs_file = range(1, 13)
    n_file = len(idxs_file)
    n_process = 12
    
    arg_lists = list(zip(idxs_file, [path] * n_file, [True] * n_file))
    
    # Parallel run
    start_time = time.time()
    with Pool(processes = n_process) as pool:
        results = pool.map(analyze_file, arg_lists)
        
        # Reduce the results
        count_fare = Series()
        mat_reg1_XX_XY = np.zeros((2, 3))
        mat_reg2_XX_XY = np.zeros((3, 4))
        for result in results:
            count_fare = count_fare.add(result[0], fill_value = 0)
            mat_reg1_XX_XY += result[1]
            mat_reg2_XX_XY += result[2]
        
        # Compute the deciles    
        cdf = np.cumsum(count_fare) / np.sum(count_fare)
        deciles = [cdf[cdf >= p].index[0] for p in np.arange(0, 1.05, 0.1)]
        
        # Solve the regressions
        coeff1 = np.linalg.solve(mat_reg1_XX_XY[:, 0:2], mat_reg1_XX_XY[:, 2])
        coeff2 = np.linalg.solve(mat_reg2_XX_XY[:, 0:3], mat_reg2_XX_XY[:, 3])
        
        print("Deciles of the total amount less toll:")
        print(deciles)
        print("Linear model of the total amount less the tolls versus trip time:")
        print(coeff1)
        print("Linear model of the total amount less the tolls versus surcharge and trip time:")
        print(coeff2)
        print("--- %s seconds ---" % (time.time() - start_time))
    
