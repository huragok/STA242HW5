#!/bin/bash

for idx in $(seq $1 $2)
do
  filename_data="trip_data_${idx}.csv.zip"
  filename_fare="trip_fare_${idx}.csv.zip"
  
  if [ ! -f $filename_data ]; then
    echo "${filename_data} does not exist!"
  elif [ ! -f $filename_fare ]; then
    echo "${filename_fare} does not exist!"
  else
    echo "Comparing ${filename_data} and ${filename_fare} ..."
    
    if diff -w <(unzip -cq $filename_data | cut -d , -f 1,2,6) <(unzip -cq $filename_fare | cut -d , -f 1,2,4) > /dev/null; then
      echo "${filename_data} and ${filename_fare} match!"
    else
      echo "${filename_data} and ${filename_fare} do not match!"
    fi
  fi
done
exit 0 
