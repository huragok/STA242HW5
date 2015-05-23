#!/bin/bash

fare="trip_fare_8.csv.zip"
fare_tmp="trip_fare_8.csv"
fare_dup="trip_fare_8_dup.csv.zip"
data="trip_data_8.csv.zip"

mv $fare $fare_dup
head -n 12597110 <(unzip -cq $fare_dup) > $fare_tmp
zip $fare_tmp
rm $fare_tmp
