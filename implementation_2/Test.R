rm(list = ls())

library(NYCTaxi)
library(hash)
library(Hmisc)

idx_file <- 9L
size_batch <- 500000L
cmd_parse_data <- paste("unzip -cq ../data/trip_data_", idx_file, ".csv.zip | cut -d , -f 6,7", sep = "")
cmd_parse_fare <- paste("unzip -cq ../data/trip_fare_", idx_file, ".csv.zip | cut -d , -f 6,7,10", sep = "")

connection_data = pipe(cmd_parse_data, 'r')
connection_fare = pipe(cmd_parse_fare, 'r')
readLines(connection_data, 1, skipNul = TRUE) # Skip the header line
readLines(connection_fare, 1, skipNul = TRUE) # Skip the header line

pattern_time <- "%Y-%m-%d %H:%M:%OS"
# The hash table to record the occurence of each fare less toll
hist <- hash()

# The matrix to be updated iteratively based on bulks of records, the 2-by-2 (3-by-3) matrix X^HX and the 2-by-1 (3-by-1) vector X^HY, which is concatenated into a single 2-by-3 (3-by-4) matrix
mat_reg1_XX_XY <- matrix(0, nrow = 2, ncol = 3) # Fare amount less the tolls vs trip time
mat_reg2_XX_XY <- matrix(0, nrow = 3, ncol = 4) # Fare amount less the tolls vs trip time and surcharge

Rprof("ProfNYCTaxiOriginal.out", line.profiling=TRUE) # Profiling the program
tryCatch(
  {
    while (TRUE) {
      pickup_dropoff <- read.csv(connection_data, nrow=size_batch, header=FALSE, stringsAsFactors=FALSE)
      pickup_dropoff <- data.frame(lapply(pickup_dropoff, strptime, pattern_time))
      fares <- read.csv(connection_fare, nrow=size_batch, header=FALSE, stringsAsFactors=FALSE)
      fares <- data.frame(lapply(fares, as.numeric))
      bulk <- na.omit(cbind(pickup_dropoff, fares)) # Now it is more robust against unrecognized any field type or pattern
      #print(trip_time)
      #print(fares)
      
      # process data here
      trip_time <- difftime(bulk[,2], bulk[,1], units="secs")
      fare_less_toll <- bulk[,3] - bulk[,5] # Get the fare less the toll

      # Update the decile using a histogram (package "hash")
      hist_bulk <- table(fare_less_toll)
      value <- names(hist_bulk)
      count <- as.vector(hist_bulk)
      
      for (idx in seq_along(hist_bulk)) {
        key <- value[idx]
        (has.key(key, hist)) || (hist[[key]] <- 0)
        hist[[key]] <- hist[[key]] + count[idx]
      }
      
      # Update the 2 matrices for regression
      mat_reg1_XX_XY <- updateSuffStat(mat_reg1_XX_XY, fare_less_toll, matrix(trip_time))
      mat_reg2_XX_XY <- updateSuffStat(mat_reg2_XX_XY, fare_less_toll, cbind(trip_time, bulk[,4]))
    }
    
  },
   error=function(cond) {
     message("Appears to be at the end of file")
     message("Here's the original warning message:")
     message(paste(cond, "\n"))
     return()
   },
   finally={
     close(connection_data)
     close(connection_fare)
   }
 )
Rprof(NULL)
coeff1 <- solve(mat_reg1_XX_XY[, 1 : 2], mat_reg1_XX_XY[, 3])
coeff2 <- solve(mat_reg2_XX_XY[, 1 : 3], mat_reg2_XX_XY[, 4])

deciles <- wtd.quantile(as.numeric(keys(hist)), weights = values(hist), probs=seq(0, 1, by=0.1))

summaryRprof("ProfNYCTaxiOriginal.out", lines = "show")
