
idx_file <- 1L
size_batch <- 4L
cmd_parse_data <- paste("unzip -cq ../data/trip_data_", idx_file, "_head.csv.zip | cut -d , -f 9", sep = "")
cmd_parse_fare <- paste("unzip -cq ../data/trip_fare_", idx_file, "_head.csv.zip | cut -d , -f 6,7,10", sep = "")

connection_data = pipe(cmd_parse_data, 'r')
connection_fare = pipe(cmd_parse_fare, 'r')
readLines(connection_data, 1, skipNul = TRUE) # Skip the header line
readLines(connection_fare, 1, skipNul = TRUE) # Skip the header line

# The matrix to be updated iteratively based on bulks of records, the 2-by-2 (3-by-3) matrix X^HX and the 2-by-1 (3-by-1) vector X^HY, which is concatenated into a single 2-by-3 (3-by-4) matrix
mat_reg1_XX_XY <- matrix(0, nrow = 2, ncol = 3) # Fare amount less the tolls vs trip time
mat_reg2_XX_XY <- matrix(0, nrow = 3, ncol = 4) # Fare amount less the tolls vs trip time and surcharge

tryCatch(
  {
    while (TRUE) {
      trip_time <- unlist(read.csv(connection_data, nrow=size_batch, header=FALSE, stringsAsFactors=FALSE), use.names = FALSE)
      fares <- data.matrix(read.csv(connection_fare, nrow=size_batch, header=FALSE, stringsAsFactors=FALSE))
      print(trip_time)
      print(fares)
      
      # FIXME: process data here
      # Update the decile using a histogram (package "hash")
      

      # Update the 2 matrices for regression
      mat_reg1_XX_XY <- updateSuffStat(mat_reg1_XX_XY, fares[,1] - fares[,3], trip_time) # FIXME: this function to be implemented in C++
      mat_reg2_XX_XY <- updateSuffStat(mat_reg2_XX_XY, fares[,1] - fares[,3], rbind(trip_time, fares[,2]))
    }
  },
  error=function(cond) {
    message("Appears to be at the end of file")
    message("Here's the original warning message:")
    message(cond)
    return()
  },
  finally={
    close(connection)
  }
)

