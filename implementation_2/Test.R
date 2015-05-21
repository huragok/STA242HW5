
idx_file <- 1L
size_batch <- 4L
cmd_parse_data <- paste("unzip -cq ../data/trip_data_", idx_file, "_head.csv.zip | cut -d , -f 9", sep = "")
cmd_parse_fare <- paste("unzip -cq ../data/trip_fare_", idx_file, "_head.csv.zip | cut -d , -f 6,7,10", sep = "")

connection_data = pipe(cmd_parse_data, 'r')
connection_fare = pipe(cmd_parse_fare, 'r')
readLines(connection_data, 1, skipNul = TRUE) # Skip the header line
readLines(connection_fare, 1, skipNul = TRUE) # Skip the header line
tryCatch(
  {
    while (TRUE) {
      trip_time <- unlist(read.csv(connection_data, nrow=size_batch, header=FALSE, stringsAsFactors=FALSE), use.names = FALSE)
      fares <- data.matrix(read.csv(connection_fare, nrow=size_batch, header=FALSE, stringsAsFactors=FALSE))
      print(trip_time)
      print(fares)
      
      # FIXME: process data here
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

