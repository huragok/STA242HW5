#' The function to analyze a single pair of files
#' 
#' Return a SummaryNYCTaxi S3 class object summarizing one pair of data and fare file, which contains the count of occurence for each value of fare amount less the tolls, and the sufficient statistics for the 2 regression tasks
#' 
#' @param idx the index of the file, ranging from 1 to 12
#' @param path the path of the directory holding the data and fare files
#' @param size_bulk the size of each bulk to process, limited by memory size
#' @param verbose whether to prompt an information when the processing is done
#' @param verbose_debug whether to print the error message when reaching the end of file (or potentially other error message)
#' @return a SummaryNYCTaxi S3 class object
analyzeFile <- function(idx, path, size_bulk = 500000L, verbose = FALSE, verbose_debug = FALSE) {
  if (verbose) {
    message(paste("Processing data/fare", idx, "..."))
  }
  
  # Check whether the files exist
  filename_data <- paste(path, "/", "trip_data_", idx, ".csv.zip", sep = "")
  filename_fare <- paste(path, "/", "trip_fare_", idx, ".csv.zip", sep = "")
  
  if (!any(file.exists(filename_data, filename_fare))) {
    stop("Both files do not exist!")
  }
  
  cmd_parse_data <- paste("unzip -cq ", filename_data, " | cut -d , -f 6,7", sep = "")
  cmd_parse_fare <- paste("unzip -cq ", filename_fare, " | cut -d , -f 7,10,11", sep = "")
  
  connection_data = pipe(cmd_parse_data, 'r') # open a connection to the file
  connection_fare = pipe(cmd_parse_fare, 'r')
  readLines(connection_data, 1, skipNul = TRUE) # Skip the header line
  readLines(connection_fare, 1, skipNul = TRUE)
  
  pattern_time <- "%Y-%m-%d %H:%M:%OS" # The pattern of the pickup and dropoff time
  hist <- hash() # The hash table to record the occurence of each fare less toll
  
  # The matrix to be updated iteratively based on bulks of records, the 2-by-2 (3-by-3) matrix X^HX and the 2-by-1 (3-by-1) vector X^HY, which is concatenated into a single 2-by-3 (3-by-4) matrix
  mat_reg1_XX_XY <- matrix(0, nrow = 2, ncol = 3) # Fare amount less the tolls vs trip time
  mat_reg2_XX_XY <- matrix(0, nrow = 3, ncol = 4) # Fare amount less the tolls vs trip time and surcharge

  # Start to go through the data file bulk by bulk
  tryCatch(
    {
      while (TRUE) {
        pickup_dropoff <- as.matrix(read.csv(connection_data, nrow=size_bulk, header=FALSE, stringsAsFactors=FALSE))
        fares <- data.matrix(read.csv(connection_fare, nrow=size_bulk, header=FALSE, stringsAsFactors=FALSE))
        
        # process data here
        trip_time <- difftime(strptime(pickup_dropoff[,2], pattern_time), strptime(pickup_dropoff[,1], pattern_time), units="secs")
        fare_less_toll <- fares[,3] - fares[,2]
        
        # Update the decile using a histogram (package "hash")
        hist_bulk <- table(fare_less_toll)
        value <- names(hist_bulk)
        count <- as.vector(hist_bulk)
        for (i in seq_along(hist_bulk)) {
          key <- value[i]
          (has.key(key, hist)) || (hist[[key]] <- 0)
          hist[[key]] <- hist[[key]] + count[i]
        }
        
        # Update the 2 matrices for regression
        mat_reg1_XX_XY <- updateSuffStat(mat_reg1_XX_XY, fare_less_toll, matrix(trip_time))
        mat_reg2_XX_XY <- updateSuffStat(mat_reg2_XX_XY, fare_less_toll, cbind(trip_time, fares[,1]))
      }
      
    },
    error=function(cond) {
      if (verbose_debug) {
        message("Appears to be at the end of file")
        message("Here's the original warning message:")
        message(paste(cond, "\n"))
      }
      return()
    },
    finally={
      close(connection_data)
      close(connection_fare)
    }
  )
  
  if (verbose) {
    message(paste("Processing data/fare", idx, "completed!"))
  }
  
  summary_NYCTaxi <- structure(list(count_occurence = hist, mat_reg1_XX_XY = mat_reg1_XX_XY, mat_reg2_XX_XY = mat_reg2_XX_XY), class = "SummaryNYCTaxi")
  return(summary_NYCTaxi)
}