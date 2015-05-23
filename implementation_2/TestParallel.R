rm(list = ls())

library(NYCTaxi)
library(parallel)

idxs <- seq(12) # The indices of the files to be analyzed
path <- "../data"
size_bulk <- 500000L
size_cluster <- 4

# The serial version
t_serial <- system.time(list_sum_serial <- lapply(idxs, analyzeFile, path, size_bulk, TRUE))

# The parallel version
cl <- makeCluster(size_cluster)
t_parallel <- system.time(list_sum_parallel <- parLapply(cl, idxs, analyzeFile, path, size_bulk))
stopCluster(cl)

results_serial <- reduceListSummaryNYCTaxi(list_sum_serial)
print(results_serial)

results_parallel <- reduceListSummaryNYCTaxi(list_sum_parallel)
print(results_parallel)
