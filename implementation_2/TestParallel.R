rm(list = ls())

library(NYCTaxi)
library(hash)
library(Hmisc)
library(parallel)

idxs <- c(1L, 2L, 3L, 4L) # The indices of the files to be analyzed
path <- "../data"
size_bulk <- 500000L
size_cluster <- 4

# The serial version
#t_serial <- system.time(list_sum <- lapply(idxs, analyzeFile, path, size_bulk, TRUE))

# The parallel version
cl <- makeCluster(size_cluster)
clusterEvalQ(cl, {library(hash)})
t_parallel <- system.time(list_sum <- parLapply(cl, idxs, analyzeFile, path, size_bulk, TRUE))
stopCluster(cl)

# Combine the hash tables
count_occurence_all <- hash()
for (i in seq_along(idxs)) {
  for (key in keys(list_sum[[i]]$count_occurence)) {
    (has.key(key, count_occurence_all)) || (count_occurence_all[[key]] <- 0)
    count_occurence_all[[key]] <- count_occurence_all[[key]] + list_sum[[i]]$count_occurence[[key]]
  }
}

deciles <- wtd.quantile(as.numeric(keys(count_occurence_all)), weights = values(count_occurence_all), probs=seq(0, 1, by=0.1))

# Combine the sufficient statistics
mat_reg1_XX_XY_all <- matrix(0, nrow = 2, ncol = 3) 
mat_reg2_XX_XY_all <- matrix(0, nrow = 3, ncol = 4)
for (i in seq_along(idxs)) {
  mat_reg1_XX_XY_all <- mat_reg1_XX_XY_all + list_sum[[i]]$mat_reg1_XX_XY
  mat_reg2_XX_XY_all <- mat_reg2_XX_XY_all + list_sum[[i]]$mat_reg2_XX_XY
}
coeff1 <- solve(mat_reg1_XX_XY_all[, 1 : 2], mat_reg1_XX_XY_all[, 3])
coeff2 <- solve(mat_reg2_XX_XY_all[, 1 : 3], mat_reg2_XX_XY_all[, 4])
