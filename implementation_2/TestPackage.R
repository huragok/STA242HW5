rm(list = ls())

library(NYCTaxi)
library(hash)
library(Hmisc)

path <- "../data"
idx_file <- 1L
size_bulk <- 500000L

# Try to analyze a single file
Rprof("analyzeFile.out")
sum <- analyzeFile(idx_file, path, size_bulk, TRUE, TRUE)
Rprof(NULL)

coeff1 <- solve(sum$mat_reg1_XX_XY[, 1 : 2], sum$mat_reg1_XX_XY[, 3])
coeff2 <- solve(sum$mat_reg2_XX_XY[, 1 : 3], sum$mat_reg2_XX_XY[, 4])

deciles <- wtd.quantile(as.numeric(keys(sum$count_occurence)), weights = values(sum$count_occurence), probs=seq(0, 1, by=0.1))

summaryRprof("analyzeFile.out")
summaryRprof("analyzeFile.out", lines = "show")
