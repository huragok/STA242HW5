library(hash)

setHash <- function(hash_table) {
  hash_table$a <- "fuck"
}

hash1 <- hash()
setHash(hash1)
