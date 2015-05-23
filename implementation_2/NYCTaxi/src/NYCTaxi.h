#ifndef NYCTAXI
#define NYCTAXI

#include <Rcpp.h>
#include <string>

using namespace Rcpp;

//' Function to update the sufficient statistics of linear regression based on a bulk of data
//' 
//' The sufficient statistics of the linear regression is recorded as a p-by-(p+1) matrix which is the row concatenation of x^Hx and x^Hy where p is the number of predictors (including constant 1).
//' @param xx_xy a p-by-(p+1) matrix, [1:p, 1:p] represents the current value of x^Hx and [p+1, 1:p] represents the current value of x^Hy
//' @param y a n-by-1 vector, the bulk of observatons
//' @param x_less_ones a n-by-(p-1) matrix, the bulk of predictors excluding 1
//' @return the updated sufficient statistic xx_xy
//' @export
// [[Rcpp::export]]
NumericMatrix updateSuffStat(NumericMatrix xx_xy, NumericVector y, NumericMatrix x_less_ones);

#endif