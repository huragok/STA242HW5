#include "NYCTaxi.h"
using namespace Rcpp;

NumericMatrix updateSuffStat(NumericMatrix xx_xy_input, NumericVector y, NumericMatrix x_less_ones)
{
  
  int p = xx_xy_input.nrow(); // Number of predictors
  int n = y.size(); // Number of observations
  NumericMatrix xx_xy(p, p + 1);
  
  // Check the dimension of the input arguments
  if ((xx_xy.ncol() != p + 1) || (x_less_ones.ncol() != p - 1) || (x_less_ones.nrow() != n))
  {
    stop("Size of input arguments must be consistent");
  }
  
  // Update x^Hx
  for (int r = 0; r < p - 1; r++)
  {
    for (int c = r; c < p - 1; c++)
    {
      for (int i = 0; i < n; i++)
      {
        xx_xy(r, c) += x_less_ones(i, r) * x_less_ones(i, c);
      }
    }
    for (int i = 0; i < n; i++)
    {
      xx_xy(r, p - 1) += x_less_ones(i, r);
    }
  }
  xx_xy(p - 1, p - 1) += n;
  for (int r = 1; r < p; r++)
  {
    for (int c = 0; c < r; c++)
    {
      xx_xy(r, c) = xx_xy(c, r);
    }
  }
  
  // Update x^Hy
  for (int r = 0; r < p - 1; r++)
  {
    for (int i = 0; i < n; i++)
    {
      xx_xy(r, p) += x_less_ones(i, r) * y[i];
    }    
  }
  for (int i = 0; i < n; i++)
  {
    xx_xy(p - 1, p) += y[i];
  }

  // Update the cumulative x^Hx and x^Hy in this way for precision reason
  NumericVector::iterator itr_bulk;
  NumericVector::iterator itr_cum = xx_xy_input.begin();
  for (itr_bulk = xx_xy.begin(); itr_bulk != xx_xy.end(); itr_bulk++, itr_cum++)
  {
    *itr_bulk += *itr_cum;
  }
  return(xx_xy);
}