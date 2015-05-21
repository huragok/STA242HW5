/*
 * main.cpp
 *
 *  Created on: May 20, 2015
 *      Author: wenhaowu
 */

#include "main.h"

int main()
{
  int idx_file = 1;
  std::string cmd_parse_data = "unzip -cq ../data/trip_data_" + std::to_string(idx_file) + ".csv.zip | cut -d , -f 9 --output-delimiter=\\  | head";
  std::string cmd_parse_fare = "unzip -cq ../data/trip_fare_"  + std::to_string(idx_file) + "_head.csv.zip | cut -d , -f 6,7,10 --output-delimiter=\\ ";

  std::cout << cmd_parse_data << std::endl;
  std::cout << cmd_parse_fare << std::endl;

  char *cmd_parse_data_char = (char*)cmd_parse_data.c_str();
  FILE* fp = popen(cmd_parse_data_char, "r");
  //while(fgets(line, PATH_MAX, fp) != NULL);
  pclose(fp);
  /*std::string time;
  while (istream_data >> time) {
    std::cout << time << std::endl;
  }*/

  return 0;
}
