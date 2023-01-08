#include <cstdlib>
#include <iostream>
#include "db_cxx.h"

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "USAGE: ./ass [database directory]\n";
    return EXIT_FAILURE;
  }
  const std::string ENV_DIR = argv[1];
  std::cout << ENV_DIR << std::endl;
  return EXIT_SUCCESS;
}