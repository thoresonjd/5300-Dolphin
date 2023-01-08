#include <cstdlib>
#include <iostream>
#include "db_cxx.h"

const u_int32_t ENV_FLAGS = DB_CREATE | DB_INIT_MPOOL;

int main(int argc, char** argv) {
  // Retrieve command line arguments
  if (argc != 2) {
    std::cout << "USAGE: ./ass [database directory]\n";
    return EXIT_FAILURE;
  }

  // Print environment directory
  const std::string ENV_DIR = argv[1];
  std::cout << ENV_DIR << std::endl;

  // Establish database environment
  DbEnv env(0U);
  env.set_message_stream(&std::cout);
  env.set_error_stream(&std::cerr);
  env.open(ENV_DIR.c_str(), ENV_FLAGS, 0);

  return EXIT_SUCCESS;
}