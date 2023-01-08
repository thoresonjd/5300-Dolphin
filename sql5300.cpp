#include <cstdlib>
#include <cstring>
#include <iostream>
#include "db_cxx.h"

const u_int32_t ENV_FLAGS = DB_CREATE | DB_INIT_MPOOL;
const u_int32_t DB_FLAGS = DB_CREATE;
const std::string SQL_5300 = "sql5300.db";
const unsigned int BLOCK_SZ = 4096;

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "USAGE: " << argv[0] << " [db_environment]\n";
    return EXIT_FAILURE;
  }
  // Establish database environment
  const std::string ENV_DIR = argv[1];
  DbEnv env(0U);
  env.set_message_stream(&std::cout);
  env.set_error_stream(&std::cerr);
  env.open(ENV_DIR.c_str(), ENV_FLAGS, 0);
  std::cout << "(sql5300: running with database environment at " << ENV_DIR << std::endl;
  std::cout << "SQL> ";
  return EXIT_SUCCESS;
}