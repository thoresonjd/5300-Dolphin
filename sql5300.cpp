#include <cstdlib>
#include <cstring>
#include <iostream>
#include "db_cxx.h"

const u_int32_t ENV_FLAGS = DB_CREATE | DB_INIT_MPOOL;
const u_int32_t DB_FLAGS = DB_CREATE;
const std::string SQL_5300 = "sql5300.db";
const unsigned int BLOCK_SZ = 4096;

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

  // Establish database
  Db db(&env, 0);
  db.set_message_stream(env.get_message_stream());
  db.set_error_stream(env.get_error_stream());
  db.set_re_len(BLOCK_SZ);
  db.open(NULL, SQL_5300.c_str(), NULL, DB_RECNO, DB_FLAGS, 0);

  // Write block to db
  char block[BLOCK_SZ];
  Dbt data(block, sizeof(block));
  int block_number;
  Dbt key(&block_number, sizeof(block_number));
  block_number = 1;
  strcpy(block, "Hello, DB!");
  db.put(NULL, &key, &data, 0);

  // Read block from db
  Dbt rData;
  db.get(NULL, &key, &rData, 0);
  std::cout << "Read (block #" << block_number << "): '" << (char *)rData.get_data() << "'";
	std::cout << " (expect 'hello!')" << std::endl;

  return EXIT_SUCCESS;
}