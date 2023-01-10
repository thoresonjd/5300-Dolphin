#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

const u_int32_t ENV_FLAGS = DB_CREATE | DB_INIT_MPOOL;
const u_int32_t DB_FLAGS = DB_CREATE;
const unsigned int BLOCK_SZ = 4096;
const std::string DB_NAME = "sql5300.db";
const std::string QUIT = "quit";

/**
 * @brief Runs the SQL shell loop and listens for queries
 */
void runSQLShell();

/**
 * @brief Processes a single SQL query
 * @param query A SQL query (or queries) to process
 */
void handleSQLQuery(std::string);

/**
 * @brief Processes SQL statements within a parsed query
 * @param parsedQuery A pointer to a parsed SQL query
 */
void handleSQLStatements(hsql::SQLParserResult*);

/**
 * @brief Executes a provided SQL statement
 * @param statement A pointer to a SQL statement
 */
void execute(const hsql::SQLStatement*);

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

  // Establish database
  Db db(&env, 0);
  db.set_message_stream(env.get_message_stream());
  db.set_error_stream(env.get_error_stream());
  db.set_re_len(BLOCK_SZ);
  db.open(NULL, DB_NAME.c_str(), NULL, DB_RECNO, DB_FLAGS, 0);

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
	std::cout << " (expect 'Hello, DB!')" << std::endl;

  // SQL shell
  std::cout << "(sql5300: running with database environment at " << ENV_DIR << std::endl;
  runSQLShell();

  return EXIT_SUCCESS;
}

void runSQLShell() {
  std::string query = "";
  while (query != QUIT) {
    std::cout << "SQL> ";
    std::getline(std::cin, query);
    handleSQLQuery(query);
  }
}

void handleSQLQuery(std::string query) {
  if (query == QUIT) return;
  hsql::SQLParserResult* const parsedQuery = hsql::SQLParser::parseSQLString(query);
  if (!parsedQuery->isValid())
    std::cout << "INVALID SQL: " << query << std::endl;
  else
    handleSQLStatements(parsedQuery);
}

void handleSQLStatements(hsql::SQLParserResult* parsedQuery) {
  for (int i = 0; i < parsedQuery->size(); i++) {
    const hsql::SQLStatement* const statement = parsedQuery->getStatement(i);
    execute(statement);
  }
}

void execute(const hsql::SQLStatement* statement) {
  switch (statement->type()) {
    case hsql::StatementType::kStmtSelect:
      // TODO: Print SELECT statement
      break;
    case hsql::StatementType::kStmtCreate:
      // TODO: Print CREATE statement
      break;
    default:
      hsql::printStatementInfo(statement);
      hsql::StatementType statementType = statement->type();
      std::cout << statementType << std::endl;
  }
}