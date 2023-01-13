/**
 * @file sql5300.cpp - TODO: describe
 * @authors Justin Thoreson & Mason Adsero
 * @see "Seattle University, CPSC5600, Winter 2023"
 */

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
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
void handleSQLStatements(hsql::SQLParserResult* const);

/**
 * @brief Executes a provided SQL statement
 * @param statement A pointer to a SQL statement
 */
void execute(const hsql::SQLStatement* const);

/**
 * @brief Unparses a statement into a string
 * @param statement A pointer to a SQL statement
 * @return An unparsed SQL statement
 */
std::string unparse(const hsql::SQLStatement* const);

/**
 * @brief Unparses a SELECT statement into a string
 * @param statement A pointer to a SELECT statement
 * @return An unparsed SELECT statement
 */
std::string unparse(const hsql::SelectStatement* const);

/**
 * @brief Unparses a CREATE statement into a string
 * @param statement A pointer to a CREATE statement
 * @return An unparsed CREATE statement
 */
std::string unparse(const hsql::CreateStatement* const);

/**
 * @brief Converts an Expr type expression to a string
 * @param expr A pointer the an expression
 * @return The string equivalent of an expression
 */
std::string toString(hsql::Expr* const);

/**
 * @brief Converts a ColumnDefinition type to a string
 * @param col A pointer to a database column
 * @return The string equivalent of a column
 */
std::string toString(hsql::ColumnDefinition* const);

/**
 * @brief Converts a TableRef type to a string
 * @param table A pointer to a database table
 * @return The string equivalent of a table
 */
std::string toString(hsql::TableRef*);

/**
 * @brief Converts a JoinDefinition type to a string
 * @param table A pointer to a join definition
 * @return The string equivalent of a join definition
 */
std::string toString(hsql::JoinDefinition*);

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

void handleSQLStatements(hsql::SQLParserResult* const parsedQuery) {
  std::size_t parsedQuerySize = parsedQuery->size();
  for (std::size_t i = 0; i < parsedQuerySize; i++) {
    const hsql::SQLStatement* const statement = parsedQuery->getStatement(i);
    execute(statement);
  }
}

void execute(const hsql::SQLStatement* const statement) {
  std::string unparsedStatement = unparse(statement);
  std::cout << unparsedStatement << std::endl;
}

std::string unparse(const hsql::SQLStatement* const statement) {
  switch (statement->type()) {
    case hsql::StatementType::kStmtSelect:
      return unparse(dynamic_cast<const hsql::SelectStatement*>(statement));
    case hsql::StatementType::kStmtCreate:
      return unparse(dynamic_cast<const hsql::CreateStatement*>(statement));
    default:
      return "Not implemented";
  }
}

std::string unparse(const hsql::SelectStatement* const selectStatement) {
  std::string unparsed = "SELECT ";
  std::vector<hsql::Expr*>* selectList = selectStatement->selectList;
  std::size_t selectListSize = selectList->size();
  for (std::size_t i = 0; i < selectListSize; i++) {
    unparsed.append(toString(selectList->at(i)));
    unparsed.append(i + 1 < selectListSize ? ", " : " ");
  }
  unparsed.append("FROM ");
  unparsed.append(toString(selectStatement->fromTable));
  if (selectStatement->whereClause)
    unparsed.append(" WHERE ").append(toString(selectStatement->whereClause));
  return unparsed;
}

std::string unparse(const hsql::CreateStatement* const createStatement) {
  if (createStatement->type != hsql::CreateStatement::CreateType::kTable)
    return "unimplemented";
  std::string unparsed = "CREATE TABLE ";
  unparsed.append(createStatement->tableName).append(" (");
  std::size_t columnsLength = createStatement->columns->size();
  for (std::size_t i = 0; i < columnsLength; i++){
    hsql::ColumnDefinition* col = createStatement->columns->at(i);
    unparsed.append(toString(col));
    unparsed.append(i + 1 < columnsLength ? ", " : ") ");
  }
  return unparsed;
}

std::string toString(hsql::Expr* const expr) {
  std::string result = "";
  switch (expr->type) {
    case hsql::ExprType::kExprStar:
      result = "*";
      break;
    case hsql::ExprType::kExprOperator:
      result.append(toString(expr->expr)).append(" ");
      result.push_back(expr->opChar);
      result.append(" ");
      if (expr->expr2)
        result.append(toString(expr->expr2));
      else if (expr->exprList)
        for(hsql::Expr* expr : *expr->exprList)
          result.append(toString(expr));
      break;
    case hsql::ExprType::kExprColumnRef:
    case hsql::ExprType::kExprLiteralString:
      if (expr->table)
        result.append(expr->table).append(".");
      else if (expr->alias)
        result.append(expr->alias).append(".");
      result.append(expr->name);
      break;
    case hsql::ExprType::kExprLiteralInt:
      result = std::to_string(expr->ival);
      break;
    case hsql::ExprType::kExprLiteralFloat:
      result = std::to_string(expr->fval);
      break;
    case hsql::ExprType::kExprFunctionRef:
      result = "kExprFunctionRef unimplemented";
      break;
  }
  return result;
}

std::string toString(hsql::ColumnDefinition* const col) {
  std::string result = "";
  result.append(col->name);
  switch (col->type) {
    case hsql::ColumnDefinition::DataType::TEXT:
      result.append(" TEXT");
      break;
    case hsql::ColumnDefinition::DataType::INT:
      result.append(" INT");
      break;
    case hsql::ColumnDefinition::DataType::DOUBLE:
      result.append(" DOUBLE");
      break;
  }
  return result;
}

std::string toString(hsql::TableRef* table) {
  std::string result = "";
  switch (table->type) {
    case hsql::TableRefType::kTableName:
      result.append(table->name);
      break;
    case hsql::TableRefType::kTableJoin:
      result.append(toString(table->join));
      break;
    case hsql::TableRefType::kTableCrossProduct:
      std::size_t tableListSize = table->list->size();
      for (std::size_t i = 0; i < tableListSize; i++) {
        result.append(toString(table->list->at(i)));
        result.append(i + 1 < tableListSize ? ", " : " ");
      }
      break;
  }
  if (table->alias)
    result.append(" AS ").append(table->alias);
  return result;
}

std::string toString(hsql::JoinDefinition* join) {
  std::string result = "";
  result.append(toString(join->left));
  switch (join->type) {
    case hsql::JoinType::kJoinInner:
      result.append(" JOIN ");
      break;
    case hsql::JoinType::kJoinLeft:
      result.append(" LEFT JOIN ");
      break;
  }
  result.append(toString(join->right));
  result.append(" ON ").append(toString(join->condition));
  return result;
}