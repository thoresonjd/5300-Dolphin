/**
 * @file sql5300.cpp - SQL Shell
 * @authors Justin Thoreson & Mason Adsero
 * @see "Seattle University, CPSC5600, Winter 2023"
 */

#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"

const u_int32_t ENV_FLAGS = DB_CREATE | DB_INIT_MPOOL;
const u_int32_t DB_FLAGS = DB_CREATE;
const unsigned int BLOCK_SZ = 4096;
const std::string DB_NAME = "sql5300.db";
const std::string TEST = "test", QUIT = "quit";

// Global DbEnv
DbEnv* _DB_ENV;

/**
 * Tests the establishment of, writing to, and reading from a database
 * @param envDir The database environment directory
 */
void dbConfig(const std::string);

/**
 * Runs the SQL shell loop and listens for queries
 */
void runSQLShell();

/**
 * Processes a single SQL query
 * @param sql A SQL query (or queries) to process
 */
void handleSQL(std::string);

/**
 * Processes SQL statements within a parsed query
 * @param parsedSQL A pointer to a parsed SQL query
 */
void handleStatements(hsql::SQLParserResult* const);

/**
 * Executes a provided SQL statement
 * @param statement A pointer to a SQL statement
 */
void execute(const hsql::SQLStatement* const);

/**
 * Unparses a statement into a string
 * @param statement A pointer to a SQL statement
 * @return An unparsed SQL statement
 */
std::string unparse(const hsql::SQLStatement* const);

/**
 * Unparses a SELECT statement into a string
 * @param statement A pointer to a SELECT statement
 * @return An unparsed SELECT statement
 */
std::string unparse(const hsql::SelectStatement* const);

/**
 * Unparses a CREATE statement into a string
 * @param statement A pointer to a CREATE statement
 * @return An unparsed CREATE statement
 */
std::string unparse(const hsql::CreateStatement* const);

/**
 * Converts an Expr type expression to a string
 * @param expr A pointer the an expression
 * @return The string equivalent of an expression
 */
std::string toString(hsql::Expr* const);

/**
 * Converts a ColumnDefinition type to a string
 * @param col A pointer to a database column
 * @return The string equivalent of a column
 */
std::string toString(hsql::ColumnDefinition* const);

/**
 * Converts a TableRef type to a string
 * @param table A pointer to a database table
 * @return The string equivalent of a table
 */
std::string toString(hsql::TableRef* const);

/**
 * Converts a JoinDefinition type to a string
 * @param table A pointer to a join definition
 * @return The string equivalent of a join definition
 */
std::string toString(hsql::JoinDefinition* const);

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cout << "USAGE: " << argv[0] << " [db_environment]\n";
        return EXIT_FAILURE;
    }
    const std::string ENV_DIR = argv[1];
    dbConfig(ENV_DIR);
    // std::cout << "(sql5300: running with database environment at " << ENV_DIR << std::endl;
    runSQLShell();
    delete _DB_ENV;
    return EXIT_SUCCESS;
}

void dbConfig(const std::string envDir) {
    _DB_ENV = new DbEnv(0U);
    _DB_ENV->set_message_stream(&std::cout);
    _DB_ENV->set_error_stream(&std::cerr);
    _DB_ENV->open(envDir.c_str(), ENV_FLAGS, 0);
}

void runSQLShell() {
    std::string sql = "";
    while (sql != QUIT) {
        std::cout << "SQL> ";
        std::getline(std::cin, sql);
        if (sql.length())
            handleSQL(sql);
    }
}

void handleSQL(std::string sql) {
    if (sql == QUIT) return;
    if (sql == TEST) {
        std::cout << (test_heap_storage() ? "Passed" : "Failed") << std::endl;
        return;
    }
    hsql::SQLParserResult *const parsedSQL = hsql::SQLParser::parseSQLString(sql);
    if (parsedSQL->isValid())
        handleStatements(parsedSQL);
    else
        std::cout << "INVALID SQL: " << sql << std::endl;
}

void handleStatements(hsql::SQLParserResult* const parsedSQL) {
    std::size_t nStatements = parsedSQL->size();
    for (std::size_t i = 0; i < nStatements; i++) {
        const hsql::SQLStatement *const statement = parsedSQL->getStatement(i);
        execute(statement);
    }
}

void execute(const hsql::SQLStatement* const statement) {
    std::cout << unparse(statement) << std::endl;
}

std::string unparse(const hsql::SQLStatement* const statement) {
    switch (statement->type()) {
        case hsql::StatementType::kStmtSelect:
            return unparse(dynamic_cast<const hsql::SelectStatement*>(statement));
        case hsql::StatementType::kStmtCreate:
            return unparse(dynamic_cast<const hsql::CreateStatement*>(statement));
        default:
            return "...";
    }
}

std::string unparse(const hsql::SelectStatement* const statement) {
    std::string unparsed = "SELECT ";
    std::size_t nSelections = statement->selectList->size();
    for (std::size_t i = 0; i < nSelections; i++) {
        unparsed.append(toString(statement->selectList->at(i)));
        unparsed.append(i + 1 < nSelections ? ", " : " ");
    }
    unparsed.append("FROM ");
    unparsed.append(toString(statement->fromTable));
    if (statement->whereClause)
        unparsed.append(" WHERE ").append(toString(statement->whereClause));
    return unparsed;
}

std::string unparse(const hsql::CreateStatement* const statement) {
    if (statement->type != hsql::CreateStatement::CreateType::kTable)
        return "...";
    std::string unparsed = "CREATE TABLE ";
    unparsed.append(statement->tableName).append(" (");
    std::size_t nCols = statement->columns->size();
    for (std::size_t i = 0; i < nCols; i++) {
        hsql::ColumnDefinition *col = statement->columns->at(i);
        unparsed.append(toString(col));
        unparsed.append(i + 1 < nCols ? ", " : ") ");
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
                for (hsql::Expr *expr : *expr->exprList)
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
        default:
            result = "...";
            break;
    }
    return result;
}

std::string toString(hsql::ColumnDefinition* const col) {
    std::string result(col->name);
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
        default:
            result.append(" ...");
            break;
    }
    return result;
}

std::string toString(hsql::TableRef* const table) {
    std::string result = "";
    switch (table->type) {
        case hsql::TableRefType::kTableName:
            result.append(table->name);
            break;
        case hsql::TableRefType::kTableJoin:
            result.append(toString(table->join));
            break;
        case hsql::TableRefType::kTableCrossProduct: {
            std::size_t nTables = table->list->size();
            for (std::size_t i = 0; i < nTables; i++) {
                result.append(toString(table->list->at(i)));
                result.append(i + 1 < nTables ? ", " : " ");
            }
            break;
        }
        default:
            result.append("...");
            break;
    }
    if (table->alias)
        result.append(" AS ").append(table->alias);
    return result;
}

std::string toString(hsql::JoinDefinition* const join) {
    std::string result(toString(join->left));
    switch (join->type) {
        case hsql::JoinType::kJoinInner:
            result.append(" JOIN ");
            break;
        case hsql::JoinType::kJoinLeft:
            result.append(" LEFT JOIN ");
            break;
        default:
            result.append(" ... ");
            break;
    }
    result.append(toString(join->right));
    result.append(" ON ").append(toString(join->condition));
    return result;
}