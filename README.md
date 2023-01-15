# 5300-Dolphin
DB Relation Manager project for CPSC5300 at Seattle U, Winter 2023, Project Dolphin

## **Verano Sprint**

### **Team**
Justin Thoreson & Mason Adsero

### **Milestone 1: Skeleton**
Initial construction of a SQL shell and basic configuration of a database via [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html). SQL statements are retrieved from the SQL shell, parsed into an abstract syntax tree via [the Hyrise C++ SQL Parser](https://github.com/hyrise/sql-parser), and printed out to the terminal.

Supports CREATE TABLE and SELECT statements. SELECT statements can include FROM clauses, JOIN clauses, WHERE clauses, and table aliases.

### **Compilation**
Execute the [`Makefile`](./Makefile) by running `$ make` in the CLI.

### **Usage**
To execute the SQL shell, run `$ ./sql5300 [ENV_DIR]` where `ENV_DIR` is the directory where the database environment resides.

SQL statements can be provided to the SQL shell when running. To terminate the SQL shell, enter `SQL> quit`.
