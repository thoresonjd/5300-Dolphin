# 5300-Dolphin
DB Relation Manager project for CPSC5300 at Seattle U, Winter 2023, Project Dolphin

## **Verano Sprint**

### **Team**
Justin Thoreson & Mason Adsero

### **Milestone 1: Skeleton**
Initial construction of a SQL shell and basic configuration of a database via [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html). SQL statements are retrieved from the SQL shell, parsed into an abstract syntax tree via [the Hyrise C++ SQL Parser](https://github.com/hyrise/sql-parser), and printed out to the terminal.

Supports CREATE TABLE and SELECT statements. SELECT statements can include FROM clauses, JOIN clauses, WHERE clauses, and table aliases.

To access the code for Milestone 1, run `git checkout tags/Milestone1`.

### **Milestone 2: Rudimentary Storage Engine**
A rudimentary heap storage engine implemented with slotted page block architecture and `RecNo` file type.

To access the code for Milestone 2, run `git checkout tags/Milestone2`.

### **Compilation**
Execute the [`Makefile`](./Makefile) by running `$ make` in the CLI.

### **Usage**
To execute, run `$ ./sql5300 [ENV_DIR]` where `ENV_DIR` is the directory where the database environment resides.

SQL statements can be provided to the SQL shell when running. To terminate the SQL shell, enter `SQL> quit`.

### **Testing**
To test the functionality of the rudimentary storage engine, enter `SQL> test`. This will run the test function, `test_heap_storage`, defined in [`heap_storage.cpp`](./heap_storage.cpp).

### **Error & Memory Leak Checking**
Checking for memory leaks can be done with [Valgrind](https://valgrind.org/). A target within the Makefile has been configured with relevant flags to execute Valgrind via running the command `$ make check`.

### **Handoff**
Our handoff video is located on YouTube [here](https://www.youtube.com/watch?v=bdTMnPUru6I).