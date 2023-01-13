# Makefile
# Justin Thoreson, Mason Adsero
# Seattle University, CPSC5300, Winter 2023

CCFLAGS 		= -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c
COURSE 			= /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR 		= $(COURSE)/lib
OBJS				= sql5300.o

# General rule for compilation
%.o : %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o $@ $<

# Rule for linking to create executable
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser

# Rule for removing all non-source files
clean : 
	rm -f *.o sql5300
