
AUTOMAKE_OPTIONS = foreign

STD=c++11
GDB=-g
GPROF=-pg
WARN=-Wall

AM_CXXFLAGS = -std=$(STD) $(GDB) $(GPROF) $(WARN)

lib_LIBRARIES = libcldb.a
libcldb_a_SOURCES = btree.cpp btree.h cache.cpp cache.h \
	cldb.cpp cldb.h common.h file.cpp file.h mempool.cpp \
	mempool.h storage.cpp storage.h
