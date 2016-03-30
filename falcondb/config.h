#ifndef FDB_CONFIG_H
#define FDB_CONFIG_H



#if defined(USE_TCMALLOC)
#include <gperftools/tcmalloc.h>
#define USE_TCMALLOC 1
#endif

#if defined(USE_JEMALLOC)
#include <jemalloc/jemalloc.h>
#define USE_JEMALLOC 1
#endif


/* test for backtrace() */
#if defined(__APPLE__) || defined(__linux__)
#define HAVE_BACKTRACE 1
#endif


#endif //FDB_CONFIG_H
