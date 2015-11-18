metisdir = $(libdir)/metis
metis_LIBRARIES=libmetis.a
MAXCPUS := $(shell grep -c processor /proc/cpuinfo)
AM_CXXFLAGS=-Wall -g -pipe -std=c++11 -fpermissive -fopenmp -g -O2 @OPT_LEVEL@ \
	     -fno-omit-frame-pointer -D_GNU_SOURCE -include ../config.h \
	     -DJTLS=__thread -DJSHARED_ATTR= \
	     -DJOS_CLINE=64 -DCACHE_LINE_SIZE=64 \
             -DJOS_NCPU=$(MAXCPUS) -D__STDC_FORMAT_MACROS
AM_CPPFLAGS=-Imicro 
libmetis_a_SOURCES= pthreadpool.cc profile.cc ibs.cc cpumap.cc mr-types.cc application.cc threadinfo.cc
