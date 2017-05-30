TESTS = $(check_PROGRAMS)
check_PROGRAMS=ut_mr_parsing
bin_PROGRAMS=ut_mr_parsing
ut_mr_parsing_SOURCES=ut-mr-parsing.cc

MAXCPUS = $(shell grep -c processor /proc/cpuinfo)
AM_CXXFLAGS=-Wall -g -pipe -std=c++11 -fpermissive -fopenmp -O2 -g \
	     -I../miw -I../metis \
	     -fno-omit-frame-pointer -D_GNU_SOURCE -include ../config.h \
	     -DJTLS=__thread -DJSHARED_ATTR= \
	     -DJOS_CLINE=64 -DCACHE_LINE_SIZE=64 \
             -DJOS_NCPU=$(MAXCPUS) -D__STDC_FORMAT_MACROS `pkg-config --cflags protobuf`
AM_LDFLAGS=`pkg-config --libs protobuf` -L../miw/ -L../metis
LDADD=-lmiw -lmetis -lc -lm -lcurl -lidn -lz -lssl -lcrypto -lpthread -ldl -ljsoncpp -lrt -lprotobuf -lsnappy -lboost_system -lcppnetlib-uri $(GLOG_LIBS) $(GFLAGS_LIBS) -lgtest -lgtest_main
