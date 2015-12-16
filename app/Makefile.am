MAXCPUS = $(shell grep -c processor /proc/cpuinfo)
AM_CXXFLAGS=-Wall -g -pipe -std=c++11 -fpermissive -fopenmp -O2 -g \
	     -I../miw -I../metis \
	     -fno-omit-frame-pointer -D_GNU_SOURCE -include ../config.h \
	     -DJTLS=__thread -DJSHARED_ATTR= \
	     -DJOS_CLINE=64 -DCACHE_LINE_SIZE=64 \
             -DJOS_NCPU=$(MAXCPUS) -D__STDC_FORMAT_MACROS `pkg-config --cflags protobuf`
AM_LDFLAGS=`pkg-config --libs protobuf` -L../miw/ -L../metis
LDADD=curl_mget.o -lmiw -lmetis -lc -lm -lcurl -lz -lssl -lcrypto -lpthread -ldl -ljsoncpp -lrt -lprotobuf -lsnappy $(GLOG_LIBS) $(GFLAGS_LIBS) -lboost_system -lcppnetlib-uri
bin_PROGRAMS=kmeans matrix_mult pca wc wr linear_regression hist string_match wrmem matrix_mult2 sf_sample btree_unit search_unit misc solr_commit miw simple_log_compacter

kmeans_SOURCES=kmeans.cc
matrix_mult_SOURCES=matrix_mult.cc
pca_SOURCES=pca.cc
wc_SOURCES=wc.cc
wr_SOURCES=wr.cc
linear_regression_SOURCES=linear_regression.cc
hist_SOURCES=hist.cc
string_match_SOURCES=string_match.cc
wrmem_SOURCES=wrmem.cc
matrix_mult2_SOURCES=matrix_mult2.cc
sf_sample_SOURCES=../metis/micro/sf_sample.cc
btree_unit_SOURCES=../metis/micro/btree_unit.cc
search_unit_SOURCES=../metis/micro/search_unit.cc
misc_SOURCES=../metis/micro/misc.cc
bluecoat_log_proc_SOURCES=bluecoat_log_proc.cc
log_compacter_SOURCES=log_compacter.cc
solr_commit_SOURCES=solr_commit.cc
miw_SOURCES=miw.cc
simple_log_compacter_SOURCES=simple_log_compacter.cc
