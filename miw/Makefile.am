protoc_inputs = log_definition.proto
protoc_outputs = log_definition.pb.cc log_definition.pb.h
$(protoc_outputs): $(protoc_inputs)
	protoc -I$(srcdir) --cpp_out=. $<

miwdir = $(libdir)/miw

MAXCPUS = $(shell grep -c processor /proc/cpuinfo)
AM_CXXFLAGS=-Wall -g -pipe -std=c++11 -fpermissive -fopenmp -O2 \
 	     -I../metis \
	     -fno-omit-frame-pointer -D_GNU_SOURCE -include ../config.h \
	     -DJTLS=__thread -DJSHARED_ATTR= \
	     -DJOS_CLINE=64 -DCACHE_LINE_SIZE=64 \
             -DJOS_NCPU=$(MAXCPUS) -D__STDC_FORMAT_MACROS
AM_CPPFLAGS=`pkg-config --cflags protobuf`
miw_LTLIBRARIES=libmiw.la
libmiw_la_SOURCES=log_format.cc log_format.h \
		 log_record.cc log_record.h mr_job.cc mr_job.h job.cc job.h str_utils.h
nodist_libmiw_la_SOURCES=$(protoc_outputs)

clean-local:
	rm -f log_definition.pb.h log_definition.pb.cc

BUILT_SOURCES = $(protoc_outputs)

EXTRA_DIST = $(protoc_inputs)

SUBDIRS = . formats
