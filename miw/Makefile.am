protoc_inputs = log_definition.proto
protoc_outputs = log_definition.pb.cc log_definition.pb.h
$(protoc_outputs): $(protoc_inputs)
	protoc -I$(srcdir) --cpp_out=. $<

miwdir = $(libdir)/miw

AM_CXXFLAGS=-Wall -g -pipe -std=c++11 -fpermissive -fopenmp
AM_CPPFLAGS=`pkg-config --cflags protobuf`
miw_LTLIBRARIES=libmiw.la
libmiw_la_SOURCES=log_format.cc log_format.h \
		 log_record.cc log_record.h
nodist_libmiw_la_SOURCES=$(protoc_outputs)

clean-local:
	rm -f log_definition.pb.h log_definition.pb.cc

BUILT_SOURCES = $(protoc_outputs)

EXTRA_DIST = $(protoc_inputs)

SUBDIRS = . formats
