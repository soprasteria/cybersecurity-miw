#AM_CXXFLAGS=-Wall -g -pipe -std=c++11 -fpermissive
#AM_CPPFLAGS=-I${srcdir}/../ `pkg-config --cflags protobuf`
#AM_LDFLAGS=-L$(top_builddir)/miw/ `pkg-config --libs protobuf`
#LDADD = -lmiw

#bin_PROGRAMS=generate_domain_controller generate_anon_bluecoat

#generate_domain_controller_SOURCES=generate_domain_controller.cc
#generate_anon_bluecoat_SOURCES=generate_anon_bluecoat.cc

protoc_inputs = ../log_definition.proto
protoc_outputs = ../log_definition_pb2.py
$(protoc_outputs): $(protoc_inputs)
	protoc --proto_path ../ -I$(srcdir) --python_out=. $<

all: $(protoc_outputs)
