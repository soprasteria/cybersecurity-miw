import os, sys, simplejson, log_definition_pb2
from pprint import pprint
import protobuf_json, protobuf_json_writer

if len(sys.argv) < 3:
    print "Usage: " + sys.argv[0] + " <json file in> <pb file out>"
    exit()

f = open(sys.argv[1],'r')
json_str = simplejson.loads(f.read())
f.close()
msg_pb = log_definition_pb2.logdef()    
json_pb = protobuf_json.json2pb(msg_pb,json_str)

#print protobuf_json_writer.proto2json(json_pb)

fo = open(sys.argv[2],'w')
fo.write(json_pb.SerializeToString())
fo.close()
