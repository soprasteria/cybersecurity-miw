import os, sys, json, log_definition_pb2
from pprint import pprint
import protobuf_json

msg = log_definition_pb2.logdef()
msg.format_name = "test"
msg.delims = " "

pprint(msg.SerializeToString())

json_obj=protobuf_json.pb2json(msg)
print json_obj
