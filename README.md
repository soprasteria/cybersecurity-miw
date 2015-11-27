## MIW
MIW is the shortname for Mobile Investivation Workstation. MIW is a tool for the fast summarization and analysis of very large quantities of logs. It is written in C++/C++11 and provides some small Python tooling.

MIW was built as an extremely efficient single-machine map-reduce alternative to Hadoop for processing hundreds of GB of logs. It is especially useful for on-premises log analysis, for generating condensed analytics out of raw logs, and for computing features useful in Machine Learning applications.

Though rather young in its development MIW has already been integrated several industrial projects and run over billions of logs, from laptops to HPC.

Main functionalities:
At the moment, MIW supports implements the following features:

- C++ map-reduce as an extension of Metis with support for multiple input files as input to the same task
- auto-splitting of files based on available RAM
- command line job launcher
- configurable input log and job control formats in JSON
- variety of outputs supported, from memory to JSON and CSV
- variety of configuration options for preprocessing common log fields such as dates, time, URLs, ...
- minimal Python job control utility

Dependencies:

- C++11 compiler + autotools
- [protocol buffers](https://developers.google.com/protocol-buffers/?hl=en) for configuration and storage;
- [glog](https://code.google.com/p/google-glog/) for logging events and debug;
- [gflags](https://code.google.com/p/gflags/) for command line parsing;
- [jsoncpp](https://github.com/open-source-parsers/jsoncpp) for JSON output;
- [gtest](https://code.google.com/p/googletest/) for unit testing (optional);
- [cppnetlib](http://cpp-netlib.org/) for preprocessing URIs;
- [snappy](http://google.github.io/snappy/) for log compression;
- [libcurl](http://curl.haxx.se/libcurl/) for connecting to external applications.

Implementation:

### Authors
MIW is designed and implemented by Emmanuel Benazera around the c++ map-reduce library Metis, on behalf of SopraSteria cybersecurity.

### Build
Below are instructions for Linux systems:

First, install dependencies
```
sudo apt-get install autotools-dev automake autoconf libtool pkg-config libprotobuf-dev protobuf-compiler python-protobuf libjsoncpp-dev libgoogle-glog-dev libgflags-dev libsnappy-dev libcurl4-openssl-dev
```

For compiling:
```
./autogen.sh
./configure
make
```

### Documentation

Using the main command line exe:
```
./app/miw --help
```
yields the list of options:
```
Flags from job.cc:
-appname (optional application name) type: string default: ""
-autosplit (whether to autosplit file based on available memory
	   type: bool default: false
	   -compressed (whether to compress the original content) type: bool
		        default: false
-fnames (comma-separated input file names) type: string default: ""
-format_name (processing format name) type: string default: ""
-map_tasks (number of map tasks (default = auto)) type: int32 default: 0
-memory_factor (heuristic value for autosplit of very large files,
		representing the expected memory requirement ratio vs the size of the
		file, e.g. 10 times more memory than log volume) type: double default: 10
-merge_results (whether to merge results over multiple input files)
	       type: bool default: false
-ndisp (number of top records to show) type: int32 default: 5
-nprocs (number of cores (default = auto)) type: int32 default: 0
-ofname (output file name) type: string default: ""
-output_format (output format (json, csv)) type: string default: ""
-quiet (quietness) type: bool default: true
-reduce_tasks (number of reduce tasks (default = auto)) type: int32 default: 0
-skip_header (whether to skip first log line file as header) type: bool default: false
-store_content (whether to store the original content in the processed output) type: bool default: false
-tmp_save (whether to save temporary output of results after each file is processed) type: bool default: false
```

Example:
```
./app/miw -fnames yourlogfile.log -format_name miw/formats/domain_controller_format -output_format csv -ofname test.csv
```

The above call uses an existing format file. However, this is unlikely the provided formats match your logs. To generate your own log format:

- Edit a json file in the manner of files in `miw/formats`
- Convert it into a proto-buffer:
```
python format_json2pb.py yourformatfile.json yourformatfile.fmt
```
- Use the format in a call:
./app/miw -fnames yourlogfile.log -format_name yourformatfile -output_format csv -ofname test.csv

Please note the omission of the `.fmt` extension.

### Log Formats

The log formats are described in JSON, see the examples in `miw/formats`. They basically describe a log as a CSV like format, in which each column can be processed through a set of operations, from basic `counts` to `aggregations`, preprocessing of `time`, `date`, `url`.

The best way to learn from the built-in possibilities at this point is to study the JSON files in `miw/formats`.

### Run tests

There are examples of unit tests in `tests/ut-mr-parsing.cc`. Edit the file as needed for using your own formats and logs and run:
```
make ut_mr_parsing
./ut_mr_parsing
```