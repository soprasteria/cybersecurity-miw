/**
 * Copyright (c) 2015 SopraSteria
 * All rights reserved.
 * Author: Emmanuel Benazera <beniz@droidnik.fr>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of XPLR Software Inc nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY XPLR SOFTWARE INC ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL XPLR SOFTWARE INC BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "job.h"
#include <sys/sysinfo.h>
#include <gflags/gflags.h>

DEFINE_string(fnames,"","comma-separated input file names");
DEFINE_int32(nprocs,0,"number of cores (default = auto)");
DEFINE_int32(ndisp,0,"number of top records to show");
DEFINE_int32(map_tasks,0,"number of map tasks (default = auto)");
DEFINE_int32(reduce_tasks,0,"number of reduce tasks (default = auto)");
DEFINE_bool(quiet,true,"quietness");
DEFINE_bool(autosplit,false,"whether to autosplit file based on available memory (experimental)");
DEFINE_string(ofname,"","output file name");
DEFINE_string(format_name,"","processing format name");
DEFINE_string(appname,"","optional application name");
DEFINE_bool(store_content,false,"whether to store the original content in the processed output");
DEFINE_bool(compressed,false,"whether to compress the original content");

std::vector<std::string>& str_split(const std::string &s, char delim, std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

//namespace miw
//{    

int job::execute(int argc, char *argv[])
  {
    google::ParseCommandLineFlags(&argc,&argv,true);
    str_split(FLAGS_fnames,',',_files);
    _nprocs = FLAGS_nprocs;
    _ndisp = FLAGS_ndisp;
    _map_tasks = FLAGS_map_tasks;
    _reduce_tasks = FLAGS_reduce_tasks;
    _quiet = FLAGS_quiet;
    _autosplit = FLAGS_autosplit;
    _ofname = FLAGS_ofname;
    _format_name = FLAGS_format_name;
    _app_name = FLAGS_appname;
    _store_content = FLAGS_store_content;
    _compressed = FLAGS_compressed;

    // list input files
    
    // open output file
    _fout.open(_ofname);
    if (!_fout.is_open())
      {
	std::cerr << "unable to open input file=" << _ofname << std::endl;
	return 1;
      }
    
    // open processing format file
    if (_lf.read(_format_name) < 0)
      {
	std::cerr << "Error opening the log format file\n";
	return 1;
      }
    
    return execute();
  }
  
  int job::execute()
  {
    for (size_t j=0;j<_files.size();j++)
      {
	std::string fname = _files.at(j);
	std::cerr << "Processing file=" << fname << std::endl;
	struct stat st;
	if (stat(fname.c_str(),&st)!=0)
	  {
	    std::cerr << "[Error] file not found: " << fname << std::endl;
	    continue;
	  }
	if (!_autosplit)
	  {
	    mapreduce_appbase::initialize();
	    _mrj = new mr_job(fname.c_str(), _map_tasks);
	    _mrj->run(_nprocs,_reduce_tasks,_quiet,_json_output,_ndisp,_fout);
	    delete _mrj;
	    _mrj = nullptr;
	    mapreduce_appbase::deinitialize();
	  }
	else
	  {
	    size_t mfsize = 0;
	    size_t nchunks = 1;
	    bool do_autosplit = file_size_autosplit(st.st_size,mfsize,nchunks);
	    std::cerr << "do_autosplit: " << do_autosplit << std::endl;
	    if (do_autosplit)
	      {
		std::cout << "Working on " << nchunks << " splitted chunks of " << mfsize << " bytes\n";
		std::ifstream fin(fname);
		//int read_count;
		//char *buf = new char[mfsize];
		for (size_t ch=0;ch<nchunks;ch++)
		  {
		    std::string line,buf;
		    while(std::getline(fin,line)
			  && buf.length() < mfsize)
		      buf += line + "\n";
		      std::cout << "--> Chunk #" << ch+1 << " / " << nchunks << std::endl;
		    mapreduce_appbase::initialize();
		    _mrj = new mr_job(const_cast<char*>(buf.c_str()),buf.length(),_map_tasks);
		    _mrj->run(_nprocs,_reduce_tasks,_quiet,_json_output,_ndisp,_fout);
		    delete _mrj;
		    _mrj = nullptr;
		    mapreduce_appbase::deinitialize();
		  }
		  //delete[] buf;
	      }
	    else
	      {
		mapreduce_appbase::initialize();
		_mrj = new mr_job(fname.c_str(), _map_tasks);
		_mrj->run(_nprocs,_reduce_tasks,_quiet,_json_output,_ndisp,_fout);
		delete _mrj;
		_mrj = nullptr;
		mapreduce_appbase::deinitialize();
	      }
	  }
      }
    return 0;
  }

    size_t job::get_available_memory()
    {
      size_t avail_mem = sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE);
      struct sysinfo sys_info;
      if (sysinfo(&sys_info) != 0)
	{
	  std::cerr << "[Warning]: could not access sysinfo data\n";
	  return avail_mem;
	}
      //std::cerr << "total swap: " << sys_info.totalswap << " -- freeswap: " << sys_info.freeswap << std::endl;
      int freeram = sys_info.freeram;
#ifdef DEBUG
      size_t used_swap = sys_info.totalswap - sys_info.freeswap;
      std::cerr << "avail mem: " << avail_mem << " -- totalram: " << sys_info.totalram << " -- freeram: " << freeram << " -- used_swap: " << used_swap << " -- avail_mem: " << freeram - used_swap << std::endl;
#endif
      return freeram; // - used_swap; // Deactivated: avail memory does not take used swap into accounts, so we're subtracting it here.
    }
    
  bool job::file_size_autosplit(const size_t &fs,
				size_t &mfsize,
				size_t &nchunks)
  {
    if (_nchunks_split == 0)
      {
	size_t ms = get_available_memory();
	std::cerr << "available memory: " << ms << " -- file size: " << fs << std::endl;
	if (fs < ms)
	  {
	    std::cerr << "autosplit not needed\n";
	    mfsize = ms;
	    return false;
	  }
	nchunks = (size_t)ceil(fs / ((1.0/_in_memory_factor)*ms));
      }
    else nchunks = _nchunks_split;
    mfsize = (size_t)ceil(fs / (double)nchunks);
    std::cerr << "max file size: " << mfsize << std::endl;
    return true;
  }
    
//}
