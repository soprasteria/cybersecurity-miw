/**
 * Copyright (c) 2015 SopraSteria
 * All rights reserved.
 * Author: Emmanuel Benazera <emmanuel.benazera@deepdetect.com>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of SopraSteria nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY SOPRASTERIA ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL SOPRASTERIA BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "job.h"
#include <chrono>
#include <sys/sysinfo.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(fnames,"","comma-separated input file names");
DEFINE_int32(nprocs,0,"number of cores (default = auto)");
DEFINE_int32(ndisp,5,"number of top records to show");
DEFINE_int32(map_tasks,0,"number of map tasks (default = auto)");
DEFINE_int32(reduce_tasks,0,"number of reduce tasks (default = auto)");
DEFINE_bool(quiet,true,"quietness");
DEFINE_bool(autosplit,false,"whether to autosplit file based on available memory");
DEFINE_string(ofname,"","output file name");
DEFINE_string(format_name,"","processing format name");
DEFINE_string(appname,"","optional application name");
DEFINE_string(output_format,"","output format (json, csv)");
DEFINE_bool(store_content,false,"whether to store the original content in the processed output");
DEFINE_bool(compressed,false,"whether to compress the original content");
DEFINE_bool(merge_results,false,"whether to merge results over multiple input files");
DEFINE_double(memory_factor,10.0,"heuristic value for autosplit of very large files, representing the expected memory requirement ratio vs the size of the file, e.g. 10 times more memory than log volume");
DEFINE_bool(skip_header,false,"whether to skip first log line file as header");
DEFINE_bool(tmp_save,false,"whether to save temporary output of results after each file is processed");

namespace miw
{    

  bool job::_glog_init = false;
  
  void job::glog_init(char *argv[])
  {
    if (!job::_glog_init)
      {
	google::InitGoogleLogging(argv[0]);
	google::SetLogDestination(google::INFO,"");
	job::_glog_init = true;
      }
  }
  
int job::execute(int argc, char *argv[])
  {
    google::ParseCommandLineFlags(&argc,&argv,true);
    glog_init(argv);
    FLAGS_logtostderr = 1;
    str_utils::str_split(FLAGS_fnames,',',_files);
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
    _output_format = FLAGS_output_format;
    _merge_results = FLAGS_merge_results;
    _in_memory_factor = FLAGS_memory_factor;
    _skip_header = FLAGS_skip_header;
    _tmp_save = FLAGS_tmp_save;
    
    // list input files
    std::cerr << "files=" << FLAGS_fnames << std::endl;

    
    // if in memory results, allocate the final object
    if (_output_format == "mem")
      _results = new xarray<keyval_t>();
    else
      {
	//TODO: open temporary output file if option is on
	// open output file
	_fout.open(_ofname);
	if (!_fout.is_open())
	  {
	    LOG(ERROR) << "unable to open output file=" << _ofname;
	    return 1;
	  }
      }

    // open processing format file
    if (_lf.read(_format_name) < 0)
      {
	LOG(ERROR) << "Error opening the log format file";
	return 1;
      }

    return execute();
  }
  
  int job::execute()
  {
    LOG(INFO) << "files size=" << _files.size();
    std::chrono::time_point<std::chrono::system_clock> tstart = std::chrono::system_clock::now();
    for (size_t j=0;j<_files.size();j++)
      {
	std::string fname = _files.at(j);
	LOG(INFO) << "Processing file=" << fname;
	struct stat st;
	if (stat(fname.c_str(),&st)!=0)
	  {
	    LOG(ERROR) << "Error file not found: " << fname;
	    return 1;
	  }
	if (!_autosplit && !_merge_results)
	  {
	    run_mr_job(fname.c_str(),j);
	  }
	else if (!_autosplit && _merge_results)
	  {
	    run_mr_job_merge_results(fname.c_str(),j,j==_files.size()-1,true);
	  }
	else
	  {
	    size_t mfsize = 0;
	    size_t nchunks = 1;
	    bool do_autosplit = file_size_autosplit(st.st_size,mfsize,nchunks);
	    LOG(INFO) << "do_autosplit: " << do_autosplit << std::endl;
	    if (do_autosplit)
	      {
		LOG(INFO) << "Working on " << nchunks << " splitted chunks of " << mfsize << " bytes\n";
		std::ifstream fin(fname);
		for (size_t ch=0;ch<nchunks;ch++)
		  {
		    bool run_end = (j == _files.size()-1) && (ch == nchunks-1);
		    std::string line,buf;
		    while(buf.length() < mfsize)
		      {
			if (!std::getline(fin,line))
			  break;
			buf += line + "\n";
		      }
		    LOG(INFO) << "--> Chunk #" << ch+1 << " / " << nchunks;
		    if (!_merge_results)
		      run_mr_job(const_cast<char*>(buf.c_str()),j,buf.length());
		    else run_mr_job_merge_results(const_cast<char*>(buf.c_str()),j+ch,run_end,buf.length(),ch==0);
		  }
	      }
	    else
	      {
		run_mr_job(fname.c_str(),j);
	      }
	  }
      }
    if (_fout.is_open())
      _fout.close();

    // final timing
    std::chrono::time_point<std::chrono::system_clock> tstop = std::chrono::system_clock::now();
    double duration = std::chrono::duration_cast<std::chrono::seconds>(tstop-tstart).count();
    LOG(INFO) << "MR duration=" << duration << " seconds\n";
    
    return 0;
  }

void job::run_mr_job(const char *fname, const int &nfile, const size_t &blength)
{
  mapreduce_appbase::initialize();
  if (blength) // from buffer
    _mrj = new mr_job(const_cast<char*>(fname),blength, _map_tasks, _app_name, &_lf, _store_content, _compressed, _quiet, _skip_header);
  else _mrj = new mr_job(fname, _map_tasks, _app_name, &_lf, _store_content, _compressed, _quiet, _skip_header);
  _mrj->run(_nprocs,_reduce_tasks,_quiet,_output_format,nfile,_ndisp,_fout,_results);
  delete _mrj;
  _mrj = nullptr;
  mapreduce_appbase::deinitialize();
}

void job::run_mr_job_merge_results(const char *fname, const int &nfile,
				   const bool &run_end, const size_t &blength,
				   const bool &newfile)
{
  if (nfile == 0)
    {
      mapreduce_appbase::initialize();
      if (blength > 0) // from buffer
	_mrj = new mr_job(const_cast<char*>(fname),blength, _map_tasks, _app_name, &_lf, _store_content, _compressed, _quiet, _skip_header);
      else _mrj = new mr_job(fname, _map_tasks, _app_name, &_lf, _store_content, _compressed, _quiet, _skip_header);
    }
  else
    {
      if (blength > 0)
	_mrj->set_defs(const_cast<char*>(fname),blength,_map_tasks);
      else _mrj->set_defs(fname,_map_tasks);
    }
  
  _mrj->run_no_final(_nprocs,_reduce_tasks,_quiet,_output_format,nfile,_ndisp,_fout,_ofname,_tmp_save,newfile);

  if (run_end)
    {
      _mrj->set_final_result();
      _mrj->reset();
      _mrj->run_finalize(_quiet,_output_format,-1,_ndisp,_fout);
      delete _mrj;
      _mrj = nullptr;
      mapreduce_appbase::deinitialize();
    }
}

    unsigned long job::get_available_memory()
    {
      size_t avail_mem;

      // On recent kernel version (>= 3.14) available memory (free + cache) is available in /proc/meminfo
      std::string line;
      bool found_with_new_method = false;
      std::ifstream meminfo("/proc/meminfo");
      while (meminfo.good() && !meminfo.eof()) {
	getline(meminfo, line);
	if (line.find("MemAvailable:") != std::string::npos) {
	  avail_mem = atoll(line.substr(strlen("MemAvailable:")).c_str()) * 1024;
	  found_with_new_method = true;
	  break;
	}
      }
      meminfo.close();

      if (!found_with_new_method) {
	avail_mem = sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE);
      }
      //std::cerr << "SC_AVPHY=" << sysconf(_SC_AVPHYS_PAGES) << " / PAGESIZE=" << sysconf(_SC_PAGESIZE) << std::endl;
      struct sysinfo sys_info;
      if (sysinfo(&sys_info) != 0)
	{
	  LOG(ERROR) << "[Warning]: could not access sysinfo data\n";
	  return avail_mem;
	}

      //std::cerr << "total swap: " << sys_info.totalswap << " -- freeswap: " << sys_info.freeswap << std::endl;
      unsigned long freeram = sys_info.freeram;
      #ifdef DEBUG
      int used_swap = sys_info.totalswap - sys_info.freeswap;
      LOG(INFO) << "avail mem: " << avail_mem << " -- totalram: " << sys_info.totalram << " -- freeram: " << freeram << " -- used_swap: " << used_swap << " -- avail_mem: " << freeram - used_swap << std::endl;
      #endif
      if (found_with_new_method) {
	return avail_mem;
      }
      return freeram; // - used_swap; // Deactivated: avail memory does not take used swap into accounts, so we're subtracting it here.
    }
    
  bool job::file_size_autosplit(const size_t &fs,
				size_t &mfsize,
				size_t &nchunks)
  {
    if (_nchunks_split == 0)
      {
	unsigned long ms = get_available_memory();
	LOG(INFO) << "available memory: " << ms << " -- file size: " << fs << std::endl;
	/*if (fs < ms)
	  {
	    std::cerr << "autosplit not needed\n";
	    mfsize = ms;
	    return false;
	    }*/
	nchunks = static_cast<unsigned long>(ceil(fs / ((1.0/_in_memory_factor)*ms)));
      }
    else nchunks = _nchunks_split;
    mfsize = (size_t)ceil(fs / (double)nchunks);
    LOG(INFO) << "max file size: " << mfsize << std::endl;
    return true;
  }
    
}
