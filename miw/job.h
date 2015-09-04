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

#ifndef MIW_JOB_H
#define MIW_JOB_H

#include "log_format.h"
#include "mr_job.h"
//#include <sys/mman.h>
//#include "application.hh"
//#include "defsplitter.hh"
//#include "bench.hh"
#include <fstream>

//#define DEFAULT_NDISP 10
//#define DEBUG

using namespace miw;

//namespace miw
//{

  class job
  {
  public:
    job() {}
    ~job() {}

    // memory management
    unsigned long get_available_memory();

    // input management
    bool file_size_autosplit(const size_t &fs,
			     size_t &mfsize,
			     size_t &nchunks);
    
    // output functions
    /*void print_top(xarray<keyval_t> *wc_vals, const int &ndisp);
    void output_all(xarray<keyval_t> *wc_vals, std::ostream &fout);
    void output_json(xarray<keyval_t> *wc_vals, std::ostream &fout);*/

    int execute();
    int execute(int argc, char *argv[]);

    void run_mr_job(const char *fname, const int &nfile, const size_t &blength=0);
    void run_mr_job_merge_results(const char *fname, const int &nfile, const bool &run_end, const size_t &blength=0);
    
    long _skipped_logs = 0;
    log_format _lf;
    std::ofstream _fout; /**< output file stream */
    
    // options
    std::string _app_name;
    bool _store_content = false; // whether to store full content into index.
    bool _compressed = false; // whether to compress the original content while working on it.
    bool _autosplit = false; // whether to split input files based on heuristic of memory-usage.
    bool _merge_results = false; // whether to merge results over multiple inputop
    int _nchunks_split = 0;
    double _in_memory_factor = 10; // we expect to use at max 10 times more memory than log volume, for processing them. Very conservative value, used in auto-splitting the log files before processing them.
    std::string _output_format; // other values: json, csv
    bool _quiet = 0;

    int _nprocs = 0; /**< number of used processors, when specified */
    int _map_tasks = 0; /**< number of map tasks, when specified */
    int _reduce_tasks = 0; /**< number of reduce tasks, when specified */
    int _ndisp = 0; /**< number of top entries to show */
    std::string _format_name;
    
    // map reduce inner job object
    mr_job *_mrj = nullptr;
    std::vector<std::string> _files; /**< list of data file names */
    std::string _ofname; /**< output file name */
  };
  
//}

#endif
