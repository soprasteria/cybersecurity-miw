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

#ifndef MR_JOB_H
#define MR_JOB_H

#include <sys/mman.h>
#include <sched.h>
#include "application.hh"
#include "defsplitter.hh"
#include "bench.hh"
#ifdef JOS_USER
#include "wc-datafile.h"
#include <inc/sysprof.h>
#endif
#include "log_record.h"
#include "log_format.h"
#include <fstream>
#include <chrono>
#include <ctime>
#include "str_utils.h"
#include <glog/logging.h>

enum { with_value_modifier = 0 };

using namespace miw;

class mr_job : public map_reduce
{
 public:
 mr_job(const char *f, int nsplit,
	const std::string &app_name,
	log_format *lf,
	const bool &store_content, const bool &compressed, const bool &quiet, const bool &skip_header)
   : _app_name(app_name),_lf(lf),_store_content(store_content),
    _compressed(compressed),_quiet(quiet),_skip_header(skip_header)
  {
    defs_ = new defsplitter(f,nsplit);
  }
 mr_job(char *d, const size_t &size, int nsplit,
	const std::string &app_name,
	log_format *lf,
	const bool &store_content, const bool &compressed, const bool &quiet, const bool &skip_header)
   : _app_name(app_name),_lf(lf),
    _store_content(store_content),_compressed(compressed),_quiet(quiet),_skip_header(skip_header)
  {
    defs_ = new defsplitter(d,size,nsplit);
  }
  virtual ~mr_job()
    {
      if (defs_)
	delete defs_;
    }
  
  static void free_records(xarray<keyval_t> *wc_vals)
  {
    for (uint32_t i=0;i<wc_vals->size();i++)
      {
	log_record *lr = (log_record*)wc_vals->at(i)->val;
	delete lr;
      }
  }
  
  bool split(split_t *ma, int ncores) {
    return defs_->split(ma, ncores, "\n",0);
  }
  
  int key_compare(const void *s1, const void *s2) {
    return strcasecmp((const char *) s1, (const char *) s2);
  }
  
  void run_no_final(const int &nprocs, const int &reduce_tasks,
		    const int &quiet, const std::string output_format, const int &nfile,
		    int &ndisp, std::ofstream &fout, const std::string &ofname, const bool &tmp_save, const bool &newfile)
  {
    set_ncore(nprocs);
    set_reduce_task(reduce_tasks);
    sched_run_no_final();
    //std::cerr << "results size=" << get_reduce_bucket_manager()->rb0_size() << std::endl;
    xarray<keyval_t> *tmp_results = static_cast<reduce_bucket_manager<keyval_t>*>(get_reduce_bucket_manager())->get(0);
    print_top(tmp_results, ndisp);
    if (tmp_save && newfile)
      temp_state_save(output_format,tmp_results,ofname); // ability to store temporary state, e.g. in CSV form
  }
  
  void run(const int &nprocs, const int &reduce_tasks,
	   const int &quiet, const std::string output_format, const int &nfile,
	   int &ndisp, std::ofstream &fout, xarray<keyval_t> *results=nullptr)
  {
    set_ncore(nprocs);
    set_reduce_task(reduce_tasks);
    sched_run();
    print_top(&results_, ndisp);
    run_finalize(quiet, output_format, nfile, ndisp, fout, results);
  }

  void run_finalize(const int &quiet, const std::string &output_format,
		    const int &nfile, int &ndisp, std::ofstream &fout,
		    xarray<keyval_t> *results=nullptr)
  {
    /* get the number of results to display */
    //if (!quiet)
    //print_top(&results_, ndisp);
    print_stats();
    if (fout.is_open()) 
      {
	if (output_format == "json")
	  output_json(&results_,fout);
	else if (output_format == "csv")
	  output_csv(&results_,nfile,fout);
	else if (output_format.empty())
	  output_all(&results_,fout);
      }
    else if (output_format == "json")
      output_json(&results_,std::cout);
    else if (output_format == "mem")
      output_mem(&results_,results);
    free_records(&results_);
    free_results();
  }

  void temp_state_save(const std::string &output_format,
		       xarray<keyval_t> *results,
		       const std::string &ofname)
  {
    std::chrono::time_point<std::chrono::system_clock> now_tp = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now_tp);
    std::string now_date = std::ctime(&now_time);
    std::replace(now_date.begin(),now_date.end(),' ','_');
    std::replace(now_date.begin(),now_date.end(),'?','_');
    std::replace(now_date.begin(),now_date.end(),'\n','_');

    std::ofstream fout;
    std::vector<std::string> tmp_ofnamev;
    str_utils::str_split(ofname,'.',tmp_ofnamev);
    std::string tmp_ofname = tmp_ofnamev[0] + '_' + now_date + '.' + tmp_ofnamev[1];
    fout.open(tmp_ofname);
    if (!fout.is_open())
      {
	LOG(ERROR) << "unable to open temporary output file=" << tmp_ofname;
	return;
      }
    else
      {
	if (output_format == "csv")
	  {
	    output_csv(results,-1,fout);
	    LOG(INFO) << "temporary result saved in " << tmp_ofname;
	  }
      }
    fout.close();
  }

  // output functions
  void print_top(xarray<keyval_t> *wc_vals, int &ndisp);
  void output_all(xarray<keyval_t> *wc_vals, std::ostream &fout);
  void output_json(xarray<keyval_t> *wc_vals, std::ostream &fout);
  void output_csv(xarray<keyval_t> *wc_vals, const int &nfile, std::ostream &fout);
  void output_mem(xarray<keyval_t> *wc_vals, xarray<keyval_t> *results);
  
  // map reduce
  void map_function(split_t *ma);
  void reduce_function(void *key_in, void **vals_in, size_t vals_len);
  int combine_function(void *key_in, void **vals_in, size_t vals_len);
  
  void *modify_function(void *oldv, void *newv) {
    log_record *lr1 = (log_record*) oldv;
    log_record *lr2 = (log_record*) newv;
    //lr1->_sum += lr2->_sum;
    lr1->merge(lr2);
    delete lr2;
    return (void*)lr1;
   }
  
  void *key_copy(void *src, size_t s) {
    char *key = safe_malloc<char>(s + 1);
    memcpy(key, src, s);
    key[s] = 0;
    return key;
  }
  
  void key_free(void *k)
  {
    free(k);
  }
  
  int final_output_compare(const keyval_t *kv1, const keyval_t *kv2) {
#ifdef HADOOP
    return strcmp((char *) kv1->key_, (char *) kv2->key_);
#else
    /*if (alphanumeric)
      return strcmp((char *) kv1->key_, (char *) kv2->key_);*/
    size_t i1 = (size_t) kv1->val;
    size_t i2 = (size_t) kv2->val;
    if (i1 != i2)
      return i2 - i1;
    else
      return strcmp((char *) kv1->key_, (char *) kv2->key_);
#endif
  }
  
  bool has_value_modifier() const {
    return with_value_modifier;
  }

  void set_defs(const char *fname, const int &nsplit)
  {
    if (defs_)
      delete defs_;
    defs_ = new defsplitter(fname,nsplit);
  }

  void set_defs(char *d, const size_t &size, const int &nsplit)
  {
    if (defs_)
      delete defs_;
    defs_ = new defsplitter(d,size,nsplit);
  }
  
  //private:
  defsplitter *defs_ = nullptr;
  std::string _app_name;
  log_format *_lf = nullptr;
  bool _store_content = false;
  bool _compressed = false;
  bool _quiet = false;
  bool _skip_header = false;
};

#endif
