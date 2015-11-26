/**
 * Copyright (c) 2013-2015 SopraSteria
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

/**
 * Log preprocessing and compacting tool.
 */

#include "log_format.h"

#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <strings.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/sysinfo.h>
#include <sched.h>
#include "application.hh"
#include "defsplitter.hh"
#include "bench.hh"
#ifdef JOS_USER
#include "wc-datafile.h"
#include <inc/sysprof.h>
#endif
#include <iostream>
#include <sstream>
#include <fstream>
#include <map>
#include <algorithm>
#include <jsoncpp/json/json.h>
#include <climits>
#include "MurmurHash3.h"

using namespace miw;

#define DEFAULT_NDISP 10
//#define DEBUG

enum { with_value_modifier = 0 };

//static unsigned int seed = 4294967291UL; // prime.
static long skipped_logs = 0;
static log_format lf;
static std::string app_name;
static bool store_content = false; // whether to store full content into index.
static bool compressed = false; // whether to compress the original content while working on it.
static bool autosplit = false; // whether to split input files based on heuristic of memory-usage.
static int nchunks_split = 0;
static double in_memory_factor = 3; // we expect to use at max 10 times more memory than log volume, for processing them. Very conservative value, used in auto-splitting the log files before processing them.
static int quiet = 0;

/*template <class T>
static std::string to_string (const T& t)
{
  std::stringstream ss;
  ss << t;
  return ss.str();
  };*/

 /* N[0] - contains least significant bits, N[3] - most significant */
/*char* Bin128ToDec(const uint32_t N[4])

void parse_url_host_and_path(const std::string &url,
			     std::string &host, std::string &path)
{
  size_t p1 = 0;
  if ((p1=url.find("http://"))!=std::string::npos)
    p1 += 7;
  else if ((p1=url.find("https://"))!=std::string::npos)
    p1 += 8;
  else if ((p1=url.find("tcp://"))!=std::string::npos)
    p1 += 6;
  else if ((p1=url.find("ftp://"))!=std::string::npos)
    p1 += 6;
  else if (p1 == std::string::npos) // malformed url.
    {
      std::cerr << "malformed url: " << url << std::endl;
      host = "";
      path = "";
      return;
    }
  size_t p2 = 0;
  if ((p2 = url.find("/",p1))!=std::string::npos)
    {
      try
	{
	  host  = url.substr(p1,p2-p1);
	}
      catch (std::exception &e)
	{
	  host = "";
	}
      try
	{
	  path = url.substr(p2);
	}
      catch (std::exception &e)
	{
	  path = "";
	}
    }
  else
    {
      host = url.substr(p1);
      path = "";
    }
}

void free_records(xarray<keyval_t> *wc_vals)
{
  for (uint32_t i=0;i<wc_vals->size();i++)
    {
      log_record *lr = (log_record*)wc_vals->at(i)->val;
      delete lr;
    }
}

static void print_top(xarray<keyval_t> *wc_vals, int ndisp) {
    size_t occurs = 0;
    std::multimap<long,std::string,std::greater<long> > ordered_records;
    std::multimap<long,std::string,std::greater<long> >::iterator mit;
    for (uint32_t i = 0; i < wc_vals->size(); i++)
      {
	log_record *lr = (log_record*)wc_vals->at(i)->val;
	occurs += lr->_sum;
	ordered_records.insert(std::pair<long,std::string>(lr->_sum,lr->key()));
	if ((int)ordered_records.size() > ndisp)
	  {
	    mit = ordered_records.end();
	    mit--;
	    ordered_records.erase(mit);
	  }
      }
    printf("\nlogs preprocessing: results (TOP %d from %zu keys, %zd logs):\n",
           ndisp, wc_vals->size(), occurs);
#ifdef HADOOP
    ndisp = wc_vals->size();
#else
    ndisp = std::min(ndisp, (int)wc_vals->size());
#endif
    int c = 0;
    mit = ordered_records.begin();
    while(mit!=ordered_records.end())
      {
	printf("%45s - %ld\n",(*mit).second.c_str(),(*mit).first);
	++mit;
	if (c++ == ndisp)
	  break;
      }
    std::cout << std::endl;
}

static void output_all(xarray<keyval_t> *wc_vals, std::ostream &fout) 
{
  for (uint32_t i = 0; i < wc_vals->size(); i++) 
    {
      keyval_t *w = wc_vals->at(i);
      fout << (char*)w->key_ << " - " << static_cast<log_record*>(w->val)->_sum << std::endl;
    }
}

static void output_json(xarray<keyval_t> *wc_vals, std::ostream &fout)
{
  Json::FastWriter writer;
  for (uint32_t i = 0; i < wc_vals->size(); i++) 
    {
      log_record *lr = (log_record*)wc_vals->at(i)->val;
      Json::Value jrec;
      lr->to_json(jrec);
      if (!compressed)
	lr->flatten_lines();
      else
	{
	  lr->_uncompressed_lines = log_record::uncompress_log_lines(lr->_compressed_lines);
	  lr->_compressed_lines.clear();
	  lr->_original_size = lr->_uncompressed_lines.length();
	}
      fout << writer.write(jrec);
      if (!lr->_uncompressed_lines.empty())
	{
	  Json::Value jrecc;
	  jrecc["id"] = lr->key() + "_content";
	  jrecc["original_size"] = lr->_original_size;
	  jrecc["content"]["add"] = lr->_uncompressed_lines;
	  //fout << "{\"compressed_size\":" << lr->_compressed_size << ",\"content\":{\"add\":\"" << lr->_compressed_lines << "\"},\"id\":" << lr->key()+"_content" << ",\"original_size\":" << lr->_original_size << "}\n";
	  fout << writer.write(jrecc);
	}
    }
}

size_t get_available_memory()
{
  size_t avail_mem = sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE);
  struct sysinfo sys_info;
  if (sysinfo(&sys_info) != 0)
    {
      std::cerr << "[Warning]: could not access sysinfo data\n";
      return avail_mem;
    }
  //std::cerr << "total swap: " << sys_info.totalswap << " -- freeswap: " << sys_info.freeswap << std::endl;
  size_t used_swap = sys_info.totalswap - sys_info.freeswap;
  int freeram = sys_info.freeram;
#ifdef DEBUG
  std::cerr << "avail mem: " << avail_mem << " -- totalram: " << sys_info.totalram << " -- freeram: " << freeram << " -- used_swap: " << used_swap << " -- avail_mem: " << freeram - used_swap << std::endl;
#endif
  return freeram; // - used_swap; // Deactivated: avail memory does not take used swap into accounts, so we're subtracting it here.
}

/*size_t count_file_lines(const char *fn)
{
  std::cerr << "Autosplit: checking for number of lines in file " << fn;
  std::ifstream inf(fn);
  size_t nlines = std::count(std::istreambuf_iterator<char>(inf),
			     std::istreambuf_iterator<char>(),'\n');
  std::cout << " -- found " << nlines << " lines\n";
  return nlines;
  }*/

bool file_size_autosplit(const size_t &fs,
			 const char *fn,
			 size_t &mfsize,
			 size_t &nchunks)
  {
    if (nchunks_split == 0)
      {
	size_t ms = get_available_memory();
	std::cerr << "available memory: " << ms << " -- file size: " << fs << std::endl;
	if (fs < ms)
	  {
	    std::cerr << "autosplit not needed\n";
	    mfsize = ms;
	    return false;
	  }
	nchunks = (size_t)ceil(fs / ((1.0/in_memory_factor)*ms));
      }
    else nchunks = nchunks_split;
    mfsize = (size_t)ceil(fs / (double)nchunks);
    std::cerr << "max file size: " << mfsize << std::endl;
    return true;
  }

class bl : public map_reduce {
public:
  bl(const char *f, int nsplit) : s_(f, nsplit) {}
  bl(char *d, const size_t &size, int nsplit) : s_(d,size,nsplit) {}
  ~bl() {};

  bool split(split_t *ma, int ncores) {
    return s_.split(ma, ncores, "\n",0);
  }

  int key_compare(const void *s1, const void *s2) {
    return  strcasecmp((const char *) s1, (const char *) s2);
  }

  void run(const int &nprocs, const int &reduce_tasks,
	   const int &quiet, const int &json_output, const int &ndisp,
	   std::ofstream &fout)
  {
    set_ncore(nprocs);
    set_reduce_task(reduce_tasks);
    sched_run();
    print_stats();
    
    /* get the number of results to display */
    //if (!quiet)
    print_top(&results_, ndisp);
    if (fout.is_open()) 
      {
	if (json_output)
	  output_json(&results_,fout);
	else output_all(&results_,fout);
      }
    else if (json_output)
      output_json(&results_,std::cout);
    free_records(&results_);
    free_results();
  }

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
    char *key = static_cast<char*>(src);//strdup((char*)src);
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

private:
  defsplitter s_;
  };

void bl::map_function(split_t *ma)
{
  std::vector<log_record*> log_records;
  std::string dat = (char*)ma->data;
  lf.parse_data(dat,ma->length,app_name,store_content,compressed,quiet,0,false,log_records);
  
#ifdef DEBUG
  std::cout << "number of mapped records: " << log_records.size() << std::endl;
#endif
  for (size_t i=0;i<log_records.size();i++)
    {
      log_records.at(i)->_sum = 1;
      std::string key = log_records.at(i)->key();
      const char *key_str = key.c_str();
      map_emit((void*)key_str,(void*)log_records.at(i),strlen(key_str));
    }
}

int bl::combine_function(void *key_in, void **vals_in, size_t vals_len)
{
  log_record **lrecords = (log_record**)vals_in;
  for (uint32_t i=1;i<vals_len;i++)
    {
      lrecords[0]->merge(lrecords[i]);
      delete lrecords[i];
    }
  return 1;
}

void bl::reduce_function(void *key_in, void **vals_in, size_t vals_len)
{
  log_record **lrecords = (log_record**)vals_in;
  for (uint32_t i=1;i<vals_len;i++)
    {
      lrecords[0]->merge(lrecords[i]);
      delete lrecords[i];
    }
  reduce_emit(key_in,(void*)lrecords[0]);
}

static void usage(char *prog) {
    printf("usage: %s <filenames> [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -o filename : save output to a file\n");
    printf("  -j : output format is JSON (for solr indexing)\n");
    printf("  -f : select log format\n");
    printf("  -d : include original content in JSON for storage in index\n");
    printf("  -n #appname : application name for tagging records\n");
    printf("  -c : compress output (only applicable with -d)\n");
    printf("  -a : auto-split the input files (0 for auto, n > 0 otherwise)\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) 
{    
    int nprocs = 0, map_tasks = 0, ndisp = 5, reduce_tasks = 0;
    int json_output = 0;
    int c;
    std::string lformat_name = "default_log_format";  //TODO: replace by true default or make mandatory
    if (argc < 2)
	usage(argv[0]);
    std::vector<std::string> files;
    int i = 0;
    while (argv[++i] && argv[i][0] != '-')
      files.push_back(argv[i]);
    std::cout << "Processing " << files.size() << " file(s)\n";
    
    std::ofstream fout;
    
    while ((c = getopt(argc - 1, argv + 1, "p:l:m:r:f:n:a:qdcjsubo:t:")) != -1) 
      {
      switch (c) {
	case 'p':
	    nprocs = atoi(optarg);
	    break;
	case 'l':
	    ndisp = atoi(optarg);
	    break;
	case 'm':
	    map_tasks = atoi(optarg);
	    break;
	case 'r':
	    reduce_tasks = atoi(optarg);
	    break;
      	case 'q':
	    quiet = 1;
	    break;
	case 'a':
	  autosplit = true;
	  nchunks_split = atoi(optarg);
	  break;
      case 'j':
	json_output = 1;
	break;
      case 'o':
	fout.open(optarg);
	if (!fout) {
	  std::cerr << "unable to open " << optarg << std::endl;
	  exit(EXIT_FAILURE);
	}
	break;
      case 'f':
	lformat_name = optarg;
	break;
      case 'n':
	app_name = optarg;
	break;
      case 'd':
	store_content = true;
	break;
      case 'c':
	compressed = true;
	break;
      default:
	    usage(argv[0]);
	    exit(EXIT_FAILURE);
	    break;
	}
    }

    /* fill log format up. */
    if (lf.read(lformat_name) < 0)
      {
	std::cerr << "Error opening the log format file\n";
	exit(1);
      }
    
    /* get input file */
    for (size_t j=0;j<files.size();j++)
      {
	std::cerr << "\nProcessing file: " << files.at(j) << std::endl;
	struct stat st;
	if (stat(files.at(j).c_str(),&st)!=0)
	  {
	    std::cerr << "[Error] file not found: " << files.at(j) << std::endl;
	    continue;
	  }
	const char *fn = files.at(j).c_str();
	bl *app = nullptr;
	if (!autosplit)
	  {
	    mapreduce_appbase::initialize();
	    app = new bl(fn, map_tasks);
	    app->run(nprocs,reduce_tasks,quiet,json_output,ndisp,fout);
	    delete app;
	    mapreduce_appbase::deinitialize();
	  }
	else
	  {
	    size_t mfsize = 0;
	    size_t nchunks = 1;
	    bool do_autosplit = file_size_autosplit(st.st_size,fn,mfsize,nchunks);
	    std::cerr << "do_autosplit: " << do_autosplit << std::endl;
	    if (do_autosplit)
	      {
		std::cout << "Working on " << nchunks << " splitted chunks of " << mfsize << " bytes\n";
		std::ifstream fin(fn);
		//int read_count;
		//char *buf = new char[mfsize];
		for (int ch=0;ch<nchunks;ch++)
		  {
		    std::string line,buf;
		    while(std::getline(fin,line)
			  && buf.length() < mfsize)
		      buf += line + "\n";
		    /*while(fin.read(buf,mfsize)
		      || (read_count = fin.gcount()) != 0)*/
		    std::cout << "--> Chunk #" << ch+1 << " / " << nchunks << std::endl;
		    mapreduce_appbase::initialize();
		    app = new bl(const_cast<char*>(buf.c_str()), buf.length(), map_tasks);
		    app->run(nprocs,reduce_tasks,quiet,json_output,ndisp,fout);
		    delete app;
		    mapreduce_appbase::deinitialize();
		  }
		  //delete[] buf;
	      }
	    else
	      {
		mapreduce_appbase::initialize();
		app = new bl(fn, map_tasks);
		app->run(nprocs,reduce_tasks,quiet,json_output,ndisp,fout);
		delete app;
		mapreduce_appbase::deinitialize();
	      }
	  }
	/*app->set_ncore(nprocs);
	app->set_reduce_task(reduce_tasks);
	app->sched_run();
	app->print_stats();*/
	//app->run(nprocs,reduce_tasks,quiet,json_output,ndisp,fout);
	/* get the number of results to display */
	/*if (!quiet)
	  print_top(&app->results_, ndisp);
	if (fout.is_open()) 
	  {
	    if (json_output)
	      output_json(&app->results_,fout);
	    else output_all(&app->results_,fout);
	  }
	else if (json_output)
	  output_json(&app->results_,std::cout);
	free_records(&app->results_);
	app->free_results();*/
	/*delete app;
	  mapreduce_appbase::deinitialize();*/
      }
    if (fout.is_open())
      fout.close();
    std::cout << "[Info]: skipped logs: " << skipped_logs << std::endl;
    return 0;
}
