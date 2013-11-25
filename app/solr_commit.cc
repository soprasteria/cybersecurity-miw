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
#include <map>
#include <algorithm>
#include <jsoncpp/json/json.h>
#include "curl_mget.h"

#define DEFAULT_NDISP 10
//#define DEBUG

/* Hadoop print all the key/value paris at the end.  This option 
 * enables wordcount to print all pairs for fair comparison. */
//#define HADOOP

enum { with_value_modifier = 0 };

static std::string solr_url = "http://localhost:8984/solr/update/json?commit=true";

class rec_parser
{
public:
  static int _pos;

  static void tokenize(const std::string &str,
		       const int &length,
		       std::vector<std::string> &tokens,
		       const std::string &delim)
  {
    
    // Skip delimiters at beginning.
    std::string::size_type lastPos = str.find_first_not_of(delim, 0);
    // Find first "non-delimiter".
    std::string::size_type pos = str.find_first_of(delim, lastPos);
    
    while (std::string::npos != pos || std::string::npos != lastPos)
      {
        // Found a token, add it to the vector.
	std::string token = str.substr(lastPos, pos - lastPos);
	tokens.push_back(token);
	if (length != -1 
	    && (int)pos >= length)
	  break;
	
        // Skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delim, pos);
        // Find next "non-delimiter"
        pos = str.find_first_of(delim, lastPos);
      }
  };

  static void parse(split_t *ma,
		    long &succeed,
		    long &failed)
  {
    static std::string delim1 = "\n";
    std::string dat = (char*)ma->data;
    std::string ldat = dat;
    std::vector<std::string> lines;
    rec_parser::tokenize(ldat,ma->length,lines,delim1);
#ifdef DEBUG    
    std::cout << "number of lines in map: " << lines.size() << std::endl;
#endif
    std::string post_content = "[";
    for (size_t i=0;i<lines.size();i++)
      {
	post_content += lines.at(i);
	if (i!=lines.size()-1)
	  post_content += ",";
      }
    post_content += "]";
    
#ifdef DEBUG
    std::cerr << "post_content: " << post_content << std::endl;
#endif    

    // post content to Solr, and collect failures.
    int status = 0;
    long http_code = 0;
    curl_mget cmg(1,300,0,300,0);
    std::string *output = cmg.www_simple(solr_url,NULL,status,http_code,
					 "POST",&post_content,post_content.length(),"application/json");
    if (output)
      delete output;
    if (status != 0 || http_code != 200)
      {
	std::cerr << "Error while posting to Solr with status " << status << " and HTTP code " << http_code << std::endl;
	failed++;
      }
    else succeed++;
  };

};

int rec_parser::_pos = 0;

class sc : public map_reduce {
public:
  sc(const char *f, int nsplit) : s_(f, nsplit) {}
  bool split(split_t *ma, int ncores) {
    return s_.split(ma, ncores, "\n",0);
  }
    int key_compare(const void *s1, const void *s2) {
      return  strcasecmp((const char *) s1, (const char *) s2);
    }
  
  void map_function(split_t *ma);

  void reduce_function(void *key_in, void **vals_in, size_t vals_len);
    
    /* write back the sums */
  int combine_function(void *key_in, void **vals_in, size_t vals_len);
  
  void *modify_function(void *oldv, void *newv) {
    uint64_t v = (uint64_t) oldv;
    uint64_t nv = (uint64_t) newv;
    return (void *) (v + nv);
  }
  
  void *key_copy(void *src, size_t s) {
    char *key = safe_malloc<char>(s + 1);
    memcpy(key, src, s);
    key[s] = 0;
    return key;
  }
  
  int final_output_compare(const keyval_t *kv1, const keyval_t *kv2) {
#ifdef HADOOP
    return strcmp((char *) kv1->key, (char *) kv2->key);
#else
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

void sc::map_function(split_t *ma)
{
  static std::string succ = "SUCCEED";
  static std::string fail = "FAILED";
  long succeed=0,failed=0;
  rec_parser::parse(ma,succeed,failed);
#ifdef DEBUG
  std::cout << "number of mapped records: " << succeed + failed << std::endl;
#endif
  map_emit((void*)succ.c_str(),(void*)succeed,strlen(succ.c_str()));
  map_emit((void*)fail.c_str(),(void*)failed,strlen(fail.c_str()));
}

int sc::combine_function(void *key_in, void **vals_in, size_t vals_len)
{
  long *vals = (long *) vals_in;
  for (uint32_t i = 1; i < vals_len; i++)
    vals[0] += vals[i];
  return 1;
}

void sc::reduce_function(void *key_in, void **vals_in, size_t vals_len)
{
  long *vals = (long*) vals_in;
  long sum = 0;
  for (uint32_t i=0;i<vals_len;i++)
    sum += vals[i];
  reduce_emit(key_in,(void*)sum);
}

static void print_top(xarray<keyval_t> *wc_vals, int ndisp) {
    size_t occurs = 0;
    std::multimap<long,std::string,std::greater<long> > ordered_records;
    std::multimap<long,std::string,std::greater<long> >::iterator mit;
    for (uint32_t i = 0; i < wc_vals->size(); i++)
      {
	occurs += size_t(wc_vals->at(i)->val);
      }
    printf("\nsolr_commit: results (TOP %d from %zu keys, %zd logs):\n",
           ndisp, wc_vals->size(), occurs);
#ifdef HADOOP
    ndisp = wc_vals->size();
#else
    ndisp = std::min(ndisp, (int)wc_vals->size());
#endif
    for (int i = 0; i < ndisp; i++) {
      keyval_t *w = wc_vals->at(i);
      printf("%15s - %d\n", (char *)w->key_, ptr2int<unsigned>(w->val));
    }
}

static void output_all(xarray<keyval_t> *wc_vals, FILE *fout) 
{
  for (uint32_t i = 0; i < wc_vals->size(); i++) 
    {
      keyval_t *w = wc_vals->at(i);
      fprintf(fout, "%18s - %d\n", (char *)w->key_,ptr2int<unsigned>(w->val));
    }
}

static void usage(char *prog) {
    printf("usage: %s <filename> [options]\n", prog);
    printf("options:\n");
    printf("  -t #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -o filename : save output to a file\n");
    printf("  -h specify solr host (e.g. localhost is default)\n");
    printf("  -p specifiy solr port\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) 
{    
    int nprocs = 0, map_tasks = 0, ndisp = 5, reduce_tasks = 0;
    int quiet = 0;    
    int c;
    std::string solr_host = "localhost";
    int solr_port = 8984;
    if (argc < 2)
      usage(argv[0]);
    char *fn = argv[1];
    FILE *fout = NULL;

    while ((c = getopt(argc - 1, argv + 1, "t:s:l:m:r:p:h:qo:")) != -1) 
      {
      switch (c) {
	case 't':
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
        case 'p':
	  solr_port = atoi(optarg);
	  break;
        case 'h':
	  solr_host = optarg;
	  break;
      case 'o':
	    fout = fopen(optarg, "w+");
	    if (!fout) {
		fprintf(stderr, "unable to open %s: %s\n", optarg,
			strerror(errno));
		exit(EXIT_FAILURE);
	    }
	    break;
      default:
	    usage(argv[0]);
	    exit(EXIT_FAILURE);
	    break;
	}
      }

    solr_url = "http://" + solr_host + ":" + to_string(solr_port) + "/solr/update/json?commit=true";
    
    mapreduce_appbase::initialize();
    /* get input file */
    sc app(fn, map_tasks);
    app.set_ncore(nprocs);
    app.set_reduce_task(reduce_tasks);
    app.sched_run();
    app.print_stats();
    /* get the number of results to display */
    if (!quiet)
	print_top(&app.results_, ndisp);
    if (fout) 
      {
	output_all(&app.results_,fout);
	fclose(fout);
      }
    app.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
