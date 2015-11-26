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

#include "mr_job.h"
#include "defsplitter.hh"

//#define DEBUG

void mr_job::map_function(split_t *ma)
{
  std::vector<log_record*> log_records;
  std::string dat = (char*)ma->data; // XXX: copies the data
  _lf->parse_data(dat,ma->length,_app_name,_store_content,_compressed,_quiet,ma->pos,_skip_header,log_records);
  
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

int mr_job::combine_function(void *key_in, void **vals_in, size_t vals_len)
{
  log_record **lrecords = (log_record**)vals_in;
  for (uint32_t i=1;i<vals_len;i++)
    {
      lrecords[0]->merge(lrecords[i]);
      delete lrecords[i];
    }
  return 1;
}

void mr_job::reduce_function(void *key_in, void **vals_in, size_t vals_len)
{
  log_record **lrecords = (log_record**)vals_in;
  for (uint32_t i=1;i<vals_len;i++)
    {
      lrecords[0]->merge(lrecords[i]);
      delete lrecords[i];
    }
    reduce_emit(key_in,(void*)lrecords[0]);
}

void mr_job::print_top(xarray<keyval_t> *wc_vals, int &ndisp) {
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

void mr_job::output_all(xarray<keyval_t> *wc_vals, std::ostream &fout) 
{
  for (uint32_t i = 0; i < wc_vals->size(); i++) 
    {
      keyval_t *w = wc_vals->at(i);
      fout << (char*)w->key_ << " - " << static_cast<log_record*>(w->val)->_sum << std::endl;
    }
}

void mr_job::output_json(xarray<keyval_t> *wc_vals, std::ostream &fout)
{
  Json::FastWriter writer;
  for (uint32_t i = 0; i < wc_vals->size(); i++) 
    {
      log_record *lr = (log_record*)wc_vals->at(i)->val;
      Json::Value jrec;
      lr->to_json(jrec);
      if (!_compressed)
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

void mr_job::output_csv(xarray<keyval_t> *wc_vals, const int &nfile, std::ostream &fout)
{
  for (uint32_t i = 0; i < wc_vals->size(); i++) 
    {
      log_record *lr = (log_record*)wc_vals->at(i)->val;
      Json::Value jrec;
      lr->to_json(jrec);
      std::string csvline;
      if (i == 0 && nfile <= 0)
	log_record::json_to_csv(jrec,csvline,true); // with header
      else log_record::json_to_csv(jrec,csvline);
      //TODO: add attached logs UUIDs to every entry
      fout << csvline;
    }
}

void mr_job::output_mem(xarray<keyval_t> *wc_vals, xarray<keyval_t> *results)
{
  wc_vals->swap(*results);
}
