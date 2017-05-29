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

#include "log_record.h"
#include <unordered_map>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <snappy.h>
#include <assert.h>
#include "str_utils.h"
#include <glog/logging.h>

namespace miw
{

  log_record::log_record(const std::string &key,
			 const logdef &ld)
    :_key(key),_sum(1),_ld(ld),_compressed_size(0),_original_size(0),_compressed(false)
  {
  }

  log_record::~log_record()
  {
  }

  std::string log_record::key() const
  {
    return _key;
  }

  void log_record::aggregation_union(const int &i,
				     const field &f,
				     const bool &count,
				     log_record *lr)
  {
    std::string ftype = f.type();
    if (ftype == "int")
      {
	int_field *ifi = _ld.mutable_fields(i)->mutable_int_fi();
	for (int j=0;j<f.int_fi().int_reap_size();j++)
	  ifi->add_int_reap(f.int_fi().int_reap(j));
      }
    else if (ftype == "string" || ftype == "date" || ftype == "url")
      {
	string_field *ifs = _ld.mutable_fields(i)->mutable_str_fi();

	std::unordered_map<int,std::unordered_map<std::string,int>>::iterator hit,hit2;
	if ((hit=_unos.find(i))==_unos.end())
	  {
	    _unos.insert(std::pair<int,std::unordered_map<std::string,int>>(i,std::unordered_map<std::string,int>()));
	    hit = _unos.find(i);
	  }
	if ((*hit).second.empty())
	  {
	    for (int j=0;j<ifs->str_reap_size();j++)
	      (*hit).second.insert(std::pair<std::string,int>(ifs->str_reap(j),j));
	    (*hit).second.max_load_factor(0.5);
	  }
	std::unordered_map<std::string,int>::const_iterator sit;
	
	
	if ((hit2=lr->_unos.find(i))==lr->_unos.end())
	  {
	    lr->_unos.insert(std::pair<int,std::unordered_map<std::string,int>>(i,std::unordered_map<std::string,int>()));
	    hit2 = lr->_unos.find(i);
	  }
	if ((*hit2).second.empty())
	  {
	    for (int j=0;j<f.str_fi().str_reap_size();j++)
	      (*hit2).second.insert(std::pair<std::string,int>(f.str_fi().str_reap(j),j));
	    (*hit2).second.max_load_factor(0.5);
	  }

	auto hit3 = (*hit2).second.begin();
	while(hit3!=(*hit2).second.end())
	  {
	    std::string str = (*hit3).first;
	    int counter = 1;
	    if (count && f.str_fi().str_count_size() > 0)
	      counter = f.str_fi().str_count((*hit3).second);
	    if ((sit=(*hit).second.find(str))==(*hit).second.end())
	      {
		(*hit).second.insert(std::pair<std::string,int>(str,(*hit).second.size())); // store string and position (for counters)
		if (count)
		  {
		    ifs->add_str_count(counter);
		  }
	      }
	    else if (count)
	      {
		int pos = (*sit).second;
		int cval = ifs->str_count_size() > 0 ? ifs->str_count(pos) : 0;
		if (ifs->str_count_size() > 0) // XXX: beware.
		  ifs->set_str_count(pos,cval + counter);
		else ifs->add_str_count(cval + counter);
	      }
	    ++hit3;
	  }
	//debug
	/*if (count)
	  assert(ifs->str_count_size() == ifs->str_reap_size());*/
	//debug
      }
    else if (ftype == "bool")
      {
	bool_field *ifb = _ld.mutable_fields(i)->mutable_bool_fi();
	for (int j=0;j<f.bool_fi().bool_reap_size();j++)
	  ifb->add_bool_reap(f.bool_fi().bool_reap(j));
      }
    else if (ftype == "float")
      {
	float_field *iff = _ld.mutable_fields(i)->mutable_real_fi();
	for (int j=0;j<f.real_fi().float_reap_size();j++)
	  iff->add_float_reap(f.real_fi().float_reap(j));
      }
  }

  void log_record::aggregation_sum(const int &i,
				   const field &f)
  {
    std::string ftype = f.type();
    if (ftype == "int")
      {
	int_field *ifi = _ld.mutable_fields(i)->mutable_int_fi();
	if (ifi->int_reap_size() == 0)
	  ifi->add_int_reap(0);
	for (int j=0;j<f.int_fi().int_reap_size();j++)
	  {
	    ifi->set_int_reap(0,ifi->int_reap(0) + f.int_fi().int_reap(j));
	  }
      }
    else if (ftype == "float")
      {
	float_field *iff = _ld.fields(i).mutable_real_fi();
	for (int j=0;j<f.real_fi().float_reap_size();j++) {
	  iff->set_float_reap(0,iff->float_reap(0) + f.real_fi().float_reap(j));
	}
      }
    else
      {
	LOG(ERROR) << "Error: trying to sum up non numerical field " << f.name();
      }
  }

  void log_record::aggregation_max(const int &i,
				   const field &f)
  {
    std::string ftype = f.type();
    if (ftype == "int")
      {
	int_field ifi = _ld.fields(i).int_fi();
	for (int j=0;j<f.int_fi().int_reap_size();j++)
	  ifi.set_int_reap(0,std::max(ifi.int_reap(0),f.int_fi().int_reap(j)));
      }
    else if (ftype == "float")
      {
	float_field iff = _ld.fields(i).real_fi();
	for (int j=0;j<f.real_fi().float_reap_size();j++)
	  iff.set_float_reap(0,std::max(iff.float_reap(0),f.real_fi().float_reap(j)));
      }
    else
      {
	LOG(ERROR) << "Error: trying max operator on non numerical field\n";
      }
  }

  void log_record::aggregation_mean(const int &i,
				    const field &f)
  {
    std::string ftype = f.type();
    if (ftype == "int")
      {
	int_field *ifi = _ld.mutable_fields(i)->mutable_int_fi();
	ifi->set_int_reap(0,ifi->int_reap(0) + f.int_fi().int_reap(0));
	//ifi->set_int_reap(1,ifi->int_reap(1) + f.int_fi().int_reap(1)); // we use the second array index for storing the number of entry.
	ifi->set_holder(ifi->holder() + f.int_fi().holder());
      }
    else if (ftype == "float")
      {
	float_field *iff = _ld.mutable_fields(i)->mutable_real_fi();
	iff->set_float_reap(0,iff->float_reap(0) + f.real_fi().float_reap(0));
	//iff->set_float_reap(1,iff->float_reap(1) + f.real_fi().float_reap(1));
	iff->set_holder(iff->holder() + f.real_fi().holder());
      }
    else
      {
	LOG(ERROR) << "Error: trying mean operator on non numerical field\n";
      }
  }

  /**
     Estimated variance (Naive algorithm)
     https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
   */
  void log_record::aggregation_variance(const int &i,
					const field &f)
  {
    std::string ftype = f.type();
    if (ftype == "int")
      {
	int_field *ifi = _ld.mutable_fields(i)->mutable_int_fi();
	ifi->set_int_reap(0,ifi->int_reap(0) + f.int_fi().int_reap(0)); // sum of values
	ifi->set_int_reap(1,ifi->int_reap(1) + (f.int_fi().int_reap(0) * f.int_fi().int_reap(0))); // sum of squared values
	ifi->set_holder(ifi->holder() + f.int_fi().holder());
      }
    else if (ftype == "float")
      {
	float_field *iff = _ld.mutable_fields(i)->mutable_real_fi();
	iff->set_float_reap(0,iff->float_reap(0) + f.real_fi().float_reap(0));
	iff->set_float_reap(1,iff->float_reap(1) + (f.real_fi().float_reap(0) * f.real_fi().float_reap(0)));
	iff->set_holder(iff->holder() + f.real_fi().holder());
      }
    else
      {
	LOG(ERROR) << "Error: trying variance operator on non numerical field\n";
      }
  }

  void log_record::aggregation_count(const int &i,
				     const field &f)
  {
    _ld.mutable_fields(i)->set_count(f.count() + 1);
  }
  
  void log_record::merge(log_record *lr)
  {
    //std::cerr << "[Debug]: merging log records\n";
    
    if (!lr)
      return;

    // look for key fields for each record.
    // iterate remaining fields:
    // if 'aggregated', aggregate (e.g. sum, mean, union, ...)
    // else if not stored, skip (remove ?) field
    for (int i=0;i<lr->_ld.fields_size();i++)
      {
	if (!lr->_ld.fields(i).key())
	  {
	    if (lr->_ld.fields(i).aggregated())
	      {
		// aggregate into hosting record (this).
		std::string aggregation = lr->_ld.fields(i).aggregation();
		if (aggregation == "count")
		  {
		    aggregation_count(i,lr->_ld.fields(i));
		  }
		if (aggregation =="union" || aggregation == "union_count")
		  {
		    aggregation_union(i,lr->_ld.fields(i),(aggregation == "union_count" ? true : false),lr);
		  }
		else if (aggregation == "sum")
		  {
		    aggregation_sum(i,lr->_ld.fields(i));
		  }
		else if (aggregation == "max")
		  {
		    aggregation_max(i,lr->_ld.fields(i));
		  }
		else if (aggregation == "mean")
		  {
		    aggregation_mean(i,lr->_ld.fields(i));
		  }
		else if (aggregation == "variance")
		  {
		    aggregation_variance(i,lr->_ld.fields(i));
		  }

	      }
	    else if (!lr->_ld.fields(i).filter().empty())
	      {
		aggregation_sum(i,lr->_ld.fields(i));
	      }
	  }
      }
    _sum += lr->_sum;

    // merge original content.
    if (!_lines.empty() || !lr->_lines.empty())
      {
	_lines.reserve(_lines.size() + lr->_lines.size());
	std::copy(lr->_lines.begin(),lr->_lines.end(),std::back_inserter(_lines));
      }
    if (_compressed)
      {
	// uncompress compressed lines.
	std::string n_uncompressed_lines;
	if (!_compressed_lines.empty())
	  n_uncompressed_lines = log_record::uncompress_log_lines(_compressed_lines);
	// flatten new lines.
	flatten_lines();
	// add them up to existing lines.
	_uncompressed_lines = n_uncompressed_lines + _uncompressed_lines;
	// compress them together.
	_compressed_lines = log_record::compress_log_lines(_uncompressed_lines);
	_uncompressed_lines.clear();
      }
  }

  void log_record::flatten_lines()
  {
    if (!_lines.empty())
      {
	std::stringstream sst;
	std::for_each(_lines.begin(),_lines.end(),[&sst](const std::string &s){ sst << s << std::endl; });
	_uncompressed_lines = sst.str();
	_original_size = _uncompressed_lines.length();
      }
  }
  
  /*void log_record::compress_lines()
  {
    if (!_lines.empty())
      {
	std::stringstream sst;
	std::for_each(_lines.begin(),_lines.end(),[&sst](const std::string &s){ sst << s << std::endl; });
	_uncompressed_lines = sst.str();
	_compressed_lines = log_record::compress_log_lines(_uncompressed_lines);
	_compressed_size = _compressed_lines.length();
	_original_size = _uncompressed_lines.length();
	_uncompressed_lines.clear();
      }
      }*/
  
  std::string log_record::compress_log_lines(const std::string &line)
  {
    std::string output;
    snappy::Compress(line.data(),line.size(),&output);
    return output;
  }
  
  std::string log_record::uncompress_log_lines(const std::string &cline)
  {
    std::string output;
    snappy::Uncompress(cline.data(),cline.size(),&output);
    return output;
  }

  /*void log_record::to_json_solr(field &f, Json::Value &jrec,
				std::string &date, std::string &time)
  {
    if (!f.preprocessing().empty())
      return;
    std::string ftype = f.type();
    Json::Value jsf,jsfc,jsfh;
    std::string json_fname = f.name(), json_fnamec = f.name() + "_count_i", json_fnameh = f.name() + "_hold_f";
    if (ftype == "int")
      {
	int_field *ifi = f.mutable_int_fi();
	int irs = ifi->int_reap_size();
	if (irs > 1)
	  {
	    json_fname += "_is";
	    for (int i=0;i<ifi->int_reap_size();i++)
	      jsf.append(ifi->int_reap(i));
	  }
	else if (ifi->int_reap_size() == 1)
	  {
	    json_fname += "_i";
	    jsf["inc"] = ifi->int_reap(0);
	  }
	if (ifi->holder() != 0)
	  jsfh["inc"] = ifi->holder();
      }
    else if (ftype == "string" || ftype == "time")
      {
	string_field *ifs = f.mutable_str_fi();
	if (ifs->str_reap_size() > 1)
	  {
	    json_fname += "_ss";
	    json_fnamec += "s";
	    for (int i=0;i<ifs->str_reap_size();i++)
	      {
		jsf.append(ifs->str_reap(i));
		if (ifs->str_count_size() > 0)
		  jsfc.append(ifs->str_count(i));
	      }
	  }
	else if (ifs->str_reap_size() == 1)
	  {
	    json_fname += "_s";
	    jsf = ifs->str_reap(0);
	    jsfc = 1;
	    if (ifs->str_count_size() > 0)
	      jsfc = ifs->str_count(0);
	    if (ftype == "time")
	      {
		time = ifs->str_reap(0);
		if (f.processing() == "hour")
		  time += ":00:00";
		else if (f.processing() == "minute")
		  time += ":00";
	      }
	  }
      }
    else if (ftype == "date")
      {
	string_field *ifs = f.mutable_str_fi();
	if (ifs->str_reap_size() > 1)
	  {
	    json_fname += "_ss"; // was dts.
	    json_fnamec += "s";
	    for (int i=0;i<ifs->str_reap_size();i++)
	      {
		jsf.append(ifs->str_reap(i));
		if (ifs->str_count_size() > 0)
		  jsfc.append(ifs->str_count(i));
	      }
	  }
	else if (ifs->str_reap_size() == 1)
	  {
	    json_fname += "_s"; // was dt but solr complains, subsumed by std_date_dt anyways.
	    jsf = ifs->str_reap(0);
	    date = ifs->str_reap(0);
	  }
      }
    else if (ftype == "bool")
      {
	json_fname += "_bs";
	bool_field *ifb = f.mutable_bool_fi();
	for (int i=0;i<ifb->bool_reap_size();i++)
	  jsf.append(ifb->bool_reap(i));
      }
    else if (ftype == "float")
      {
	float_field *iff = f.mutable_real_fi();
	if (iff->float_reap_size() > 1)
	  {
	    json_fname += "_fs";
	    for (int i=0;i<iff->float_reap_size();i++)
	      jsf.append(iff->float_reap(i));
	  }
	else if (iff->float_reap_size() == 1)
	  {
	    json_fname += "_f";
	    jsf["inc"] = iff->float_reap(0);
	  }
	if (iff->holder() != 0)
	  jsfh["inc"] = iff->holder();
      }
    if (!jsf.isNull())
      {
	if (f.aggregated())
	  {
	    if (f.aggregation() == "union")
	      jrec[json_fname]["add"] = jsf;
	    else if (f.aggregation() == "union_count")
	      {
		jrec[json_fname]["add"] = jsf;
		jrec[json_fnamec]["add"] = jsfc;
	      }
	    else if (f.aggregation() == "sum"
		     || f.aggregation() == "count"
		     || f.aggregation() == "mean")
	      {
		jrec[json_fname] = jsf;
		if (!jsfh.isNull())
		  jrec[json_fnameh] = jsfh;
	      }
	  }
	else jrec[json_fname] = jsf;
      }
    if (f.count() > 1)
      jrec[json_fname + "_count"] = f.count();
      }*/

  void log_record::to_json(field &f, const int &i, Json::Value &jrec,
			   std::string &date, std::string &time)
  {
    if (!f.preprocessing().empty())
      return;
    std::string ftype = f.type();
    Json::Value jsf,jsfc,jsfh;
    std::string json_fname = f.name(), json_fnamec = f.name() + "_count", json_fnameh = f.name() + "_hold";
    if (ftype == "int")
      {
	int_field *ifi = f.mutable_int_fi();
	int irs = ifi->int_reap_size();
	if (irs > 1)
	  {
	    for (int j=0;j<ifi->int_reap_size();j++)
	      jsf.append(static_cast<long long>(ifi->int_reap(j)));
	  }
	else if (ifi->int_reap_size() == 1)
	  {
	    jsf = static_cast<long long>(ifi->int_reap(0));
	  }
	if (ifi->holder() != 0)
	  jsfh = static_cast<long long>(ifi->holder());
      }
    else if (ftype == "string" || ftype == "time" || ftype == "url")
      {
	string_field *ifs = f.mutable_str_fi();
	std::unordered_map<int,std::unordered_map<std::string,int>>::iterator hit;
	if ((hit=_unos.find(i))==_unos.end())
	  {
	    //LOG(ERROR) << "empty cache string field (uno)";
	    //return; //XXX: beware
	  }
	else 
	  {
	    if (!(*hit).second.empty())
	      {
		ifs->clear_str_reap();
		for (size_t l=0;l<(*hit).second.size();l++) // start from reap_size if not empty
		  ifs->add_str_reap("");
		auto rhit = (*hit).second.begin();
		while(rhit!=(*hit).second.end())
		  {
		    ifs->set_str_reap((*rhit).second,(*rhit).first);
		    ++rhit;
		  }
	      }
	  }
	if (ifs->str_reap_size() > 1)
	  {
	    for (int j=0;j<ifs->str_reap_size();j++)
	      {
		jsf.append(ifs->str_reap(j));
		if (ifs->str_count_size() > 0)
		  jsfc.append(ifs->str_count(j));
	      }
	  }
	else if (ifs->str_reap_size() == 1)
	  {
	    jsf = ifs->str_reap(0);
	    jsfc = 1;
	    if (ifs->str_count_size() > 0)
	      jsfc = ifs->str_count(0);
	    if (ftype == "time")
	      {
		time = ifs->str_reap(0);
		if (f.processing() == "hour")
		  time += ":00:00";
		else if (f.processing() == "minute")
		  time += ":00";
	      }
	  }
      }
    else if (ftype == "date")
      {
	string_field *ifs = f.mutable_str_fi();
	if (ifs->str_reap_size() > 1)
	  {
	    for (int j=0;j<ifs->str_reap_size();j++)
	      {
		jsf.append(ifs->str_reap(j));
		if (ifs->str_count_size() > 0)
		  jsfc.append(ifs->str_count(j));
	      }
	  }
	else if (ifs->str_reap_size() == 1)
	  {
	    jsf = ifs->str_reap(0);
	    date = ifs->str_reap(0);
	  }
      }
    else if (ftype == "bool")
      {
	bool_field *ifb = f.mutable_bool_fi();
	for (int j=0;j<ifb->bool_reap_size();j++)
	  jsf.append(ifb->bool_reap(j));
      }
    else if (ftype == "float")
      {
	float_field *iff = f.mutable_real_fi();
	if (iff->float_reap_size() > 1)
	  {
	    for (int j=0;j<iff->float_reap_size();j++)
	      jsf.append(iff->float_reap(j));
	  }
	else if (iff->float_reap_size() == 1)
	  {
	    jsf = iff->float_reap(0);
	  }
	if (iff->holder() != 0)
	  jsfh = iff->holder();
      }
    if (!jsf.isNull())
      {
	if (f.aggregated())
	  {
	    if (f.aggregation() == "union")
	      jrec[json_fname] = jsf;
	    else if (f.aggregation() == "union_count")
	      {
		jrec[json_fname] = jsf;
		jrec[json_fnamec] = jsfc;
	      }
	    else if (f.aggregation() == "sum"
		     || f.aggregation() == "count")
	      {
		jrec[json_fname] = jsf;
		if (!jsfh.isNull())
		  jrec[json_fnameh] = jsfh;
	      }
	    else if (f.aggregation() == "ratio")
	      {
		jrec[json_fname] = compute_ratio(f.numerator(), f.denominator());
	      }
	    else if (f.aggregation() == "mean")
	      {
		if (!jsfh.isNull())
		  jrec[json_fname] = jsf.asDouble() / jsfh.asDouble();
		else jrec[json_fname] = jsf;
	      }
	    else if (f.aggregation() == "variance")
	      {
		if (!jsfh.isNull())
		  {
		    jrec[json_fname] = (jsf[1].asDouble() - (jsf[0].asDouble() * jsf[0].asDouble()) / jsfh.asDouble()) / std::max(1.0,(jsfh.asDouble() - 1)); // note we discard Bessel's correction when n=1
		    //std::cerr << "sum=" << jsf[0].asDouble() << " / sum_of_square=" << jsf[1].asDouble() << " / n=" << jsfh.asDouble() << " / variance=" << jrec[json_fname].asDouble() << std::endl;
		  }
		else jrec[json_fname] = jsf;
	      }
	  }
	else jrec[json_fname] = jsf;
      }
    if (f.count() > 1)
      jrec[json_fname + "_count"] = f.count();
  }
  
  void log_record::to_json(Json::Value &jlrec)
  {
    //debug
    //std::cerr << "number of fields: " << _ld.fields_size() << std::endl;
    //debug
    
    std::string date = "0000-00-00", time = "00:00:00";
    jlrec["id"] = _key;
    for  (int i=0;i<_ld.fields_size();i++)
      {
	field f = _ld.fields(i);
	std::string ldate,ltime;
	to_json(f,i,jlrec,ldate,ltime);
	if (!ldate.empty())
	  date = ldate;
	else if (!ltime.empty())
	  time = ltime;
      }
    
    if (!_ld.appname().empty())
      jlrec["appname"] = _ld.appname();
    //jlrec["logs"]["inc"] = (int)_sum;
    jlrec["logs"] = static_cast<int>(_sum);
    jlrec["format_name"] = _ld.format_name();
    jlrec["std_date_dt"] = (date.find("T")!=std::string::npos) ? date + "Z" : date + "T" + time + "Z"; 
    
    //debug
    //Json::FastWriter writer;
    //std::cout << "JSON: " << writer.write(jlrec) << std::endl;
    //debug
  }

  void log_record::json_to_csv(const Json::Value &jl,
			       std::string &csvline,
			       const bool &header)
  {
    static std::string separator = ",";
    std::stringstream sts,stshead;
    for (Json::ValueConstIterator itr = jl.begin();itr!=jl.end();itr++)
      {
	std::string key = itr.key().asString();
	if (itr != jl.begin())
	  {
	    sts << separator;
	    if (header)
	      stshead << separator;
	  }
	if (header)
	  stshead << key;
	Json::Value val = (*itr);
	if (key == "logs")
	  {
	    sts << val.asInt();
	  }
	else
	  {
	    if (val.isString())
	      sts << "\"" << val.asString() << "\"";
	    else if (val.isInt())
	      sts << val.asInt();
	    else if (val.isDouble())
	      sts << val.asDouble();
	    else if (val.isBool())
	      sts << val.asBool();
	    else if (val.isArray())
	      {
		sts << "\"[";
		int i = 0;
		for (const Json::Value &va : val)
		  {
		    if (i != 0)
		      sts << separator;
		    else i++;
		    if (va.isString())
		      {
			//sts << "\\\"" << va.asString() << "\\\"";
			std::string vastr = va.asString();
			if (vastr.find(separator)!=std::string::npos)
			  str_utils::replace_in_string(vastr,separator,"");
			sts << vastr;
		      }
		    else if (va.isInt())
		      sts << va.asInt();
		    else if (va.isDouble())
		      sts << va.asDouble();
		    else if (va.isBool())
		      sts << va.asBool();
		  }
		sts << "]\"";
	      }
	  }
      }
    if (header)
      csvline = stshead.str() + "\n";
    csvline += sts.str() + "\n";
    //std::cerr << "csvline=" << csvline << std::endl;
  }

  float log_record::compute_ratio(const std::string &numerator,
				  const std::string &denominator)
  {
    const field *num = NULL;
    const field *denom = NULL;

    for (int i=0;i<_ld.fields_size();i++)
      {
	if (!numerator.compare(_ld.fields(i).name())) {
	  num = &_ld.fields(i);
	}
	else if (!denominator.compare(_ld.fields(i).name())) {
	  denom = &_ld.fields(i);
	}
      }

    if (num == NULL && numerator != "logs")
      {
	std::cerr << "Warning: numerator field "
		  << numerator
		  << " not found." << std::endl;
	return 0;
      }
    if (denom == NULL && denominator != "logs")
      {
	std::cerr << "Warning: denominator field "
		  << denominator
		  << " not found." << std::endl;
	return 0;
      }

    float fnum;
    float fdenom;

    if (num == NULL) {
      fnum = static_cast<float>(_sum);
    }
    else {
      if (num->type() == "int") {
	const int_field &inum = num->int_fi();
	fnum = inum.int_reap(0);
      }
      else if (num->type() == "float") {
	const float_field &inum = num->real_fi();
	fnum = inum.float_reap(0);
      }
      else {
	std::cerr << "Warning: numerator field "
		  << numerator
		  << " must be of type int or string." << std::endl;
	fnum = 0;
      }
    }

    if (denom == NULL) {
      fdenom = static_cast<float>(_sum);
    }
    else {
      if (denom->type() == "int") {
	const int_field &inum = denom->int_fi();
	fdenom = inum.int_reap(0);
      }
      else if (denom->type() == "float") {
	const float_field &inum = denom->real_fi();
	fdenom = inum.float_reap(0);
      }
      else {
	std::cerr << "Warning: denominator field "
		  << denominator
		  << " must be of type int or string." << std::endl;
	fdenom = 0;
      }
    }
    if (fdenom == 0)
      return 0.0;
    return fnum / fdenom;
  }
}
