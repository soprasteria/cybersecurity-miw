/**
 * Copyright (c) 2013 XPLR Software Inc.
 * All rights reserved.
 * Author: Emmanuel Benazera <emmanuel.benazera@xplr.com>
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

#include "log_record.h"
#include <iostream>

namespace miw
{

  log_record::log_record(const std::string &key,
			 const logdef &ld)
    :_key(key),_sum(1),_ld(ld)
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
				     const field &f)
  {
    std::string ftype = f.type();
    if (ftype == "int")
      {
	int_field ifi = _ld.fields(i).int_fi();
	for (int j=0;j<f.int_fi().int_reap_size();j++)
	  ifi.add_int_reap(f.int_fi().int_reap(j));
      }
    else if (ftype == "string")
      {
	string_field ifs = _ld.fields(i).str_fi();
	for (int j=0;j<f.str_fi().str_reap_size();j++)
	  ifs.add_str_reap(f.str_fi().str_reap(j));
      }
    else if (ftype == "bool")
      {
	bool_field ifb = _ld.fields(i).bool_fi();
	for (int j=0;j<f.bool_fi().bool_reap_size();j++)
	  ifb.add_bool_reap(f.bool_fi().bool_reap(j));
      }
    else if (ftype == "float")
      {
	float_field iff = _ld.fields(i).real_fi();
	for (int j=0;j<f.real_fi().float_reap_size();j++)
	  iff.add_float_reap(f.real_fi().float_reap(j));
      }
  }

  void log_record::aggregation_sum(const int &i,
				   const field &f)
  {
    std::string ftype = f.type();
    if (ftype == "int")
      {
	int_field ifi = _ld.fields(i).int_fi();
	for (int j=0;j<f.int_fi().int_reap_size();j++)
	  ifi.set_int_reap(0,ifi.int_reap(0) + f.int_fi().int_reap(j));
      }
    else if (ftype == "float")
      {
	float_field iff = _ld.fields(i).real_fi();
	for (int j=0;j<f.real_fi().float_reap_size();j++)
	  iff.set_float_reap(0,iff.float_reap(0) + f.real_fi().float_reap(j));
      }
    else
      {
	std::cerr << "[Error]: trying to sum up non numerical field\n";
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
	std::cerr << "[Error]: trying max operator on non numerical field\n";
      }
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
    for (size_t i=0;i<lr->_ld.fields_size();i++)
      {
	if (!lr->_ld.fields(i).key())
	  {
	    if (lr->_ld.fields(i).aggregated())
	      {
		// aggregate into hosting record (this).
		std::string aggregation = lr->_ld.fields(i).aggregation();
		if (aggregation =="union")
		  {
		    aggregation_union(i,lr->_ld.fields(i));
		  }
		else if (aggregation == "sum")
		  {
		    aggregation_sum(i,lr->_ld.fields(i));
		  }
		else if (aggregation == "max")
		  {
		    aggregation_max(i,lr->_ld.fields(i));
		  }
	      }
	  }
      }
    _sum += lr->_sum;
  }

  Json::Value log_record::to_json() const
  {

  }
  
}
