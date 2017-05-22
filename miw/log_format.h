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
 * Generic framework for defining and processing log formats.
 */

#ifndef LOG_FORMAT_H
#define LOG_FORMAT_H

#include "log_record.h"
#include "log_definition.pb.h"
#include <vector>
#include <string>
#include <sstream>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace miw
{

  class log_format
  {
  public:
    log_format();
    ~log_format();

    int save();
    
    int read(const std::string &name);
    
    //bool check() const;  // XXX: unused ?

    static std::string chomp_cpp(const std::string &s);

    static void tokenize_simple(const std::string &str,
				std::vector<std::string> &tokens,
				const std::string &delim);
    
    static void tokenize(const std::string &str,
			 const int &length,
			 std::vector<std::string> &tokens,
			 const std::string &delim,
			 const std::string &quotechar);
    
    int parse_data(const std::string &data,
		   const int &length,
		   const std::string &appname,
		   const bool &store_content,
		   const bool &compressed,
		   const bool &quiet,
		   const size_t &pos,
		   const bool &skip_header,
		   std::vector<log_record*> &lrecords) const;

    log_record* parse_line(const std::string &line,
			   const std::string &appname,
			   const bool &store_content,
			   const bool &compressed,
			   const bool &quiet,
			   int &skipped_logs);

    // custom pre-processing.
    int pre_process_evtxcsv(field *f,
			    const std::string &token,
			    std::vector<field*> &nfields) const;

    int pre_process_evtxcsv2(field *f,
			     const std::string &token,
			     std::vector<field*> &nfields) const;

    int pre_process_microsoftdnslogs(field *f,
				     const std::string &token,
				     std::vector<field*> &nfields) const;

    bool filter_contain(logdef &ldef, const int &i) const;
    
    std::unordered_map<std::string,std::unordered_set<std::string>*> _match_file_fields;
    std::mutex _loading_match_file_mutex;
    logdef _ldef;  // protocol buffer object.
  };
  
}

#endif
