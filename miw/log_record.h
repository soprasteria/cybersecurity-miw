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
 * In-memory log record.
 */

#ifndef LOG_RECORD_H
#define LOG_RECORD_H

#include "log_definition.pb.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <jsoncpp/json/json.h>

namespace miw
{

  class log_record
  {
  public:
    log_record(const std::string &key,
	       const logdef &ld);
    ~log_record();

    std::string key() const;

    void merge(log_record *lr); //TODO: need log format to check on aggregated fields etc ?

    void flatten_lines();
    
    /*static void to_json_solr(field &f, Json::Value &jrec,
      std::string &date, std::string &time);*/
    void to_json(field &f, const int &i, Json::Value &jrec,
		 std::string &date, std::string &time);
    void to_json(Json::Value &jlrec);
    static void json_to_csv(const Json::Value &jl,
			    std::string &csvline,
			    const bool &header=false);

    // compression for storage.
    static std::string compress_log_lines(const std::string &line);
    static std::string uncompress_log_lines(const std::string &cline);

    // field aggregation functions.
    void aggregation_union(const int &i,
			   const field &f,
			   const bool &count,
			   log_record *lr);
    
    void aggregation_sum(const int &i,
			 const field &f);

    void aggregation_max(const int &i,
			 const field &f);

    void aggregation_mean(const int &i,
			  const field &f);
    
    void aggregation_count(const int &i,
			   const field &f);

    void aggregation_variance(const int &i,
			      const field &f);

    float compute_ratio(const std::string &numerator,
				    const std::string &denominator);

    std::string _key;
    long int _sum;
    logdef _ld;
    std::vector<std::string> _lines; // original log lines from which the compacted record was created.
    std::string _uncompressed_lines;
    std::string _compressed_lines;
    int _compressed_size;
    int _original_size;
    bool _compressed;
    std::unordered_map<int,std::unordered_map<std::string,int>> _unos; // cache for string arrays in aggregated unions.
  };
  
}

#endif
