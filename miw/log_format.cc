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

#include "log_format.h"
#include <fstream>
#include <iostream>

//#define DEBUG

#define FMT_EXT ".fmt"

namespace miw
{

  log_format::log_format()
  {
  }

  log_format::~log_format()
  {
  }

  int log_format::read(const std::string &name)
  {
    const std::string filename = name + FMT_EXT;
    std::fstream input(filename,std::ios::in|std::ios::binary);
    if (!input)
      {
	std::cerr << filename << ": File not found\n";
	return -1;
      }
    else if (!_ldef.ParseFromIstream(&input))
      {
	std::cerr << "Failed to parse log format file " << filename << std::endl;
	return -2;
      }
    return 0;
  }
  
  void log_format::tokenize(const std::string &str,
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
  }
  
  int log_format::parse_data(const std::string &data,
			     const int &length,
			     std::vector<log_record*> &lrecords) const
  {
    std::vector<std::string> lines;
    log_format::tokenize(data,length,lines,"\n");
#ifdef DEBUG    
    std::cout << "number of lines in map: " << lines.size() << std::endl;
#endif

    int skipped_logs = 0;
    for (size_t i=0;i<lines.size();i++)
      {
	log_record *lr = parse_line(lines.at(i),skipped_logs);
	if (lr)
	  lrecords.push_back(lr);
      }
  }

  log_record* log_format::parse_line(const std::string &line,
				     int &skipped_logs) const
  {
    std::string key;
    std::vector<std::string> tokens;
    log_format::tokenize(line,-1,tokens,_ldef.delims());
    if ((int)tokens.size() > _ldef.fields_size())
      {
	std::cerr << "[Error]: wrong number of tokens detected, " << tokens.size()
		  << " expected " << _ldef.fields_size() << " for log: " << line << std::endl;
	++skipped_logs;
	return NULL;
      }

    // parse fields according to format.
    for (int i=0;i<_ldef.fields_size();i++)
      {
	field f = _ldef.fields(i);

	if (f.pos() >= tokens.size())
	  {
	    std::cerr << "[Error]: token position " << f.pos() << " is beyond the number of log fields. Skipping. Check your format file\n";
	    continue;
	  }
	
	std::string token = tokens.at(f.pos());
	
	// apply preprocessing (or not) to field according to type.
	std::string ftype = f.type();
	if (ftype == "int")
	  {
	    int_field ifi = f.int_fi();
	    ifi.add_int_reap(atoi(token.c_str()));
	  }
	else if (ftype == "string")
	  {
	    string_field ifs = f.str_fi();
	    ifs.add_str_reap(token);
	  }
	else if (ftype == "bool")
	  {
	    bool_field ifb = f.bool_fi();
	    ifb.add_bool_reap(static_cast<bool>(atoi(token.c_str())));
	  }
	else if (ftype == "float")
	  {
	    float_field iff = f.real_fi();
	    iff.add_float_reap(atof(token.c_str()));
	  }

	//TODO: pre-processing of field based on (yet to define) configuration field.
	
	//TODO: process fields that are part of the key.
	if (f.key())
	  {
	    if (!key.empty())
	      key += "_";
	    key += token;
	  }
	
      }

    log_record *lr = new log_record(key,this->_ldef);
    return lr;
  }
  
}
