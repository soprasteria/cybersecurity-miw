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

#include "log_format.h"
#include <algorithm>
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

  std::string log_format::chomp_cpp(const std::string &s)
  {
    std::string sc = s;
    size_t p = 0;

    // strip leading whitespace.                                                                                                           
    while(p < sc.length() && isspace(sc[p]))
      p++;
    sc.erase(0,p);
    if (sc.empty())
      return sc;

    // strip trailing whitespace.                                                                                                          
    p = sc.length()-1;
    while(p > 0 && isspace(sc[p]))
      p--;
    sc.erase(p+1,sc.length()-p+1);

    return sc;
  }
  
  void log_format::tokenize(const std::string &str,
			    const int &length,
			    std::vector<std::string> &tokens,
			    const std::string &delim,
			    const std::string &quotechar)
  {
    
    // Skip delimiters at beginning.
    std::string::size_type lastPos = str.find_first_not_of(delim, 0);
    std::string::size_type pos = 0;
    if (!quotechar.empty() && str[0] == quotechar[0])
      {
	// find closing quotechar.
	pos = str.find_first_of(quotechar,lastPos);
      }
    // Find first "non-delimiter".
    else pos = str.find_first_of(delim, lastPos);
    
    while (std::string::npos != pos || std::string::npos != lastPos)
      {
	std::string token;
	// check for quotechar delimited token.
	if (!quotechar.empty() && str[lastPos] == quotechar[0])
	  {
	    // find closing quotechar.
	    pos = str.find_first_of(quotechar, lastPos+1)-1;
	    token = str.substr(lastPos+1,pos-lastPos);
	    pos += 2;
	  }
        else token = str.substr(lastPos, pos - lastPos);
	/*std::cerr << "pos: " << pos << " -- lastPos: " << lastPos << std::endl;
	  std::cerr << "token: " << token << std::endl;*/
	tokens.push_back(token);
	if (length != -1 
	    && (int)pos >= length)
	  break;
	
        // Skip delimiters.  Note the "not_of"
	if (quotechar.empty())
	  lastPos = str.find_first_not_of(delim, pos);
	else lastPos = std::min(pos + 1,str.length());
	// Find next "non-delimiter"
        pos = str.find_first_of(delim, lastPos);
	
	/*std::cerr << "pos end: " << pos << " -- lastPos end: " << lastPos << std::endl;
	  std::cerr << "str length: " << str.length() << std::endl;*/
	if (!quotechar.empty() && (pos >= str.length() || lastPos >= str.length()))
	  break;
      }
  }
  
  int log_format::parse_data(const std::string &data,
			     const int &length,
			     const std::string &appname,
			     const bool &store_content,
			     std::vector<log_record*> &lrecords) const
  {
    std::vector<std::string> lines;
    log_format::tokenize(data,length,lines,"\n","");
#ifdef DEBUG    
    std::cout << "number of lines in map: " << lines.size() << std::endl;
#endif

    int skipped_logs = 0;
    for (size_t i=0;i<lines.size();i++)
      {
	if (lines.at(i).substr(0,1) == _ldef.commentchar())  // skip comments
	  continue;
	log_record *lr = parse_line(lines.at(i),appname,store_content,skipped_logs);
	if (lr)
	  lrecords.push_back(lr);
      }
    return 1;
  }

  log_record* log_format::parse_line(const std::string &line,
				     const std::string &appname,
				     const bool &store_content,
				     int &skipped_logs) const
  {
    std::string key;
    std::vector<std::string> tokens;
    log_format::tokenize(line,-1,tokens,_ldef.delims(),_ldef.quotechar());
    if ((int)tokens.size() > _ldef.fields_size())
      {
	std::cerr << "[Error]: wrong number of tokens detected, " << tokens.size()
		  << " expected " << _ldef.fields_size() << " for log: " << line << std::endl;
	++skipped_logs;
	return NULL;
      }

    // parse fields according to format.
    int pos = -1;
    logdef ldef = _ldef;
    ldef.set_appname(appname);
    /*std::cerr << "fields size: " << ldef.fields_size() << std::endl;
      std::cerr << "tokens size: " << tokens.size() << std::endl;*/
    for (int i=0;i<ldef.fields_size();i++)
      {
	field *f = ldef.mutable_fields(i);
	if (f->pos() == -1)
	  {
	    // auto increment wrt previous marked pos log.
	    f->set_pos(++pos);
	  }
	else pos = f->pos();
	
	if (f->pos() >= tokens.size())
	  {
	    std::cerr << "[Error]: token position " << f->pos() << " is beyond the number of log fields. Skipping. Check your format file\n";
	    continue;
	  }
	
	std::string token = tokens.at(f->pos());
	std::string ftype = f->type();

	// processing of token
	std::string ntoken;
	std::remove_copy(token.begin(),token.end(),std::back_inserter(ntoken),'"');
	token = ntoken;
	if (f->processing() == "day" || f->processing() == "month")
	  {
	    std::vector<std::string> elts;
	    log_format::tokenize(token,-1,elts,"-",""); // XXX: very basic tokenization of dates of the form 2012-12-10.
	    if (elts.size() == 3)
	      {
		if (f->processing() == "day")
		  token = elts.at(2);
		else if (f->processing() == "month")
		  token = elts.at(1);
	      }
	    else std::cerr << "[Warning]: unrecognized date format " << token << std::endl;
	  }
	else if (f->processing() == "hour" || f->processing() == "minute" || f->processing() == "second")
	  {
	    std::vector <std::string> elts;
	    log_format::tokenize(token,-1,elts,":",""); // XXX: very basic tokenization of time fields of the form 14:39:02.
	    if (elts.size() == 3)
	      {
		if (f->processing() == "hour")
		  token = elts.at(0);
		else if (f->processing() == "minute")
		  token = elts.at(1);
		else if (f->processing() == "second")
		  token = elts.at(2);
	      }
	    else std::cerr << "[Warning]: unrecognized time format " << token << std::endl;
	  }
	
	// apply preprocessing (or not) to field according to type.
	if (ftype == "int")
	  {
	    int_field *ifi = f->mutable_int_fi();
	    ifi->add_int_reap(atoi(token.c_str()));
	    if (f->aggregated() && f->aggregation() == "mean")
	      ifi->add_int_reap(1);
	  }
	else if (ftype == "string")
	  {
	    string_field *ifs = f->mutable_str_fi();
	    token = chomp_cpp(token);
	    if (!token.empty())
	      {
		ifs->add_str_reap(token);
		if (f->aggregated() && f->aggregation() == "union_count")
		  ifs->add_str_count(1);
	      }
	    //std::cerr << "string field size: " << f->str_fi().str_reap_size() << std::endl;
	  }
	else if (ftype == "bool")
	  {
	    bool_field *ifb = f->mutable_bool_fi();
	    ifb->add_bool_reap(static_cast<bool>(atoi(token.c_str())));
	  }
	else if (ftype == "float")
	  {
	    float_field *iff = f->mutable_real_fi();
	    iff->add_float_reap(atof(token.c_str()));
	    if (f->aggregated() && f->aggregation() == "mean")
	      iff->add_float_reap(1);
	  }

	//TODO: pre-processing of field based on (yet to define) configuration field.
	
	// process fields that are part of the key.
	if (f->key())
	  {
	    if (!key.empty())
	      key += "_";
	    key += token;
	  }
      }

    //debug
    //std::cerr << "ldef first field string size: " << ldef.fields(0).str_fi().str_reap_size() << std::endl;
    //debug

    if (!appname.empty())
      key += "_" + appname;
    log_record *lr = new log_record(key,ldef);

    if (store_content)
      lr->_lines.push_back(line);
    
    //debug
    //std::cerr << "created log record: " << lr->to_json() << std::endl;
    //debug

    return lr;
  }
  
}
