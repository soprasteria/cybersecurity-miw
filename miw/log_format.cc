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
#include <time.h>

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

  void log_format::tokenize_simple(const std::string &str,
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
	tokens.push_back(str.substr(lastPos, pos - lastPos));
	// Skip delimiters.  Note the "not_of"
	lastPos = str.find_first_not_of(delim, pos);
	// Find next "non-delimiter"
	pos = str.find_first_of(delim, lastPos);
      }
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
	if (lastPos != std::string::npos && lastPos - pos > 1) // avoid skipping empty ',,' fields.
	  lastPos = pos + 1;
	
	//else lastPos = std::min(pos+1,str.length());
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
			     const bool &compressed,
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
	log_record *lr = parse_line(lines.at(i),appname,store_content,compressed,skipped_logs);
	if (lr)
	  lrecords.push_back(lr);
      }
    return 1;
  }

  log_record* log_format::parse_line(const std::string &line,
				     const std::string &appname,
				     const bool &store_content,
				     const bool &compressed,
				     int &skipped_logs) const
  {
    std::string key;
    std::vector<std::string> tokens;
    log_format::tokenize(line,-1,tokens,_ldef.delims(),_ldef.quotechar());

    /*std::cerr << "tokens size: " << tokens.size() << std::endl;
    for (size_t i=0;i<tokens.size();i++)
    std::cerr << "tok: " << tokens.at(i) << std::endl;*/

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
    std::vector<field*> nfields;
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
	
	if (f->pos() >= (int)tokens.size())
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
	if (f->processing() == "day" || f->processing() == "month" || f->processing() == "year")
	  {
	    struct tm tm;
	    if (strptime(token.c_str(),f->date_format().c_str(),&tm))
	      {
		if (f->processing() == "day")
		  token = to_string(tm.tm_year+1900) + "-" + to_string(tm.tm_mon+1) + "-" + to_string(tm.tm_mday);
		else if (f->processing() == "month")
		  token = to_string(tm.tm_year+1900) + "-" + to_string(tm.tm_mon+1);
		else if (f->processing() == "year")
		  token = to_string(tm.tm_year+1900);
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
		  token = elts.at(0) + "-" + elts.at(1);
		else if (f->processing() == "second")
		  token = elts.at(0) + "-" + elts.at(1) + "-" + elts.at(2);
	      }
	    else std::cerr << "[Warning]: unrecognized time format " << token << std::endl;
	  }
	
	// apply preprocessing (or not) to field according to type.
	if (ftype == "int")
	  {
	    int_field *ifi = f->mutable_int_fi();
	    ifi->add_int_reap(atoi(token.c_str()));
	    if (f->aggregated() && f->aggregation() == "mean")
	      ifi->set_holder(1);
	  }
	else if (ftype == "string" || ftype == "date")
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
	      iff->set_holder(1.f);
	  }

	// pre-processing of field based on (yet to define) configuration field.
	if (f->preprocessing() == "evtxcsv")
	  {
	    pre_process_evtxcsv(f,token,nfields);
	  }
	else if (f->preprocessing() == "evtxcsv2")
	  {
	    pre_process_evtxcsv2(f,token,nfields);
	  }

	// process fields that are part of the key.
	if (f->key())
	  {
	    if (!key.empty())
	      key += "_";
	    key += token;
	  }
      }

    // add new fields to ldef if any found in pre-processing step.
    for (size_t i=0;i<nfields.size();i++)
      {
	field *f = ldef.add_fields();
	*f = *nfields.at(i);
	delete nfields.at(i);
      }
    
    //debug
    //std::cerr << "ldef first field string size: " << ldef.fields(0).str_fi().str_reap_size() << std::endl;
    //debug

    if (!appname.empty())
      key += "_" + appname;
    log_record *lr = new log_record(key,ldef);
    lr->_compressed = compressed;
    
    if (store_content)
      lr->_lines.push_back(line);
    
    //debug
    //std::cerr << "created log record: " << lr->to_json() << std::endl;
    //debug

    return lr;
  }

  int log_format::pre_process_evtxcsv(field *f,
				      const std::string &token,
				      std::vector<field*> &nfields)
  {
    static std::string arrow_start = "->";
    
    std::string ftype = f->type();
    //std::cerr << "token: " << token << std::endl;

    // lookup for '->'
    std::string::size_type pos = token.find(arrow_start,0);
    std::string remain = token.substr(pos+2);
    
    // split on '=' and assemble the new fields.
    std::vector<std::string> results;
    log_format::tokenize_simple(remain,results,"=");
    if (results.empty())
      return 1;
    std::string head = results.at(0);
    for (size_t i=1;i<results.size();i++)
      {
	std::string r = results.at(i),nhead;
	if (i < results.size()-1)
	  {
	    r = r.substr(0,r.size()-1); // remove last char.
	    std::string::size_type head_pos = r.find_last_of(' ');
	    nhead = r.substr(head_pos);
	    r = r.substr(0,head_pos);
	  }
	field *nf = new field();
	nf->set_name(chomp_cpp(head));
	nf->set_type("string");
	string_field *ifs = nf->mutable_str_fi();
	ifs->add_str_reap(chomp_cpp(r));
	nfields.push_back(nf);
	head = nhead;
      }
    return 0;
  }

  int log_format::pre_process_evtxcsv2(field *f,
				       const std::string &token,
				       std::vector<field*> &nfields)
  {
    std::vector<std::string> pairs;
    log_format::tokenize_simple(token,pairs," ");
    for (size_t i=0;i<pairs.size();i++)
      {
	std::vector<std::string> elts;
	log_format::tokenize_simple(pairs.at(i),elts,":");
	if (elts.size() == 2)
	  {
	    field *nf = new field();
	    nf->set_name(elts.at(0));
	    nf->set_type("string");
	    string_field *ifs = nf->mutable_str_fi();
	    ifs->add_str_reap(elts.at(1));
	    nfields.push_back(nf);
	  }
      }
    return 0;
  }
  
}
