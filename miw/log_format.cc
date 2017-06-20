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
#include <boost/network/uri.hpp>
#include <boost/tokenizer.hpp>
#include "str_utils.h"
#include <glog/logging.h>

using namespace boost::network;

//#define DEBUG

#define FMT_EXT ".fmt"

namespace miw
{

  log_format::log_format()
  {
  }

  log_format::~log_format()
  {
    auto hit = _match_file_fields.begin();
    while(hit!=_match_file_fields.end())
      {
	delete (*hit).second;
	++hit;
      }
  }

  int log_format::read(const std::string &name)
  {
    const std::string filename = name + FMT_EXT;
    std::fstream input(filename,std::ios::in|std::ios::binary);
    if (!input)
      {
	LOG(ERROR) << filename << ": File not found\n";
	return -1;
      }
    else if (!_ldef.ParseFromIstream(&input))
      {
	LOG(ERROR) << "Failed to parse log format file " << filename << std::endl;
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
    std::stringstream ss(str);
    std::string item,tmp_item;
    bool has_quote = false;
    int pos = 0;
    boost::char_separator<char> sep(delim.c_str());
    boost::tokenizer<boost::char_separator<char>> btokens(str,sep);
    for (std::string item: btokens)
      {
	if (!item.empty())
	  {
	    bool begin = false;
	    if (!has_quote && item.at(0) == quotechar[0])
	      {
		begin = true;
		tmp_item += item;
		has_quote = true;
	      }
	    if (has_quote)
	      {
		if (!begin) {
		  while (delim.find(str[pos + tmp_item.length()]) != std::string::npos) {
		    tmp_item += str[pos + tmp_item.length()];
		  }
		  tmp_item += item;
		}
		if (item.at(item.size()-1) == quotechar[0])
		  {
		    item = tmp_item;
		    tmp_item.clear();
		    has_quote = false;
		  }
	      }
	  }
	if (!has_quote)
	  {
	    tokens.push_back(item);
	    pos += item.length() + 1;
	    if (length != -1
		&& pos >= length)
	      break;
	  }
      }
    if (!tmp_item.empty()) {
      tokens.push_back(tmp_item);
    }
  }

  int log_format::parse_data(const std::string &data,
			     const int &length,
			     const std::string &appname,
			     const bool &store_content,
			     const bool &compressed,
			     const bool &quiet,
			     const size_t &pos,
			     const bool &skip_header,
			     std::vector<log_record*> &lrecords) const
  {
    std::vector<std::string> lines;
    log_format::tokenize(data,length,lines,"\n","");
#ifdef DEBUG
    LOG(INFO) << "number of lines in map: " << lines.size() << std::endl;
#endif

    int skipped_logs = 0;
    for (size_t i=0;i<lines.size();i++)
      {
	if (i == 0 && pos == 0 && skip_header) // we're onto the first line in file, and we are asked to skip it
	  {
	    //std::cerr << "header line=" << lines.at(i) << std::endl;
	    continue;
	  }
	if (lines.at(i).empty())
	  continue;
	if (lines.at(i).substr(0,1) == _ldef.commentchar())  // skip comments
	  continue;
	log_record *lr = parse_line(lines.at(i),appname,store_content,compressed,quiet,skipped_logs);
	if (lr)
	  lrecords.push_back(lr);
      }
    return 1;
  }

  log_record* log_format::parse_line(const std::string &line,
				     const std::string &appname,
				     const bool &store_content,
				     const bool &compressed,
				     const bool &quiet,
				     int &skipped_logs)
  {
    if (chomp_cpp(line).empty())
      return NULL;
    std::string key;
    std::vector<std::string> tokens;
    log_format::tokenize(line,-1,tokens,_ldef.delims(),_ldef.quotechar());

    /*std::cerr << "tokens size: " << tokens.size() << std::endl;
    for (size_t i=0;i<tokens.size();i++)
    std::cerr << "tok: " << tokens.at(i) << std::endl;*/

    // if tokenized line has more fields than defined in the format, bail out...
    /*if ((int)tokens.size() > _ldef.fields_size()
	&& _ldef.fields(0).pos() == -1)
      {
	if (!quiet)
	  std::cerr << "[Error]: wrong number of tokens detected, " << tokens.size()
		    << " expected " << _ldef.fields_size() << " for log: " << line << std::endl;
	++skipped_logs;
	return NULL;
	}*/

    // parse fields according to format.
    int pos = -1;
    logdef ldef = _ldef;
    ldef.set_appname(appname);
    std::vector<field*> nfields;
    /*std::cerr << "fields size: " << ldef.fields_size() << std::endl;
      std::cerr << "tokens size: " << tokens.size() << std::endl;*/
    bool match = false;
    bool has_or_match = false;
    for (int i=0;i<ldef.fields_size();i++)
      {
	field *f = ldef.mutable_fields(i);
	if (f->pos() == -1)
	  {
	    // auto increment wrt previous marked pos log.
	    f->set_pos(++pos);
	  }
	else if (f->aggregation() == "ratio")
	  pos = f->pos();

	if (f->pos() >= (int)tokens.size())
	  {
	    LOG(ERROR) << "Error: token position " << f->pos() << " is beyond the number of log fields. Skipping line: " << line << std::endl;
	    return NULL;
	  }
	else if (f->filter_type() == "contain")
	  {
	    continue;
	  }


	std::string token = tokens.at(f->pos());
	std::string ftype = f->type();

	// processing of token
	std::string ntoken;
	std::remove_copy(token.begin(),token.end(),std::back_inserter(ntoken),'"');
	token = ntoken;

	// field string matching: key is a 'and', other fields can be 'or' conditions
	if (f->has_match())
	  {
	    std::unordered_set<std::string> *matches_str;
	    
	    // read match_file once if any
	    if (!f->mutable_match()->match_file().empty())
	      {
		matches_str = new std::unordered_set<std::string>();
		std::unordered_map<std::string,std::unordered_set<std::string>*>::const_iterator muit;
		std::lock_guard<std::mutex> lock(_loading_match_file_mutex);
		if ((muit=_match_file_fields.find(f->name()))==_match_file_fields.end())
		  {
		    std::ifstream infile(f->mutable_match()->match_file());
		    if (!infile.is_open())
		      {
			LOG(FATAL) << "Failed opening match file " << f->mutable_match()->match_file() << std::endl;
		      }
		    LOG(INFO) << "Reading file " << f->mutable_match()->match_file()
			      << " for field " << f->name() << std::endl;
		    std::string mstr;
		    while (infile >> mstr)
		      {
			matches_str->insert(mstr);
		      }
		    matches_str->rehash(matches_str->size());
		    _match_file_fields.insert(std::make_pair(f->name(),matches_str));
		    _ldef.mutable_fields(i)->mutable_match()->set_match_file("");
		    LOG(INFO) << "Done reading " << matches_str->size() << " line in file " << f->mutable_match()->match_file() << std::endl;
		  }
		else
		  {
		    matches_str = (*muit).second;
		  }
	      }
	    else
	      {
		matches_str = new std::unordered_set<std::string>();
		std::unordered_map<std::string,std::unordered_set<std::string>*>::const_iterator muit;
		if ((muit=_match_file_fields.find(f->name()))==_match_file_fields.end())
		  {
		    matches_str->insert(f->mutable_match()->match_str());
		    _match_file_fields.insert(std::make_pair(f->name(),matches_str));
		  }
		else
		  {
		    matches_str = (*muit).second;
		  }
	      }

	    bool negative = f->mutable_match()->negative();
	    if (!negative) // matching means keeping
	      {
		std::unordered_set<std::string>::const_iterator uit = matches_str->begin();
		if ((uit=matches_str->find(token))!=matches_str->end())
		  {
		    if (f->mutable_match()->logic() == "or")
		      match = true; // has match specified, if no 'or' match condition kicks in, the data entry should be later killed
		    uit = matches_str->end();
		  }
		else if (f->mutable_match()->exact())
		  return NULL;

		// reverse linear-time lookup if not exact matching
		if (!f->mutable_match()->exact())
		  {
		    uit=matches_str->begin();
		    while(uit!=matches_str->end())
		      {
			if (token.find((*uit))==std::string::npos)
			  {
			    if (f->key() || f->mutable_match()->logic() == "and") {
			      return NULL;
			    }
			    else if (f->mutable_match()->logic() == "or") {
			      match = true; // has match specified, if no 'or' match condition kicks in, the data entry should be later killed
			    }
			    break;
			  }
			else
			  {
			    if (f->mutable_match()->logic() == "or")
			      {
				match = true;
				has_or_match = true;
				break;
			      }
			  }
			++uit;
		      }
		  }
	      }
	    else  // matching means killing
	      {
		std::unordered_set<std::string>::const_iterator uit;// = matches_str.begin();
		if ((uit=matches_str->find(token))!=matches_str->end())
		  {
		    if (f->key() || f->mutable_match()->logic() == "and")
		      return NULL;
		    else if (f->mutable_match()->logic() == "or")
		      match = true; // has match specified, if no 'or' match condition kicks in, the data entry should be later killed
		    uit = matches_str->end();
		  }

		
		// reverse linear-time lookup if not exact matching
		if (!f->mutable_match()->exact())
		  {
		    uit=matches_str->begin();
		    while(uit!=matches_str->end())
		      {
			if (token.find((*uit))!=std::string::npos)
			  {
			    if (f->key() || f->mutable_match()->logic() == "and") {
			      return NULL;
			    }
			    else break;
			  }
			++uit;
		      }
		  }
	      }
	  }

	if (ftype == "date" || f->processing() == "day" || f->processing() == "month" || f->processing() == "year")
	  {
	    struct tm tm;
	    bool datef_ok = false;
	    if (f->date_format() == "unix")
	      {
		time_t ut = std::stoi(token);
		gmtime_r(&ut,&tm);
		datef_ok = true;
	      }
	    else
	      {
		if (strptime(token.c_str(),f->date_format().c_str(),&tm) != NULL)
		  datef_ok = true;
	      }

	    if (datef_ok)
	      {
		if (f->processing() == "day")
		  {
		    token = std::to_string(tm.tm_year+1900) + "-" + std::to_string(tm.tm_mon+1) + "-" + std::to_string(tm.tm_mday);
		  }
		else if (f->processing() == "month")
		  token = std::to_string(tm.tm_year+1900) + "-" + std::to_string(tm.tm_mon+1);
		else if (f->processing() == "year")
		  token = std::to_string(tm.tm_year+1900);
		else if (f->processing() == "hour")
		  {
		    token = std::to_string(tm.tm_year+1900) + "-" + std::to_string(tm.tm_mon+1) + "-" + std::to_string(tm.tm_mday) + "T" + std::to_string(tm.tm_hour) + ":00:00";
		  }
		else if (f->processing() == "minute")
		  {
		    int m = tm.tm_min / f->processing_offset();
		    m *= f->processing_offset();
		    std::string mins_token = (m < 10 ? "0" : "") + std::to_string(m);
		    token = std::to_string(tm.tm_year+1900) + "-" + std::to_string(tm.tm_mon+1) + "-" + std::to_string(tm.tm_mday) + "T" + std::to_string(tm.tm_hour) + ":" + mins_token + ":00";
		  }
		else if (f->processing() == "second")
		  {
		    token = std::to_string(tm.tm_year+1900) + "-" + std::to_string(tm.tm_mon+1) + "-" + std::to_string(tm.tm_mday) + "T" + std::to_string(tm.tm_hour) + ":" + std::to_string(tm.tm_min) + ":" + std::to_string(tm.tm_sec);
		  }
	      }
	    else LOG(WARNING) << "Warning: unrecognized date format " << token << std::endl;
	  }
	else if (f->processing() == "hour" || f->processing() == "minute" || f->processing() == "second")
	  {
	    token = chomp_cpp(token);
	    std::vector <std::string> elts;
	    log_format::tokenize(token,-1,elts,":",""); // XXX: very basic tokenization of time fields of the form 14:39:02.
	    if (elts.size() == 3)
	      {
		if (f->processing() == "hour")
		  {
		    int h = std::stoi(elts.at(0)) / f->processing_offset();
		    h *= f->processing_offset();
		    token = (h < 10 ? "0" : "") + std::to_string(h);
		  }
		else if (f->processing() == "minute")
		  {
		    int m = std::stoi(elts.at(1)) / f->processing_offset();
		    m *= f->processing_offset();
		    token = elts.at(0) + ":" + (m < 10 ? "0" : "") + std::to_string(m);
		  }
		else if (f->processing() == "second")
		  {
		    int s = std::stoi(elts.at(2)) / f->processing_offset();
		    s *= f->processing_offset();
		    token = elts.at(0) + ":" + elts.at(1) + ":" + (s < 10 ? "0" : "") + std::to_string(s);
		  }
	      }
	    else LOG(WARNING) << "Warning: unrecognized time format " << token << std::endl;
	  }
	else if (ftype == "url")
	  {
	    // - parse field value into tokens
	    if (token.find("://[")==std::string::npos) // XXX: cppnetlib hangs on such URIs, possibly a consequence of the not working is_valid() call at the moment
	      {
		uri::uri uri_token(token);
		/*if (!uri_token.is_valid()) // validity appears to not be qualifiying urls properly, cppnetlib bug ?
		  {
		  LOG(WARNING) << "invalid URL " << token << std::endl;
		  continue;
		  }*/

		// fill out format with tokens
		if (!uri_token.scheme().empty())
		  {
		    std::string nuri = f->url_format();
		    str_utils::replace_in_string(nuri,"%scheme",uri_token.scheme());
		    str_utils::replace_in_string(nuri,"%host",uri_token.host());
		    if (!uri_token.port().empty())
		      str_utils::replace_in_string(nuri,"%port",":"+uri_token.port());
		    else str_utils::replace_in_string(nuri,"%port","");
		    str_utils::replace_in_string(nuri,"%path",uri_token.path());
		    str_utils::replace_in_string(nuri,"%query",uri_token.query());
		    str_utils::replace_in_string(nuri,"%fragment",uri_token.fragment());
		    token = nuri;
		  }
	      }
	  }

	// apply preprocessing (or not) to field according to type.
	if (ftype == "int")
	  {
	    int_field *ifi = f->mutable_int_fi();
	    ifi->add_int_reap(atoi(token.c_str()));
	    if (f->aggregation() == "variance")
	      ifi->add_int_reap(atoi(token.c_str()) * atoi(token.c_str()));
	    if (f->aggregated() && (f->aggregation() == "mean" or f->aggregation() == "variance"))
	      ifi->set_holder(1);
	  }
	else if (ftype == "string" || ftype == "date" || ftype == "time" || ftype == "url")
	  {
	    string_field *ifs = f->mutable_str_fi();
	    token = chomp_cpp(token);
	    if (!token.empty())
	      {
		ifs->add_str_reap(token);
		if (f->aggregated() && f->aggregation() == "union_count")
		  ifs->add_str_count(1);
		f->set_count(1);
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
	    if (f->aggregation() == "variance")
	      iff->add_float_reap(atof(token.c_str()) * atof(token.c_str()));
	    if (f->aggregated() && (f->aggregation() == "mean" or f->aggregation() == "variance"))
	      iff->set_holder(1.f);
	  }

	// pre-processing of field based on 'preprocessing' configuration field.
	if (f->preprocessing() == "evtxcsv")
	  {
	    pre_process_evtxcsv(f,token,nfields);
	  }
	else if (f->preprocessing() == "evtxcsv2")
	  {
	    pre_process_evtxcsv2(f,token,nfields);
	  }
	else if (f->preprocessing() == "microsoftdnslogs")
	  {
	    std::string tail;
	    for (size_t j=f->pos();j<tokens.size();j++)
	      tail += tokens.at(j);
	    pre_process_microsoftdnslogs(f,tail,nfields);
	  }

	// process fields that are part of the key.
	if (f->key())
	  {
	    if (!key.empty())
	      key += "_";
	    key += token;
	  }
      }

    // check on 'or' matching conditions
    if (match && !has_or_match)
      return NULL;

    // add new fields to ldef if any found in pre-processing step.
    for (size_t i=0;i<nfields.size();i++)
      {
	field *f = ldef.add_fields();
	*f = *nfields.at(i);
	delete nfields.at(i);
      }

    for (int i=0;i<ldef.fields_size();i++)
      {
	field *f = ldef.mutable_fields(i);
	if (f->filter_type() == "contain")
	  {
	    filter_contain(ldef,i);
	  }
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
				      std::vector<field*> &nfields) const
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
				       std::vector<field*> &nfields) const
  {
    std::vector<std::string> pairs;
    log_format::tokenize_simple(token,pairs,"[");
    for (size_t i=0;i<pairs.size();i++)
      {
	std::vector<std::string> elts;
	log_format::tokenize_simple(pairs.at(i),elts,":");
	if (elts.size() >= 2)
	  {
	    std::string ename = chomp_cpp(elts.at(0));
	    std::string val = chomp_cpp(elts.at(1));
	    if (elts.size() > 2)
	      {
		for (size_t j=2;j<elts.size();j++)
		  val += elts.at(j);
	      }
	    if (val.back() == ']')
	      {
		val.erase(val.length()-1);
	      }
	    field *nf = new field();
	    nf->set_name(ename);
	    nf->set_type("string");
	    string_field *ifs = nf->mutable_str_fi();
	    ifs->add_str_reap(val);
	    nfields.push_back(nf);
	  }
      }
    return 0;
  }

  int log_format::pre_process_microsoftdnslogs(field *f,
					       const std::string &token,
					       std::vector<field*> &nfields) const
  {
    std::string ctoken = chomp_cpp(token);
    std::string::size_type p = ctoken.find_first_of('(');
    if (p == std::string::npos)
      return 0;
    std::string val = ctoken.substr(p);

    bool first = true;
    int i = 0;
    std::string val_clean;
    while (i < static_cast<int>(val.length()))
    {
      if (val[i] == '(')
	{
	  std::string::size_type p = val.substr(i).find_first_of(')');
	  i+=p;
	  if (!first)
	    val_clean += '.';
	  else first = false;
	}
      else val_clean += val[i];
      ++i;
    }
    if (!val.empty())
      val_clean = val_clean.substr(0,val_clean.size()-1);

    field *nf = new field();
    nf->set_name("target");
    nf->set_type("string");
    string_field *ifs = nf->mutable_str_fi();
    ifs->add_str_reap(val_clean);
    nfields.push_back(nf);
    return 0;
  }

  bool log_format::filter_contain(logdef &ldef,
				  const int &i) const
  {
    field *f = ldef.mutable_fields(i);
    for (int j=0;j<ldef.fields_size();j++)
      {
	if (i == j)
	  continue;
	field *g = ldef.mutable_fields(j);
	if (g->pos() == f->pos() && g->filter().size() == 0)
	  {
	    int_field *ifi = f->mutable_int_fi();
	    if (g->type().compare("string")) {
	      LOG(WARNING) << "Warning: trying to use filter on "
			   << g->type() << " field (name: "
			   << g->name()
			   << ", pos: "
			   << g->pos() << "), only string is supported"
			   << std::endl;
	      ifi->add_int_reap(0);
	      continue;
	    }
	    if (g->str_fi().str_reap_size()) {
	      if (g->str_fi().str_reap(0).find(f->filter())!=std::string::npos)
		{
		  ifi->add_int_reap(1);
		  return true;
		}
	      else ifi->add_int_reap(0);
	    }
	  }
      }
    return false;
  }
  
}
