/**
 * Copyright (c) 2009 Emmanuel Benazera
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

#ifndef CURL_MGET_H
#define CURL_MGET_H

#include <string>
#include <vector>
#include <list>
#include <sstream>

#include <curl/curl.h>

template <class T>
static std::string to_string (const T& t)
{
  std::stringstream ss;
  ss << t;
  return ss.str();
};

  typedef struct _cbget
  {
    _cbget()
    :_url(NULL),_output(NULL),_proxy_port(0),_headers(NULL),_status(0),_http_code(0),_handler(NULL),
       _content(NULL),_content_size(-1)
    {};

    ~_cbget()
    {};

    const char *_url;
    std::string *_output;

    long _connect_timeout_sec;
    long _transfer_timeout_sec;
    std::string _proxy_addr;
    short _proxy_port;
    const std::list<const char*> *_headers; // forced http headers
    int _status;
    long _http_code;
    CURL *_handler; // optional
    std::string _cookies; // optional
    std::string _http_method; // optional
    std::string *_content; // optional
    int _content_size; // optional
    std::string _content_type; // optional.
  } cbget;

  void* pull_one_url(void *arg_cbget);

  class curl_mget
  {
    public:
      curl_mget(const int &nrequests,
                const long &connect_timeout_sec,
                const long &connect_timeout_ms,
                const long &transfer_timeout_sec,
                const long &transfer_timeout_ms);

      ~curl_mget();

      // direct connection.
      std::string** www_mget(const std::vector<std::string> &urls, const int &nrequests,
                             const std::vector<std::list<const char*>*> *headers,
                             const std::string &proxy_addr, const short &proxy_port,
                             std::vector<int> &status,
			     std::vector<long> &http_codes,
                             std::vector<CURL*> *chandlers=NULL,
                             std::vector<std::string> *cookies=NULL,
                             const std::string &http_method="GET",
                             std::string *content=NULL,
                             const int &content_size=-1,
                             const std::string &content_type="");

      std::string* www_simple(const std::string &url,
                              std::list<const char*> *headers,
                              int &status,
			      long &http_code,
                              const std::string &http_method="GET",
                              std::string *content=NULL,
                              const int &content_size=-1,
                              const std::string &content_type="",
                              const std::string &proxy_addr="",
                              const short &proxy_port=0);
    public:
      int _nrequests;
      long _connect_timeout_sec;
      long _connect_timeout_ms;
      long _transfer_timeout_sec;
      long _transfer_timeout_ms;
      std::string _lang;
      const std::list<const char*> *_headers; // forced http headers.

      std::string **_outputs;
      cbget **_cbgets;
  };

#endif
