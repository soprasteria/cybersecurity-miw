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

#include "curl_mget.h"

#include <pthread.h>

#include <stdlib.h>
#include <string.h>
#include <iostream>

#include <assert.h>


  static size_t write_data(void *ptr, size_t size, size_t nmemb, void *userp)
  {
    char *buffer = static_cast<char*>(ptr);
    cbget *arg = static_cast<cbget*>(userp);
    size *= nmemb;

    if (!arg->_output)
      arg->_output = new std::string();

    arg->_output->append(buffer,size);

    return size;
  }

  curl_mget::curl_mget(const int &nrequests,
                       const long &connect_timeout_sec,
                       const long &connect_timeout_ms,
                       const long &transfer_timeout_sec,
                       const long &transfer_timeout_ms)
    :_nrequests(nrequests),_connect_timeout_sec(connect_timeout_sec),
     _connect_timeout_ms(connect_timeout_ms),_transfer_timeout_sec(transfer_timeout_sec),
     _transfer_timeout_ms(transfer_timeout_ms)
  {
    _outputs = new std::string*[_nrequests];
    for (int i=0; i<_nrequests; i++)
      _outputs[i] = NULL;
    _cbgets = new cbget*[_nrequests];
  }

  curl_mget::~curl_mget()
  {
    delete[] _cbgets;
  }

  void* pull_one_url(void *arg_cbget)
  {
    if (!arg_cbget)
      return NULL;

    cbget *arg = static_cast<cbget*>(arg_cbget);

    CURL *curl = NULL;

    if (!arg->_handler)
      {
        curl = curl_easy_init();
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 100);
        curl_easy_setopt(curl, CURLOPT_MAXREDIRS,100);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0); // do not check on SSL certificate.
      }
    else curl = arg->_handler;

    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, arg->_connect_timeout_sec);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, arg->_transfer_timeout_sec);
    curl_easy_setopt(curl, CURLOPT_URL, arg->_url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, arg);

    if (!arg->_cookies.empty())
      curl_easy_setopt(curl, CURLOPT_COOKIE, arg->_cookies.c_str());

    if (!arg->_proxy_addr.empty())
      {
        std::string proxy_str = arg->_proxy_addr + ":" + to_string(arg->_proxy_port);
        curl_easy_setopt(curl, CURLOPT_PROXY, proxy_str.c_str());
      }

    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, arg->_http_method.c_str());
    if (arg->_http_method == "POST"
	|| arg->_http_method == "PUT") // POST request.
      {
        if (arg->_content)
          {
            //curl_easy_setopt(curl, CURLOPT_POST, 1);
	    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, arg->_http_method.c_str());
	    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, (void*)arg->_content->c_str());
            if (arg->_content_size >= 0)
              curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, arg->_content_size);
            /*std::cerr << "curl_mget POST size: " << arg->_content_size << std::endl
              << "content: " << *arg->_content << std::endl;*/
          }
      }
    
    struct curl_slist *slist=NULL;

    // useful headers.
    std::vector<char*> added_headers;
    if (arg->_headers)
      {
        std::list<const char*>::const_iterator sit = arg->_headers->begin();
        while (sit!=arg->_headers->end())
          {
            slist = curl_slist_append(slist,(*sit));
            ++sit;
          }
        if (arg->_content)
          {
	    std::string header_ct = "content-type:" + arg->_content_type;
            char *h = strdup(header_ct.c_str());
	    added_headers.push_back(h);
	    slist = curl_slist_append(slist,h);
	    h = strdup("Expect:");
            added_headers.push_back(h);
	    slist = curl_slist_append(slist,h);
          }
      }
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);

    char errorbuffer[CURL_ERROR_SIZE];
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, &errorbuffer);

    try
      {
        int status = curl_easy_perform(curl);
        if (status != 0)  // an error occurred.
          {
            arg->_status = status;
	    std::cerr << "curl error on url: " << arg->_url << ": " << errorbuffer << std::endl;

            if (arg->_output)
              {
                delete arg->_output;
                arg->_output = NULL;
              }
          }
      }
    catch (std::exception &e)
      {
        arg->_status = -1;
	std::cerr << "Error " << e.what() << " in fetching remote data with curl.\n";
	if (arg->_output)
          {
            delete arg->_output;
            arg->_output = NULL;
          }
      }
    catch (...)
      {
        arg->_status = -2;
        if (arg->_output)
          {
            delete arg->_output;
            arg->_output = NULL;
          }
      }

  for (size_t i=0;i<added_headers.size();i++)
    free(added_headers.at(i));

    curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&arg->_http_code);

    if (!arg->_handler)
      curl_easy_cleanup(curl);

    if (slist)
      curl_slist_free_all(slist);

    return NULL;
  }

  std::string** curl_mget::www_mget(const std::vector<std::string> &urls, const int &nrequests,
                                    const std::vector<std::list<const char*>*> *headers,
                                    const std::string &proxy_addr, const short &proxy_port,
                                    std::vector<int> &status,
                                    std::vector<long> &http_codes,
				    std::vector<CURL*> *chandlers,
                                    std::vector<std::string> *cookies,
                                    const std::string &http_method,
                                    std::string *content,
                                    const int &content_size,
                                    const std::string &content_type)
  {
    assert((int)urls.size() == nrequests); // check.

    pthread_t tid[nrequests];

    /* Must initialize libcurl before any threads are started */
    curl_global_init(CURL_GLOBAL_ALL);

    for (int i=0; i<nrequests; i++)
      {
        cbget *arg_cbget = new cbget();
        arg_cbget->_url = urls[i].c_str();
        arg_cbget->_transfer_timeout_sec = _transfer_timeout_sec;
        arg_cbget->_connect_timeout_sec = _connect_timeout_sec;
        arg_cbget->_proxy_addr = proxy_addr;
        arg_cbget->_proxy_port = proxy_port;
        if (headers)
          arg_cbget->_headers = headers->at(i); // headers are url dependent.
        if (chandlers)
          arg_cbget->_handler = chandlers->at(i);
        if (cookies)
          arg_cbget->_cookies = cookies->at(i);
        arg_cbget->_http_method = http_method;
        if (content)
          {
	    if (!headers)
	      {
		// headers are required for handling content_type.
		arg_cbget->_headers = new std::list<const char*>();
	      }
            arg_cbget->_content = content;
            arg_cbget->_content_size = content_size;
	    arg_cbget->_content_type = content_type;
	  }
        _cbgets[i] = arg_cbget;

        int error = pthread_create(&tid[i],
                                   NULL, /* default attributes please */
                                   pull_one_url,
                                   (void *)arg_cbget);

        if (error != 0)
	  std::cerr << "Couldn't run thread number " << i << std::endl;
      }

    /* now wait for all threads to terminate */
    for (int i=0; i<nrequests; i++)
      {
        pthread_join(tid[i], NULL);
      }

    for (int i=0; i<nrequests; i++)
      {
        _outputs[i] = _cbgets[i]->_output;
        status.push_back(_cbgets[i]->_status);
        http_codes.push_back(_cbgets[i]->_http_code);
	if (!headers && _cbgets[i]->_headers)
	  delete _cbgets[i]->_headers;
	delete _cbgets[i];
      }

    return _outputs;
  }

  std::string* curl_mget::www_simple(const std::string &url,
                                     std::list<const char*> *headers,
                                     int &status,
                                     long &http_code,
				     const std::string &http_method,
                                     std::string *content,
                                     const int &content_size,
                                     const std::string &content_type,
                                     const std::string &proxy_addr,
                                     const short &proxy_port)
  {
    std::vector<std::string> urls;
    urls.reserve(1);
    urls.push_back(url);
    std::vector<int> statuses;
    std::vector<long> http_codes;
    std::vector<std::list<const char*>*> *lheaders = NULL;
    if (headers)
      {
        lheaders = new std::vector<std::list<const char*>*>();
        lheaders->push_back(headers);
      }
    www_mget(urls,1,lheaders,proxy_addr,proxy_port,statuses,http_codes,NULL,NULL,http_method,
	     content,content_size,content_type);
    if (statuses[0] != 0)
      {
        // failed connection.
        status = statuses[0];
        delete[] _outputs;
        if (headers)
          delete lheaders;
        std::string msg = "failed connection to " + url;
	std::cerr << msg << std::endl;
        return NULL;
      }
    else if (statuses[0] == 0 && !_outputs)
      {
        // no result.
        status = statuses[0];
        std::string msg = "no output from " + url;
        if (headers)
          delete lheaders;
	std::cerr << msg << std::endl;
	delete _outputs[0];
        delete[] _outputs;
        return NULL;
      }
    status = statuses[0];
    http_code = http_codes[0];
    std::string *result = _outputs[0];
    delete[] _outputs;
    if (headers)
      delete lheaders;
    return result;
  }
