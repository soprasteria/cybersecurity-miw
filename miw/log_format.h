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

/**
 * Generic framework for defining and processing log formats.
 */

#include "log_record.h"
#include "log_definition.pb.h"
#include <vector>
#include <string>

namespace miw
{

  class log_format
  {
  public:
    log_format();
    ~log_format();

    int save();
    
    int read(const std::string &name);
    
    bool check() const;

    static void tokenize(const std::string &str,
			 const int &length,
			 std::vector<std::string> &tokens,
			 const std::string &delim);
    
    int parse_data(const std::string &data,
		   const int &length,
		   std::vector<log_record*> &lrecords) const;

    log_record* parse_line(const std::string &line,
			   int &skipped_logs) const;
    
    logdef _ldef;  // protocol buffer object.
  };
  
}
