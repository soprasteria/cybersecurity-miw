/**
 * Copyright (c) 2015 SopraSteria
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

#include "job.h"
#include <gtest/gtest.h>
#include <stdio.h>

using namespace miw;

std::string path = "../";

/*TEST(job,proxy_format)
{
  job j;
  std::string arg_line = "-fnames ../data/pxyinternet-10lines.log.orig.anon -format_name ../miw/formats/proxy_format -output_format mem";
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);
  ASSERT_EQ(2,j._results->size());
}*/

/*TEST(job,domain_controller_format)
{
  job j;
  std::string arg_line = "-fnames ../data/domain_controller_100lines_test.log -format_name ../miw/formats/domain_controller_format -output_format mem --skip_header";
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);
  ASSERT_EQ(4,j._results->size());
}*/

/*TEST(job,evtx)
{
  job j;
  std::string arg_line = "-fnames ../data/SecuritySample_10.csv -format_name ../miw/formats/evtx -output_format mem";
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);
  ASSERT_EQ(1,j._results->size());
  //TODO: check on per field result
}*/

/*TEST(job,evtx2)
{
  job j;
  std::string arg_line = "-fnames ../data/SecuritySample_10_2.csv -format_name ../miw/formats/evtx2 -output_format mem";
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);
  ASSERT_EQ(1,j._results->size());
  //TODO: check on per field result
}*/

/*TEST(job,firewall_checkpoint)
{
  job j;
  std::string arg_line = "-fnames ../data/fw_checkpoint_100lines.log -format_name ../miw/formats/firewall_checkpoint -output_format mem";
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);
  ASSERT_EQ(1,j._results->size());
  }*/

/*TEST(job,allCiscoIportwsa)
{
  job j;
  std::string arg_line = "-fnames ../data/RSSallCisco10.csv -format_name ../miw/formats/allCiscoIportwsa -output_format mem";
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);
  ASSERT_EQ(7,j._results->size());
  //TODO: check on per field result
}*/

TEST(job,testVariance)
{
  job j;
  char tmp_ouputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_ouputfile));
  
  std::string arg_line = "-fnames ../data/tests/variance.log -format_name ../miw/formats/tests/variance -output_format json -map_tasks 2 -ofname ";
  arg_line.append(tmp_ouputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_ouputfile);
  if (!jsonfile.good())
    remove(tmp_ouputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);

  remove(tmp_ouputfile);

  /**
   https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
   ---
   import numpy as np
   a = np.array([3,1,5,3,2,2])
   (np.sum(a**2) - (a.sum() * a.sum()) / float(len(a))) / (len(a) - 1)
   ==> 1.8666666666666671
   */
  ASSERT_NE(first_line.find("\"var\":1.8666666666666671"), std::string::npos);
  //TODO: check on per field result
}

TEST(job,testVarianceMean)
{
  job j;
  char tmp_ouputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_ouputfile));

  std::string arg_line = "-fnames ../data/tests/variance.log -format_name ../miw/formats/tests/variance-mean-sum -output_format json --map_tasks 2 -ofname ";
  arg_line.append(tmp_ouputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_ouputfile);
  if (!jsonfile.good())
    remove(tmp_ouputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);

  remove(tmp_ouputfile);

  ASSERT_NE(first_line.find("\"variance\":1.8666666666666671"), std::string::npos);
  ASSERT_NE(first_line.find("\"mean\":2.6666666666666665"), std::string::npos);
  ASSERT_NE(first_line.find("\"sum\":16"), std::string::npos);
  //TODO: check on per field result
}

TEST(job,testFilter)
{
  job j;
  char tmp_ouputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_ouputfile));

  std::string arg_line = "-fnames ../data/tests/string.log -format_name ../miw/formats/tests/filter -output_format json -map_tasks 2 -ofname ";
  arg_line.append(tmp_ouputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_ouputfile);
  if (!jsonfile.good())
    remove(tmp_ouputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);

  remove(tmp_ouputfile);

  ASSERT_NE(first_line.find("\"denied_count\":2"), std::string::npos);
  ASSERT_NE(first_line.find("\"ok_count\":3"), std::string::npos);
  //TODO: check on per field result
}

TEST(job,testMatch)
{
  job j;
  char tmp_outputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_outputfile));
  std::cerr << "TMPFILE=" << tmp_outputfile << std::endl;
  
  std::string arg_line = "-fnames ../data/tests/matching.log -format_name ../miw/formats/tests/match -output_format json -map_tasks 2 -ofname ";
  arg_line.append(tmp_outputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_outputfile);
  if (!jsonfile.good())
    remove(tmp_outputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);
  std::string second_line;
  std::getline(jsonfile, second_line);

  remove(tmp_outputfile);

  ASSERT_NE(first_line.find("\"val\":\"OOKK\""), std::string::npos);
  ASSERT_NE(second_line.find("\"val\":\"OOKK\""), std::string::npos);
}

TEST(job,testMatchNeg)
{
  job j;
  char tmp_outputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_outputfile));
  std::cerr << "TMPFILE=" << tmp_outputfile << std::endl;

  std::string arg_line = "-fnames ../data/tests/matching.log -format_name ../miw/formats/tests/match_neg -output_format json -map_tasks 2 -ofname ";
  arg_line.append(tmp_outputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_outputfile);
  if (!jsonfile.good())
    remove(tmp_outputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);
  std::string second_line;
  std::getline(jsonfile, second_line);

  remove(tmp_outputfile);

  ASSERT_NE(first_line.find("\"val\":\"OOKK\""), std::string::npos);
  ASSERT_NE(second_line.find("\"val\":\"OOKK\""), std::string::npos);
}

TEST(job,testMatchExact)
{
  job j;
  char tmp_outputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_outputfile));
  std::cerr << "TMPFILE=" << tmp_outputfile << std::endl;
  
  std::string arg_line = "-fnames ../data/tests/matching_exact.log -format_name ../miw/formats/tests/match_exact -output_format json -map_tasks 2 -ofname ";
  arg_line.append(tmp_outputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_outputfile);
  if (!jsonfile.good())
    remove(tmp_outputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);
  std::string second_line;
  std::getline(jsonfile, second_line);
    
  remove(tmp_outputfile);

  std::cerr << "first_line=" << first_line << std::endl;
  std::cerr << "second_line=" << second_line << std::endl;

  // lines can come in any order so we check for OK everywhere
  ASSERT_TRUE((first_line.find("\"val\":\"OK\"") != std::string::npos) && (second_line.find("\"val\":\"OK\"") != std::string::npos));
}

TEST(job,testMatchExactNeg)
{
  job j;
  char tmp_outputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_outputfile));
  std::cerr << "TMPFILE=" << tmp_outputfile << std::endl;
  
  std::string arg_line = "-fnames ../data/tests/matching_exact.log -format_name ../miw/formats/tests/match_exact_neg -output_format json -map_tasks 2 -ofname ";
  arg_line.append(tmp_outputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_outputfile);
  if (!jsonfile.good())
    remove(tmp_outputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);
  std::string second_line;
  std::getline(jsonfile, second_line);
  std::string third_line;
  std::getline(jsonfile, third_line);
    
  remove(tmp_outputfile);

  std::cerr << "first_line=" << first_line << std::endl;
  std::cerr << "second_line=" << second_line << std::endl;
  std::cerr << "third_line=" << third_line << std::endl;

  // lines can come in any order so we check for OK and KO2 everywhere
  ASSERT_TRUE((first_line.find("\"val\":\"OK\"") != std::string::npos) || (second_line.find("\"val\":\"OK\"") != std::string::npos) || (third_line.find("\"val\":\"OK\"") != std::string::npos));
  ASSERT_TRUE((first_line.find("\"val\":\"KO2\"") != std::string::npos) || (second_line.find("\"val\":\"KO2\"") != std::string::npos) || (third_line.find("\"val\":\"KO2\"") != std::string::npos));
}

TEST(job,testSum)
{
  job j;
  char tmp_outputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_outputfile));
  std::cerr << "TMPFILE=" << tmp_outputfile << std::endl;

  std::string arg_line = "-fnames ../data/tests/sum.log -format_name ../miw/formats/tests/sum -output_format json -map_tasks 2 -ofname ";
  arg_line.append(tmp_outputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_outputfile);
  if (!jsonfile.good())
    remove(tmp_outputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);

  remove(tmp_outputfile);

  ASSERT_NE(first_line.find("\"v1\":16"), std::string::npos);
  ASSERT_NE(first_line.find("\"v2\":17"), std::string::npos);
}

TEST(job,testMatchRatio)
{
  job j;
  char tmp_outputfile[L_tmpnam];

  ASSERT_NE(NULL, tmpnam(tmp_outputfile));
  std::cerr << "TMPFILE=" << tmp_outputfile << std::endl;
  
  std::string arg_line = "-fnames ../data/tests/ratio.log -format_name ../miw/formats/tests/ratio -output_format json -map_tasks 2 -ofname ";
  arg_line.append(tmp_outputfile);
  std::vector<std::string> args;
  log_format::tokenize(arg_line,-1,args," ","");
  char* cargs[args.size()+1];
  cargs[0] = "miw";
  for (size_t i=0;i<args.size();i++)
    cargs[i+1] = const_cast<char*>(args.at(i).c_str());
  j.execute(args.size()+1,cargs);

  std::ifstream jsonfile(tmp_outputfile);
  if (!jsonfile.good())
    remove(tmp_outputfile);
  ASSERT_EQ(true, jsonfile.good());

  std::string first_line;
  std::getline(jsonfile, first_line);

  ASSERT_NE(first_line.find("\"fratio\":0.57142859697341919"), std::string::npos);
  ASSERT_NE(first_line.find("\"iratio\":0.57142859697341919"), std::string::npos);
  ASSERT_NE(first_line.find("\"tratio\":2.6666667461395264"), std::string::npos);
}
