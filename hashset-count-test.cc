//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#include <iostream>
#include <math.h>

#include <impala_udf/uda-test-harness.h>
#include "hashset-count.h"

using namespace impala;
using namespace impala_udf;
using namespace std;


bool TestHashSetCount() {
  UdaTestHarness<StringVal, StringVal, StringVal> test(
      DistHashSetInit, DistHashSetUpdate, DistHashSetMerge, DistHashSetSerialize, DistHashSetFinalize);
  test.SetIntermediateSize(32);

  vector<StringVal> vals;

  // Test empty input
  if (!test.Execute<StringVal>(vals, StringVal::null())) {
    cerr << "DHS empty: " << test.GetErrorMsg() << endl;
    return false;
  }

  //Test Multiple values
  vals.push_back("Hello");
  vals.push_back("");
  vals.push_back("World");
  vals.push_back("Hello");
  vals.push_back("costarring");
  vals.push_back("liquid");  
  
  if (!test.Execute<StringVal>(vals, StringVal("5"))) {
    cerr << "DHS: " << test.GetErrorMsg() << endl;
    return false;
  }

  return true;
}



int main(int argc, char** argv) {
  bool passed = true;
  passed &= TestHashSetCount();
  cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
  return 0;
}
