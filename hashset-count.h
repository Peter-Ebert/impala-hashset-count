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


#ifndef SAMPLES_UDA_H
#define SAMPLES_UDA_H

#include <impala_udf/udf.h>

using namespace impala_udf;

// This is an example of the AVG(double) aggregate function. This function needs to
// maintain two pieces of state, the current sum and the count. We do this using
// the StringVal intermediate type. When this UDA is registered, it would specify
// 16 bytes (8 byte sum + 8 byte count) as the size for this buffer.
//
// Usage: > create aggregate function my_avg(double) returns string 
//          location '/user/cloudera/libudasample.so' update_fn='AvgUpdate';
//        > select cast(my_avg(col) as double) from tbl;
void DistHashSetInit300k(FunctionContext* context, StringVal* val);
void DistHashSetUpdate(FunctionContext* context, const StringVal& input, StringVal* val);
void DistHashSetMerge(FunctionContext* context, const StringVal& src, StringVal* dst);
const StringVal DistHashSetSerialize(FunctionContext* context, const StringVal& val);
IntVal DistHashSetFinalize(FunctionContext* context, const StringVal& val);




// Utility function for serialization to StringVal
template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val);

#endif
