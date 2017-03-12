// Abandon hope all ye who enter here
// code has not been cleaned up at all, TURN BACK NOW

//Caveats:
//  WILL NOT work with strings containing \0 (NULL)
//  May not work with spill to disk, then again it may
//

#include "hashset-count.h"
#include <assert.h>
#include <sstream>
#include <algorithm>

using namespace impala_udf;
using namespace std;

//for converting final value to string, needed for return type bug where intermediate data type must match final
template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val) {
  stringstream ss;
  ss << val;
  string str = ss.str();
  StringVal string_val(context, str.size());
  memcpy(string_val.ptr, str.c_str(), str.size());
  return string_val;
}

template <>
StringVal ToStringVal<DoubleVal>(FunctionContext* context, const DoubleVal& val) {
  if (val.is_null) return StringVal::null();
  return ToStringVal(context, val.val);
}


//// ---------------------------------------------------------------------------
//   HashSet Distinct Count
//// ---------------------------------------------------------------------------

// Hash Function
//todo: murmur3 would be faster, but FNV is easier to implement
// costarring collides with liquid
// declinate collides with macallums
// altarage collides with zinke
// altarages collides with zinkes

static const uint64_t FNV64_PRIME = 1099511628211UL;
static const uint64_t FNV64_SEED = 14695981039346656037UL;

static uint64_t FnvHash(const void* data, int32_t bytes, uint64_t hash) {
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
  while (bytes--) {
    hash = (*ptr ^ hash) * FNV64_PRIME;
    ++ptr;
  }
  return hash;
}

//HashTable
// static const IntVal UPDATE_BUCKETS = 200000;
// static const IntVal FINALIZE_BUCKETS = 300000;
//Note: seperator cannot be present in source strings (will cause bad counts)
static const StringVal STRING_SEPARATOR((uint8_t*)"\0", 1); //"\0"
static const uint8_t MAGIC_BYTE_DHS = 'H';
static const uint8_t MAGIC_BYTE_DELIMSTR = 'D';
static const uint8_t MAGIC_BYTE_SIZE = 1;
static const int BUCKET_COUNT = 300000;
//static const StringVal MAGIC_BYTE_DELIMSTR((uint8_t*)255, 1);

struct DistHashSet {
  //DistHashSet(): buckets(NULL) {}
  //add check to ensure data is well formed? length at start of string?
  uint8_t magic_byte;
  //to test: handle empty string
  int bucket_count;
  bool buckets_need_alloc;
  StringVal** buckets;
};


// Initialize the StringVal intermediate to a zero'd DistHashSet
void DistHashSetInit300k(FunctionContext* context, StringVal* strvaldhs) {
  strvaldhs->is_null = false;
  strvaldhs->len = sizeof(DistHashSet);
  strvaldhs->ptr = context->Allocate(strvaldhs->len);
  if (!strvaldhs->ptr) {
    //allocation failed
    context->SetError("HashSetCount: Init memory allocation failed.");
    strvaldhs->is_null = true;
    strvaldhs->len = 0;
    return;
  }
  memset(strvaldhs->ptr, 0, strvaldhs->len);

  DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs->ptr);
  //set magic byte
  dhs->magic_byte = MAGIC_BYTE_DHS;

  //could also be moved to update to avoid spinup for finalize where not needed
  dhs->bucket_count = BUCKET_COUNT;
  dhs->buckets_need_alloc = true;

}

void DistHashSetUpdate(FunctionContext* context, const StringVal& str, StringVal* strvaldhs) {
  if (str.is_null) return;
  assert(!strvaldhs->is_null);
  assert(strvaldhs->len == sizeof(DistHashSet));
  DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs->ptr);
  
  if (dhs->buckets_need_alloc) {
    //allocate memory for buckets
    dhs->buckets = (StringVal **) context->Allocate(sizeof(StringVal *) * dhs->bucket_count);
    if (!dhs->buckets) {
      //allocation failed
      context->SetError("HashSetCount: Bucket array memory allocation failed.");
      return;
    }
    memset(dhs->buckets, 0, sizeof(StringVal *) * dhs->bucket_count);
    dhs->buckets_need_alloc = false;
  }

  uint64_t mybucket = FnvHash(str.ptr, str.len, FNV64_SEED) % dhs->bucket_count;
  if(!dhs->buckets[mybucket]) {
    // First entry in bucket
    // allocate bucket memory
    dhs->buckets[mybucket] = (StringVal*) context->Allocate(sizeof(StringVal));
    if (!dhs->buckets[mybucket]) {
      //allocation failed
      context->SetError("HashSetCount: Bucket stringval memory allocation failed.");
      return;
    }
    // copy str+delimiter into bucket
    int new_len = STRING_SEPARATOR.len + str.len;
    uint8_t* copy = context->Allocate(new_len);
    if (!copy) {
      //allocation failed
      context->SetError("HashSetCount: Bucket contents memory allocation failed.");
      dhs->buckets[mybucket]->is_null = true;
      dhs->buckets[mybucket]->len = 0;
      dhs->buckets[mybucket]->ptr = copy;
      return;
    }
    memcpy(copy, str.ptr, str.len);
    memcpy(copy+str.len, STRING_SEPARATOR.ptr, STRING_SEPARATOR.len);
    dhs->buckets[mybucket]->is_null = false;
    dhs->buckets[mybucket]->len = new_len;
    dhs->buckets[mybucket]->ptr = copy;

  } else {
    // Collision
    // search bucket for duplicate, add if not found in existing bucket

    uint8_t* loc_start = dhs->buckets[mybucket]->ptr;
    uint8_t* loc_delim;
    uint8_t* bucket_end = dhs->buckets[mybucket]->ptr + dhs->buckets[mybucket]->len;
    bool match_found = false;

    while (loc_start < bucket_end) {

      loc_delim = (uint8_t*)memchr(loc_start, *STRING_SEPARATOR.ptr, bucket_end - loc_start);

      if ( (str.len) == (loc_delim - loc_start) ) {
        if (!memcmp(str.ptr, loc_start, str.len)) {
          // strings identical, mark found, exit loop
          match_found = true;
          loc_delim = bucket_end;
        }
      }

      loc_start = loc_delim + STRING_SEPARATOR.len;
    }

    if (!match_found) {
      //append
      int new_len = dhs->buckets[mybucket]->len + str.len + STRING_SEPARATOR.len;
      dhs->buckets[mybucket]->ptr = context->Reallocate(dhs->buckets[mybucket]->ptr, new_len);
      if (!dhs->buckets[mybucket]->ptr) {
        //allocation failed
        //!todo!!not working
        context->SetError("HashSetCount: Bucket contents reallocation failed.");
        dhs->buckets[mybucket]->is_null = true;
        dhs->buckets[mybucket]->len = 0;
        return;
      }
      memcpy(dhs->buckets[mybucket]->ptr + dhs->buckets[mybucket]->len, str.ptr, str.len);
      //add delim
      memcpy(dhs->buckets[mybucket]->ptr + new_len - STRING_SEPARATOR.len, STRING_SEPARATOR.ptr, STRING_SEPARATOR.len);
      dhs->buckets[mybucket]->len = new_len;
    }
  }

}


const StringVal DistHashSetSerialize(FunctionContext* context, const StringVal& strvaldhs) {
  //ensure this is a disthashset
  //assert(strvaldhs.len == sizeof(DistHashSet));
  StringVal temp;
  
  if (strvaldhs.ptr[0] == MAGIC_BYTE_DELIMSTR) {
    //intermediate type is delimited string
    context->AddWarning("Spilling may have happened, not guarunteed to be safe for this function (DistHashSetSerialize).");

    temp.ptr = strvaldhs.ptr;
    temp.len = strvaldhs.len;
  } else {
    //intermediate type is hashset
    temp.ptr = context->Allocate(sizeof(MAGIC_BYTE_DELIMSTR));
    if (!temp.ptr) {
      //allocation failed
      //!todo:doesnt work mem leak
      context->SetError("HashSetCount: Serialize allocation failed.");
      context->Free(strvaldhs.ptr);
      return StringVal::null();
    }

    memcpy(temp.ptr, &MAGIC_BYTE_DELIMSTR, sizeof(MAGIC_BYTE_DELIMSTR));
    temp.len = sizeof(MAGIC_BYTE_DELIMSTR);
    temp.is_null = false;

    DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs.ptr);
    
    //build serialized string and free memory as we go
    if(!dhs->buckets_need_alloc) {
      //check mem allocation
      for (int i = 0; i < dhs->bucket_count; i++) {
        if (dhs->buckets[i]) {
          if (dhs->buckets[i]->ptr) {
            //always append to list, seperator already added.
            int new_len = temp.len + dhs->buckets[i]->len;
            temp.ptr = context->Reallocate(temp.ptr, new_len);
            //!todo:doesnt work mem leak
            if (!temp.ptr) {
              //allocation failed
              context->SetError("HashSetCount: Serialize reallocation failed.");
              return StringVal::null();
            }
            memcpy(temp.ptr + temp.len, dhs->buckets[i]->ptr, dhs->buckets[i]->len);
            temp.len = new_len;
            
            //free bucket ptrs
            context->Free((uint8_t*) dhs->buckets[i]->ptr);  
          }  
          //free buckets contents
          context->Free((uint8_t*) dhs->buckets[i]);
        }  
      }
      // free buckets array
      context->Free((uint8_t*) dhs->buckets);
    }
    
    // free struct
    context->Free(strvaldhs.ptr);
    // All Memory Freed
  }

  // finalize result
  StringVal result(context, temp.len);
  memcpy(result.ptr, temp.ptr, temp.len);
  context->Free(temp.ptr);
  return result;
}


//todo: rename intermediate
//todo: add error handling for out of order buckets (would indicate error in merging or input, e.g. \0 in the string
// simply loop through the list and append where no duplicates, use larger list (dst) and loop through smaller (src) for memmbership until greater hash value detected
// finalize the large combined string by counting each value or /0
void DistHashSetMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  //if string contains only magic byte there are no values in the list, can safely return
  if (src.len <= 1 || !dst->ptr) return;

  if (dst->ptr[0] == MAGIC_BYTE_DHS) { //todo:move to end, less likely than other if
    //init was run for dhs, drop and set equal to current string to be merged
    //should happen once per merge
    context->Free(dst->ptr);
    uint8_t* copy = context->Allocate(src.len);
    if (!copy) {
      //allocation failed
      context->SetError("HashSetCount: Merge initial allocation failed.");
      return;
    }
    memcpy(copy, src.ptr, src.len);
    *dst = StringVal(copy, src.len);

  } else if (dst->ptr[0] == MAGIC_BYTE_DELIMSTR) {
    //note:technically if size changed [0] would error
    //merge delimited strings

    //to avoid having to grow the buffer, set it to the max possible size (shrink at end)
    uint8_t* merge_buffer = context->Allocate(src.len + (dst->len - MAGIC_BYTE_SIZE));
    if (!merge_buffer) {
      //allocation failed
      context->SetError("HashSetCount: Merge buffer memory allocation failed.");
      return;
    }
    memcpy(merge_buffer, &MAGIC_BYTE_DELIMSTR, MAGIC_BYTE_SIZE);
    uint8_t* buffer_loc = merge_buffer + MAGIC_BYTE_SIZE;

    // initial values - will always be at least one item in delim list
    uint8_t* src_end = src.ptr + src.len;
    uint8_t* dst_end = dst->ptr + dst->len;

    //cur_loc always points to the the start of the string
    uint8_t* src_cur_loc = src.ptr + sizeof(MAGIC_BYTE_DELIMSTR);
    uint8_t* dst_cur_loc = dst->ptr + sizeof(MAGIC_BYTE_DELIMSTR);

    uint8_t* src_chunk_start = src_cur_loc;
    uint8_t* dst_chunk_start = dst_cur_loc;//needed?

    //next_loc always points to the location after the delimiter (begining of next string)
    uint8_t* src_next_loc = (uint8_t*)memchr(src_cur_loc, *STRING_SEPARATOR.ptr, src_end - src_cur_loc) + STRING_SEPARATOR.len;
    uint8_t* dst_next_loc = (uint8_t*)memchr(dst_cur_loc, *STRING_SEPARATOR.ptr, dst_end - dst_cur_loc) + STRING_SEPARATOR.len;

    uint64_t src_bucket_val = FnvHash(src_cur_loc, (src_next_loc - STRING_SEPARATOR.len) - src_cur_loc, FNV64_SEED) % BUCKET_COUNT;
    uint64_t dst_bucket_val = FnvHash(dst_cur_loc, (dst_next_loc - STRING_SEPARATOR.len) - dst_cur_loc, FNV64_SEED) % BUCKET_COUNT;

    uint64_t src_next_bucket_val = src_bucket_val;
    uint64_t dst_next_bucket_val = dst_bucket_val;

    do {
      if (dst_bucket_val < src_bucket_val) { // OR at end of dst?
        //keep going until > src_bucket or end of dst
        
        while (dst_bucket_val < src_bucket_val && dst_next_loc < dst_end) {
          //keep searching
          dst_cur_loc = dst_next_loc;
          
          if (dst_cur_loc < dst_end) {
            dst_next_loc = (uint8_t*)memchr(dst_cur_loc, *STRING_SEPARATOR.ptr, dst_end - dst_cur_loc) + STRING_SEPARATOR.len;
            dst_bucket_val = FnvHash(dst_cur_loc, (dst_next_loc - STRING_SEPARATOR.len) - dst_cur_loc, FNV64_SEED) % BUCKET_COUNT;  
          } else {
            dst_next_loc = dst_end;  
          }
        }

        //reached end 
        if (dst_next_loc == dst_end && dst_bucket_val < src_bucket_val) {
          dst_cur_loc = dst_next_loc;//reevaluate?
        }

        //append dst
        memcpy(buffer_loc, dst_chunk_start, dst_cur_loc - dst_chunk_start);
        buffer_loc += (dst_cur_loc - dst_chunk_start);
        //advance start of chunk
        dst_chunk_start = dst_cur_loc;

      } else if (src_bucket_val < dst_bucket_val){
        //loop through src until > dst_bucket or end of src
        
        while (src_bucket_val < dst_bucket_val && src_next_loc < src_end) {
          //keep searching
          src_cur_loc = src_next_loc;
          
          if (src_cur_loc < src_end) {
            src_next_loc = (uint8_t*)memchr(src_cur_loc, *STRING_SEPARATOR.ptr, src_end - src_cur_loc) + STRING_SEPARATOR.len;
            src_bucket_val = FnvHash(src_cur_loc, (src_next_loc - STRING_SEPARATOR.len) - src_cur_loc, FNV64_SEED) % BUCKET_COUNT;
          } else {
            src_next_loc = src_end;
          }
        }

        //end reached and less than, write it all
        if (src_next_loc == src_end && src_bucket_val < dst_bucket_val) {
          src_cur_loc = src_next_loc;//revalute:move out?
        }

        //append dst
        memcpy(buffer_loc, src_chunk_start, src_cur_loc - src_chunk_start);
        buffer_loc += src_cur_loc - src_chunk_start;
        src_chunk_start = src_cur_loc;
          
      } else {
        ////Same Bucket Values
        //work on this bucket value only

        //find end of src bucket
        uint8_t* src_bucket_start = src_cur_loc;
        uint64_t src_next_bucket_val = src_bucket_val;
        while (src_next_bucket_val == src_bucket_val && src_next_loc < src_end) {
          src_cur_loc = src_next_loc;
          if (src_cur_loc < src_end) {
            src_next_loc = (uint8_t*)memchr(src_cur_loc, *STRING_SEPARATOR.ptr, src_end - src_cur_loc) + STRING_SEPARATOR.len;
            src_next_bucket_val = FnvHash(src_cur_loc, (src_next_loc - STRING_SEPARATOR.len) - src_cur_loc, FNV64_SEED) % BUCKET_COUNT;
          } 
        }

        uint8_t* src_bucket_end = src_next_loc;
        // src variables setup for next outter loop at this point

        // loop through current dst bucket
        do {

          //loop through src, test for duplicate
          bool match_found = false;
          uint8_t* src_inner_cur_loc = src_bucket_start;
          uint8_t* src_inner_next_loc;
          do {
            src_inner_next_loc = (uint8_t*)memchr(src_inner_cur_loc, *STRING_SEPARATOR.ptr, src_bucket_end - src_inner_cur_loc) + STRING_SEPARATOR.len;

            if ( (src_inner_next_loc - src_inner_cur_loc) == (dst_next_loc - dst_cur_loc) ) {
              if (!memcmp(dst_cur_loc, src_inner_cur_loc, src_inner_next_loc - src_inner_cur_loc)) {
                match_found = true;
              }
            }
            src_inner_cur_loc = src_inner_next_loc;
          } while (src_inner_cur_loc < src_bucket_end && !match_found);
        
          if (!match_found) {
            //append to end
            memcpy(buffer_loc, dst_cur_loc, dst_next_loc - dst_cur_loc);
            buffer_loc += (dst_next_loc - dst_cur_loc);            
          }

          //advance position
          dst_cur_loc = dst_next_loc;
          if (dst_cur_loc < dst_end) {
            dst_next_loc = (uint8_t*)memchr(dst_cur_loc, *STRING_SEPARATOR.ptr, dst_end - dst_cur_loc) + STRING_SEPARATOR.len;
            dst_bucket_val = FnvHash(dst_cur_loc, (dst_next_loc - STRING_SEPARATOR.len) - dst_cur_loc, FNV64_SEED) % BUCKET_COUNT;
          }

        } while (dst_bucket_val == src_bucket_val && dst_next_loc < dst_end);

        //no more dst values, append src bucket
        memcpy(buffer_loc, src_chunk_start, src_cur_loc - src_chunk_start);
        buffer_loc += src_cur_loc - src_chunk_start;
        //advance src
        src_chunk_start = src_cur_loc;
        src_bucket_val = src_next_bucket_val;

        //advance to chunk to first item in next dst bucket
        dst_chunk_start = dst_cur_loc;

      }

    } while (dst_chunk_start < dst_end && src_chunk_start < src_end);
    //!todo:test empty string at end of list

    //check if one list still contains entries, if so append and advance
    if (dst_chunk_start < dst_end) {
      memcpy(buffer_loc, dst_chunk_start, dst_end - dst_chunk_start);
      buffer_loc += (dst_end - dst_chunk_start);
    } else if (src_chunk_start < src_end) {
      memcpy(buffer_loc, src_chunk_start, src_end - src_chunk_start);
      buffer_loc += src_end - src_chunk_start;
    }

    context->Free(dst->ptr);
    dst->ptr = context->Reallocate(merge_buffer, buffer_loc - merge_buffer);
    if (!dst->ptr) {
      //allocation failed
      context->SetError("HashSetCount: Merge memory reallocation failed.");
      return;
    }
    dst->len = buffer_loc - merge_buffer; 

  } else {
    context->SetError("HashSetCount: Undefined intermediate type (merge).");
  }
}


StringVal DistHashSetFinalize(FunctionContext* context, const StringVal& strvaldhs) {
  //assert(!strvaldhs.ptr);
  if(!strvaldhs.ptr) {
    context->SetError("DistHashFinalize: intermediate contains null pointer.");
    return StringVal::null();
  }

  int unique_count = 0;
  StringVal result;
  
  if (strvaldhs.ptr[0] == MAGIC_BYTE_DELIMSTR) {
    //intermediate type is delimited string

    //count number of seperators
    unique_count = count(strvaldhs.ptr, strvaldhs.ptr + strvaldhs.len, (int) *STRING_SEPARATOR.ptr);
    result = ToStringVal(context, unique_count);

    //context->Free(strvaldhs.ptr); 
  } else if (strvaldhs.ptr[0] == MAGIC_BYTE_DHS) {
    //intermediate type is DHS

    assert(strvaldhs.len == sizeof(DistHashSet));
    DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs.ptr);
    
    ////Free memory
    if(!dhs->buckets_need_alloc) {
      //check mem allocation
      for (int i = 0; i < dhs->bucket_count; i++) {
        if (dhs->buckets[i]) {
          if (dhs->buckets[i]->ptr) {
            //increment for every separator
            unique_count += count(dhs->buckets[i]->ptr, dhs->buckets[i]->ptr + dhs->buckets[i]->len, (int) *STRING_SEPARATOR.ptr);
            //free bucket ptrs
            context->Free((uint8_t*) dhs->buckets[i]->ptr);  
          }  
          //free buckets contents
          context->Free((uint8_t*) dhs->buckets[i]);
          //dhs->buckets[i] = NULL;
        }  
      }
      //free buckets array
      context->Free((uint8_t*) dhs->buckets);
      result = ToStringVal(context, unique_count);
    } else {
      //this handles an empty dhs, e.g. all nulls or 0 rows
      result = StringVal::null();
    }
    //context->Free(strvaldhs.ptr); 
    /////Bucket Memory Freed

    
  } else {
    context->SetError("DistHashFinalize: Bad final type found.");
    result = StringVal::null();
  }

  //both paths lead to freeing ptr
  context->Free(strvaldhs.ptr); 

  return result;
}

