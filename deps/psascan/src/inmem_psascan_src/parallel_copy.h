/*============================================
 # Source code: https://www.cs.helsinki.fi/group/pads/pSAscan.html
 #
 # Paper: Parallel external memory suffix sorting
 # Authors: Juha Kärkkäinen, Dominik Kempa, Simon J. Puglisi.
 # In Proc. CPM 2015, pp. 329–342.
============================================*/
#ifndef __PSASCAN_SRC_INMEM_PSASCAN_SRC_PARALLEL_COPY_H_INCLUDED
#define __PSASCAN_SRC_INMEM_PSASCAN_SRC_PARALLEL_COPY_H_INCLUDED

#include <algorithm>
#include <thread>

#include "../uint40.h"
#include "bwtsa.h"


namespace psascan_private {
namespace inmem_psascan_private {

template<typename T, typename S>
void parallel_copy_aux(const T *src, S *dest, long length) {
  for (long i = 0; i < length; ++i)
    dest[i] = (S)src[i];
}

// Specilization
template<>
void parallel_copy_aux(const bwtsa_t<uint40> *src, unsigned char *dest, long length) {
  for (long i = 0; i < length; ++i)
    dest[i] = src[i].bwt;
}

// Specilization
template<>
void parallel_copy_aux(const bwtsa_t<int> *src, unsigned char *dest, long length) {
  for (long i = 0; i < length; ++i)
    dest[i] = src[i].bwt;
}


// Conversion from T to S has to make sense.
template<typename T, typename S>
void parallel_copy(const T *src, S *dest, long length, long max_threads) {
  long max_block_size = (length + max_threads - 1) / max_threads;
  long n_blocks = (length + max_block_size - 1) / max_block_size;

  std::thread **threads = new std::thread*[n_blocks];
  for (long i = 0; i < n_blocks; ++i) {
    long block_beg = i * max_block_size;
    long block_end = std::min(block_beg + max_block_size, length);
    long block_size = block_end - block_beg;

    threads[i] = new std::thread(parallel_copy_aux<T, S>,
        src + block_beg, dest + block_beg, block_size);
  }

  for (long i = 0; i < n_blocks; ++i) threads[i]->join();
  for (long i = 0; i < n_blocks; ++i) delete threads[i];
  delete[] threads;
}

// Specialization
template<>
void parallel_copy(const bwtsa_t<uint40> *src, unsigned char *dest, long length, long max_threads) {
  long max_block_size = (length + max_threads - 1) / max_threads;
  long n_blocks = (length + max_block_size - 1) / max_block_size;

  std::thread **threads = new std::thread*[n_blocks];
  for (long i = 0; i < n_blocks; ++i) {
    long block_beg = i * max_block_size;
    long block_end = std::min(block_beg + max_block_size, length);
    long block_size = block_end - block_beg;

    threads[i] = new std::thread(parallel_copy_aux<bwtsa_t<uint40>, unsigned char>,
        src + block_beg, dest + block_beg, block_size);
  }

  for (long i = 0; i < n_blocks; ++i) threads[i]->join();
  for (long i = 0; i < n_blocks; ++i) delete threads[i];
  delete[] threads;
}

// Specialization
template<>
void parallel_copy(const bwtsa_t<int> *src, unsigned char *dest, long length, long max_threads) {
  long max_block_size = (length + max_threads - 1) / max_threads;
  long n_blocks = (length + max_block_size - 1) / max_block_size;

  std::thread **threads = new std::thread*[n_blocks];
  for (long i = 0; i < n_blocks; ++i) {
    long block_beg = i * max_block_size;
    long block_end = std::min(block_beg + max_block_size, length);
    long block_size = block_end - block_beg;

    threads[i] = new std::thread(parallel_copy_aux<bwtsa_t<int>, unsigned char>,
        src + block_beg, dest + block_beg, block_size);
  }

  for (long i = 0; i < n_blocks; ++i) threads[i]->join();
  for (long i = 0; i < n_blocks; ++i) delete threads[i];
  delete[] threads;
}

}  // namespace inmem_psascan_private
}  // namespace psascan_private

#endif  // __PSASCAN_SRC_INMEM_PSASCAN_SRC_PARALLEL_SHRINK_H_INCLUDED
