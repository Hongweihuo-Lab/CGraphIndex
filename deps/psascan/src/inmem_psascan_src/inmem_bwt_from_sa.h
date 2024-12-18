/*============================================
 # Source code: https://www.cs.helsinki.fi/group/pads/pSAscan.html
 #
 # Paper: Parallel external memory suffix sorting
 # Authors: Juha Kärkkäinen, Dominik Kempa, Simon J. Puglisi.
 # In Proc. CPM 2015, pp. 329–342.
============================================*/
#ifndef __PSASCAN_SRC_INMEM_PSASCAN_SRC_INMEM_BWT_FROM_SA_H_INCLUDED
#define __PSASCAN_SRC_INMEM_PSASCAN_SRC_INMEM_BWT_FROM_SA_H_INCLUDED

#include <algorithm>
#include <thread>

#include "../utils.h"
#include "bwtsa.h"


namespace psascan_private {
namespace inmem_psascan_private {

template<typename saidx_t>
void compute_bwt_in_bwtsa_aux(const unsigned char *text, long beg,
    long end, bwtsa_t<saidx_t> *dest, long *i0) {
  *i0 = -1;
  for (long j = beg; j < end; ++j) {
    if (dest[j].sa) dest[j].bwt = text[dest[j].sa - 1];
    else { dest[j].bwt = 0; *i0 = j; }
  }
}

template<typename saidx_t>
void compute_bwt_in_bwtsa(const unsigned char *text, long length,
  bwtsa_t<saidx_t> *dest, long max_threads, long &result) {
  long max_block_size = (length + max_threads - 1) / max_threads;
  long n_blocks = (length + max_block_size - 1) / max_block_size;
  long *index_0 = new long[n_blocks];

  // Compute bwt and find i0, where sa[i0] == 0.
  std::thread **threads = new std::thread*[n_blocks];
  for (long i = 0; i < n_blocks; ++i) {
    long block_beg = i * max_block_size;
    long block_end = std::min(block_beg + max_block_size, length);

    threads[i] = new std::thread(compute_bwt_in_bwtsa_aux<saidx_t>,
        text, block_beg, block_end, dest, index_0 + i);
  }

  for (long i = 0; i < n_blocks; ++i) threads[i]->join();
  for (long i = 0; i < n_blocks; ++i) delete threads[i];
  delete[] threads;

  // Find and return i0.
  result = -1;
  for (long i = 0; i < n_blocks; ++i)
    if (index_0[i] != -1) result = index_0[i];
  delete[] index_0;
}

}  // namespace inmem_psascan_private
}  // namespace psascan_private

#endif  // __PSASCAN_SRC_INMEM_PSASCAN_SRC_INMEM_BWT_FROM_SA_INCLUDED
