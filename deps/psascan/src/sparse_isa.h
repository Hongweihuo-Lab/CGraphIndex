/*============================================
 # Source code: https://www.cs.helsinki.fi/group/pads/pSAscan.html
 #
 # Paper: Parallel external memory suffix sorting
 # Authors: Juha Kärkkäinen, Dominik Kempa, Simon J. Puglisi.
 # In Proc. CPM 2015, pp. 329–342.
============================================*/
#ifndef __PSASCAN_SRC_SPARSE_ISA_H_INCLUDED
#define __PSASCAN_SRC_SPARSE_ISA_H_INCLUDED

#include <algorithm>
#include <thread>


namespace psascan_private {

template<typename approx_rank_type, typename saidx_t, long k_sampling_rate_log>
struct sparse_isa {
  private:
    long m_length;
    long m_last_isa;
    long m_i0;

    long *m_count;
    long *m_sparse_isa;

    const saidx_t *m_psa;
    const unsigned char *m_text;
    const approx_rank_type *m_rank;
  
    static const long k_sampling_rate;
    static const long k_sampling_rate_mask;
    static const long k_sigma = 256;

  private:
    template<typename T>
    static void compute_sparse_isa_aux(const T *psa, long block_beg,
        long block_end, long psa_size, long *sparse_isa, long &last) {
      for (long j = block_beg; j < block_end; ++j) {
        long sa_j = (long)psa[j];
        if (!(sa_j & k_sampling_rate_mask))
          sparse_isa[sa_j >> k_sampling_rate_log] = j;
        if (sa_j == psa_size - 1) last = j;
      }
    }

  public:
    sparse_isa(const saidx_t *psa, const unsigned char *text, long length,
        long i0, const approx_rank_type *rank, long max_threads) {
      m_psa = psa;
      m_length = length;
      m_rank = rank;
      m_text = text;
      m_i0 = i0;

      long elems = (m_length + k_sampling_rate - 1) / k_sampling_rate + 1;
      m_sparse_isa = (long *)malloc(elems * sizeof(long));

      long max_block_size = (m_length + max_threads - 1) / max_threads;
      long n_blocks = (m_length + max_block_size - 1) / max_block_size;

      std::thread **threads = new std::thread*[n_blocks];
      for (long t = 0; t < n_blocks; ++t) {
        long block_beg = t * max_block_size;
        long block_end = std::min(block_beg + max_block_size, m_length);

        threads[t] = new std::thread(compute_sparse_isa_aux<saidx_t>, m_psa,
            block_beg, block_end, m_length, m_sparse_isa, std::ref(m_last_isa));
      }

      for (long t = 0; t < n_blocks; ++t) threads[t]->join();
      for (long t = 0; t < n_blocks; ++t) delete threads[t];
      delete[] threads;

      m_count = (long *)malloc(k_sigma * sizeof(long));
      std::copy(rank->m_count, rank->m_count + k_sigma, m_count);
      ++m_count[text[length - 1]];
      --m_count[0];

      for (long i = 0, s = 0; i < k_sigma; ++i) {
        long t = m_count[i];
        m_count[i] = s;
        s += t;
      }
    }

    inline long query(long j) const {
      long isa_i;
      long i = ((j + k_sampling_rate - 1) >> k_sampling_rate_log);
      if ((i << k_sampling_rate_log) < m_length) {
        isa_i = (long)m_sparse_isa[i];
        i <<= k_sampling_rate_log;
      } else {
        isa_i = m_last_isa;
        i = m_length - 1;
      }

      while (i != j) {
        // Compute ISA[i - 1] from ISA[i].
        // Invariant:
        //   isa_i = ISA[i]
        //   j <= i
        unsigned char c = m_text[i - 1];
        int delta = (isa_i > m_i0 && c == 0);

        isa_i = m_count[c] + m_rank->rank(isa_i, c) - delta;
        while (isa_i < 0 || (long)m_psa[isa_i] != i - 1)
          ++isa_i;

        --i;
      }

      return isa_i;
    }

    ~sparse_isa() {
      free(m_sparse_isa);
      free(m_count);
    }
};

template<typename approx_rank_type, typename saidx_t, long k_sampling_rate_log>
const long sparse_isa<approx_rank_type, saidx_t, k_sampling_rate_log>
  ::k_sampling_rate = (1L << k_sampling_rate_log);

template<typename approx_rank_type, typename saidx_t, long k_sampling_rate_log>
const long sparse_isa<approx_rank_type, saidx_t, k_sampling_rate_log>
  ::k_sampling_rate_mask = (1L << k_sampling_rate_log) - 1;

}  // namespace psascan_private

#endif  // __PSASCAN_SRC_SPARSE_ISA_H_INCLUDED
