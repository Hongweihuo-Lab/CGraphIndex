/*============================================
 # Source code: https://www.cs.helsinki.fi/group/pads/pSAscan.html
 #
 # Paper: Parallel external memory suffix sorting
 # Authors: Juha Kärkkäinen, Dominik Kempa, Simon J. Puglisi.
 # In Proc. CPM 2015, pp. 329–342.
============================================*/
#ifndef __PSASCAN_SRC_BACKGROUND_CHUNK_READER_H_INCLUDED
#define __PSASCAN_SRC_BACKGROUND_CHUNK_READER_H_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <string>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "utils.h"


namespace psascan_private {

struct background_chunk_reader {
  private:
    std::FILE *m_file;
    long m_chunk_length;
    long m_end;
    
    std::condition_variable m_cv;
    std::mutex m_mutex;
    std::thread *m_thread;
    
    bool m_signal_read_next_chunk;
    bool m_signal_stop;

    long m_cur;
    unsigned char *m_passive_chunk;

  public:
    unsigned char *m_chunk;
    
  private:
    static void async_io_code(background_chunk_reader &r) {
      while (true) {
        std::unique_lock<std::mutex> lk(r.m_mutex);
        while (!r.m_signal_read_next_chunk && !r.m_signal_stop)
          r.m_cv.wait(lk);
          
        bool sig_stop = r.m_signal_stop;
        r.m_signal_read_next_chunk = false;
        lk.unlock();
        
        if (sig_stop) break;
        
        long next_chunk_length = std::min(r.m_chunk_length, r.m_end - r.m_cur);
        utils::read_n_objects_from_file(r.m_passive_chunk, next_chunk_length, r.m_file);
        
        lk.lock();
        r.m_cur += next_chunk_length;
        lk.unlock();
        r.m_cv.notify_all();
      }
    }

  public:
    background_chunk_reader(std::string filename, long beg,
        long end, long chunk_length = (1L << 20)) {
      if (beg > end) {
        fprintf(stderr, "Error: beg > end in background_chunk_reader.\n");
        std::exit(EXIT_FAILURE);
      }
      
      if (beg == end) return;

      m_cur = beg;
      m_end = end;

      m_chunk_length = chunk_length;
      m_chunk = (unsigned char *)malloc(m_chunk_length);
      m_passive_chunk = (unsigned char *)malloc(m_chunk_length);
      
      m_file = utils::open_file(filename, "r");
      std::fseek(m_file, m_cur, SEEK_SET);

      m_signal_stop = false;
      m_signal_read_next_chunk = true;
      m_thread = new std::thread(async_io_code, std::ref(*this));
    }

    inline void wait(long end) {
      if (end > m_end) {
        fprintf(stderr, "Error: end > m_end in background_chunk_reader.\n");
        std::exit(EXIT_FAILURE);
      }
      
      std::unique_lock<std::mutex> lk(m_mutex);
      while (m_cur != end)
        m_cv.wait(lk);
        
      if (m_signal_read_next_chunk) {
        fprintf(stderr, "Error: m_signal_read_next_chunk in the wrong state.\n");
        std::exit(EXIT_FAILURE);
      }

      std::swap(m_chunk, m_passive_chunk);
      m_signal_read_next_chunk = true;

      lk.unlock();
      m_cv.notify_all();
    }
    
    ~background_chunk_reader() {
      std::unique_lock<std::mutex> lk(m_mutex);
      m_signal_stop = true;
      lk.unlock();
      m_cv.notify_all();

      // Wait until the thread notices the flag and exits. Possibly the thread
      // is already not running, but in this case this call will do nothing.
      m_thread->join();

      std::fclose(m_file);

      // Clean up.  
      delete m_thread;
      free(m_chunk);
      free(m_passive_chunk);
    }

    inline long get_chunk_size() const {
      return m_chunk_length;
    }
};

}  // namespace psascan_private

#endif  // __PSASCAN_SRC_BACKGROUND_CHUNK_READER_H_INCLUDED
