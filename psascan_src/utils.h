/*============================================
 # Source code: https://www.cs.helsinki.fi/group/pads/pSAscan.html
 #
 # Paper: Parallel external memory suffix sorting
 # Authors: Juha Kärkkäinen, Dominik Kempa, Simon J. Puglisi.
 # In Proc. CPM 2015, pp. 329–342.
============================================*/
#ifndef __PSASCAN_SRC_PSASCAN_UTILS_H_INCLUDED
#define __PSASCAN_SRC_PSASCAN_UTILS_H_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <string>
#include <sstream>


namespace psascan_private {
namespace utils {

#define STRX(x) #x
#define STR(x) STRX(x)
template<typename value_type>
void read_n_objects_from_file(value_type* tab, long length, std::FILE *f);
// Time
long double wclock() {
  timeval tim;
  gettimeofday(&tim, NULL);

  return tim.tv_sec + (tim.tv_usec / 1000000.0L);
}

// Basic file handling
std::FILE *open_file(std::string fname, std::string mode) {
  std::FILE *f = std::fopen(fname.c_str(), mode.c_str());
  if (f == NULL) {
    std::perror(fname.c_str());
    std::exit(EXIT_FAILURE);
  }

  return f;
}
long file_size(std::string fname) {
  std::FILE *f = open_file(fname, "rt");
  std::fseek(f, 0L, SEEK_END);
  long size = std::ftell(f);
  std::fclose(f);

  return size;
}
bool file_exists(std::string fname) {
  std::FILE *f = std::fopen(fname.c_str(), "r");
  bool ret = (f != NULL);
  if (f != NULL)
    std::fclose(f);

  return ret;
}

void file_delete(std::string fname) {
  int res = std::remove(fname.c_str());
  if (res) {
    fprintf(stderr, "Failed to delete %s: %s\n",
        fname.c_str(), strerror(errno));
    std::exit(EXIT_FAILURE);
  }
}
std::string absolute_path(std::string fname) {
  char path[1 << 12];
  bool created = false;

  if (!file_exists(fname)) {
    // We need to create the file, since realpath fails on non-existing files.
    std::fclose(open_file(fname, "w"));
    created = true;
  }
  if (!realpath(fname.c_str(), path)) {
    fprintf(stderr, "\nError: realpath failed for %s\n", fname.c_str());
    std::exit(EXIT_FAILURE);
  }

  if (created)
    file_delete(fname);

  return std::string(path);
}


// File I/O
void read_block(std::FILE *f, long beg, long length, unsigned char *b) {
  std::fseek(f, beg, SEEK_SET);
  read_n_objects_from_file<unsigned char>(b, length, f);
}

void read_block(std::string fname, long beg, long length, unsigned char *b) {
  std::FILE *f = open_file(fname.c_str(), "r");
  read_block(f, beg, length, b);
  std::fclose(f);
}

template<typename value_type>
void write_objects_to_file(const value_type *tab, long length, std::string fname) {
  std::FILE *f = open_file(fname, "w");
  size_t fwrite_ret = std::fwrite(tab, sizeof(value_type), length, f);
  if ((long)fwrite_ret != length) {
    fprintf(stderr, "\nError: fwrite in line %s of %s returned %ld\n",
        STR(__LINE__), STR(__FILE__), fwrite_ret);
    std::exit(EXIT_FAILURE);
  }

  std::fclose(f);
}

template<typename value_type>
void add_objects_to_file(const value_type *tab, long length, std::FILE *f) {
  size_t fwrite_ret = std::fwrite(tab, sizeof(value_type), length, f);
  if ((long)fwrite_ret != length) {
    fprintf(stderr, "\nError: fwrite in line %s of %s returned %lu\n",
        STR(__LINE__), STR(__FILE__), fwrite_ret);
    std::exit(EXIT_FAILURE);
  }
}

template<typename value_type>
void add_objects_to_file(const value_type *tab, long length, std::string fname) {
  std::FILE *f = utils::open_file(fname.c_str(), "a");
  add_objects_to_file<value_type>(tab, length, f);
  std::fclose(f);
}

template<typename value_type>
void read_n_objects_from_file(value_type* tab, long length, std::FILE *f) {
  size_t fread_ret = std::fread(tab, sizeof(value_type), length, f);
  if ((long)fread_ret != length) {
    fprintf(stderr, "\nError: fread in line %s of %s returned %ld\n",
        STR(__LINE__), STR(__FILE__), fread_ret);
    std::exit(EXIT_FAILURE);
  }
}

template<typename value_type>
void read_n_objects_from_file(value_type* tab, long length, std::string fname) {
  std::FILE *f = open_file(fname, "r");
  read_n_objects_from_file<value_type>(tab, length, f);
  std::fclose(f);
}

template<typename value_type>
void read_objects_from_file(value_type* &tab, long &length, std::string fname) {
  std::FILE *f = open_file(fname, "r");
  std::fseek(f, 0L, SEEK_END);
  length = (long)(std::ftell(f) / sizeof(value_type));
  std::rewind(f);
  tab = (value_type *)malloc(length * sizeof(value_type));
  read_n_objects_from_file<value_type>(tab, length, f);
  std::fclose(f);
}

// Randomness
int random_int(int p, int r) {
  return p + rand() % (r - p + 1);
}
long random_long(long p, long r) {
  long x = random_int(0, 1000000000);
  long y = random_int(0, 1000000000);
  long z = x * 1000000000L + y;
  return p + z % (r - p + 1);
}
void fill_random_string(unsigned char* &s, long length, int sigma) {
  for (long i = 0; i < length; ++i)
    s[i] = random_int(0, sigma - 1);
}

void fill_random_letters(unsigned char* &s, long n, int sigma) {
  fill_random_string(s, n, sigma);
  for (long i = 0; i < n; ++i) s[i] += 'a';
}

std::string random_string_hash() {
  uint64_t hash = (uint64_t)rand() * RAND_MAX + rand();
  std::stringstream ss;
  ss << hash;
  return ss.str();
}


// Math
long log2ceil(long x) {
  long pow2 = 1, w = 0;
  while (pow2 < x) { pow2 <<= 1; ++w; }
  return w;
}
long log2floor(long x) {
  long pow2 = 1, w = 0;
  while ((pow2 << 1) <= x) { pow2 <<= 1; ++w; }
  return w;
}

// Misc
template<typename int_type>
std::string intToStr(int_type x) {
  std::stringstream ss;
  ss << x;
  return ss.str();
}

}  // namespace utils
}  // namespace psascan_private

#endif  // __PSASCAN_SRC_PSASCAN_UTILS_H_INCLUDED
