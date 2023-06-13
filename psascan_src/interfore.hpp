#pragma once
#include "psascan.h"//
#include "../divsufsort/divsufsort.h"
#include"../divsufsort/divsufsort64.h"
#include <omp.h>//
void pSACreate(std::string input_filename,long *SA, long size)
{
    std::string output_filename(input_filename + ".sa5");
    long max_threads = (long)omp_get_max_threads();
    long use_size =  size * 6 > (8ull << 30) ? size * 6 : (8ull << 30);
    pSAscan1(input_filename, output_filename,
    output_filename, use_size , max_threads, false,SA);
}

