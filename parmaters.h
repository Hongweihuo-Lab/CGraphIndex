/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
#ifndef _PARMATERS
#define _PARMATERS
#include"BaseClass.h"
#include"./divsufsort/divsufsort64.h"
#include<pthread.h>
typedef struct
{
	integer alphabetsize;
	i64 n;
	integer SL;
	integer L;

	i64 * start;
	uchar lastchar;
	saidx64_t *& SA;
	uchar *& T;
	i64 * code;
}parmaters;
#endif
