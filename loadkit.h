/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
#ifndef _LOADKIT
#define _LOADKIT
#include<stdlib.h>
#include<stdio.h>
#include"BaseClass.h"
#include<iostream>
using namespace std;
class loadkit
{
	public:
		loadkit(const char * file);
		~loadkit();
		integer loadi64(i64 & value);
		integer loadu64(u64 & value);
		integer loadinteger(integer & value);
		integer loadu32(u32 & value);
		integer loadi32(i32 & value);
		integer loadi16(i16 & value);
		integer loadu16(u16 & value);
		integer loadi64array(i64 * value,integer len);
		i64 loadi64array(i64 * value,i64 len);
		integer loadu64array(u64 * value,i64 len);
		integer loadintegerarray(integer * value,integer len);
		integer loadu32array(u32 * value,integer len);
		i64 loadu32array(u32 * value,i64 len);
		integer loadi16array(i16 * value,integer len);
		integer loadu16array(u16 * value,integer len);
		void close();
	private:
		FILE * r;
};
#endif


