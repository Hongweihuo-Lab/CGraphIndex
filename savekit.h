/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
#ifndef _SAVEKIT
#define _SAVEKIT
#include<stdio.h>
#include<stdlib.h>
#include"BaseClass.h"
#include<fstream>
#include<iostream>
using namespace std;
class savekit
{
	public:
		savekit(const char * file);
		~savekit();

		integer writei64(i64 value);
		integer writeu64(u64 value);
		integer writeinteger(integer value);
	    integer writeu32(u32 value);
		integer writei32(i32 value);
		integer writei16(i16 value);
		integer writeu16(u16 value);

		integer writei64array(i64 * value,integer len);
		integer writei64array(i64 * value,i64 len);
		integer writeu64array(u64 * value,i64 len);
		integer writeintegerarray(integer * value,integer len);
		integer writeu32array(u32 * value,integer len);
		integer writeu32array(u32 * value,i64 len);
		integer writei16array(i16 * value,integer len);
		integer writeu16array(u16 * value,integer len);
	    void close();
	private:
		FILE *w;
};
#endif
