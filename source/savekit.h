// Copyright (C) 2024  Zongtao He, Yongze Yu, Hongwei Huo, and Jeffrey S. Vitter
// 
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
// USA

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
