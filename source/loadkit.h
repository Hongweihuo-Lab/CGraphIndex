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


