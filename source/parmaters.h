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

#ifndef _PARMATERS
#define _PARMATERS
#include"BaseClass.h"
#include"divsufsort64.h"
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
