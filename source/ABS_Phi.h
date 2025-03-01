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

#ifndef _ABS_PHI
#define _ABS_PHI
#include"BaseClass.h"
#include"savekit.h"
#include"loadkit.h"
class ABS_Phi
{
	public:
		virtual ~ABS_Phi(){};
		//virtual integer * GetPhiArray()=0;
		//virtual void GetSubPhiArray(integer,integer,integer *)=0;
		virtual i64 GetValue(i64 i) = 0;
		virtual i64 RightBoundary(i64 r, i64 L, i64 R) = 0;
		virtual i64 LeftBoundary(i64 l, i64 L, i64 R) = 0;
		virtual i64 GetMemorySize() = 0;
		virtual integer write(savekit & s)=0;
		virtual integer load(loadkit &s)=0;
		virtual i64 sizeofEcode(int type)=0;
};
#endif
