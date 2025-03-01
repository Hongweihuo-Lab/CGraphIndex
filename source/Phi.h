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

#include"BaseClass.h"
#include"parmaters.h"
#include"savekit.h"
#include"loadkit.h"
#include"ABS_Phi.h"
#include"Gen_Phi.h"
#include<map>
class Phi
{
	public:
		Phi(parmaters *csa);
		Phi();
		~Phi();

		//integer *GetPhiArray();
		//void GetPhiPiece(integer index,integer num,integer *phipiece);
		i64 GetValue(i64 i);
		i64 RightBoundary(i64 r, i64 L, i64 R);
		i64 LeftBoundary(i64 l, i64 L, i64 R);
		i64 Size();

		integer write(savekit & s);
		integer load(loadkit &s);
		i64**doublec;
		i64*range;
		i64 mini;
		int range_len;

		i64 getEcodeSpace(int type);

	private:
		i64* PreSpace(parmaters * csa);
		ABS_Phi *phi;
		uchar*T;
		i64*code;
		i64*start;
		i64 n;
		void randchar(uchar*T);
		i64 left(i64 i, i64 j);
		i64 right(i64 i, i64 j);
		i64*phivalue;
		integer type;
		i64 blogsize(i64 x);
		i64 puredelta(i64 x);
		uchar lastchar;
};
