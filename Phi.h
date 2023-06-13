/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
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
