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
