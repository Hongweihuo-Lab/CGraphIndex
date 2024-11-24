#ifndef _GBV_H_
#define _GBV_H_
#include <cstring>
#include "ABS_Phi.h"
#include "InArray.h"
#include "parmaters.h"
#include "BaseClass.h"
#include "select.h"
#include <assert.h>
#include "./wheel/mutexlock.hpp"

class GEN_Phi :public ABS_Phi
{
public:
	friend void* work_phi(void *arg); // Phi多线程压缩时的每个工作函数
	friend void Append_Gamma_out(i64 x,i64& index,u32 *sequence);
	friend void Append_out(i64 x,i64& index,u32 *sequenc);
	GEN_Phi(parmaters * csa, i64*phivalue,double multi = 1.0);
	GEN_Phi();
	~GEN_Phi();
	i64 GetValue(i64 i);
	i64 RightBoundary(i64 r, i64 L, i64 R);
	i64 LeftBoundary(i64 l, i64 L, i64 R);
	i64 GetMemorySize();
	integer write(savekit & s);
	integer load(loadkit & s);
	void Testforgetphivalue();

	i64 bvsum;
	i64 gamasum;
	i64 rldatasum;
	i64 sizeofEcode(int type); 
	i64 deltasize(i64 v);
	i64 blogsize(i64 x);
	enum CodeType{gamma,bv,allone,rldelta};
private:
	i64 mod(i64 x);
	i64 GetOneBit(i64 i);
	integer a;
	integer b;
	integer alpsize;
	integer tablewidth;
	u16 *zerostable_gamma;
	u16 *decodevaluenum_gamma;
	u16 *decodebitsnum_gamma;
	u16 *decoderesult_gamma;

	u16 *zerostable;
	u16 *decodevaluenum;
	u16 *decodebitsnum;
	u16 *decoderesult;

	i64  * superoffset;
	InArray* samples;
	InArray * offsects;
	InArray*method;
	u32 * sequence;
	i64 index;
	i64 lenofsequence;
	i64 lenofsuperoffset;
	i64 n;
	i64 * phivalue;

	

	double multi;

	
	void SamplingAndCoding(parmaters * csa);
	void SamplingAndCoding(parmaters * csa, int num);
	void Test();
	void Append_Gamma(i64 x);
	i64 Decodegamma(i64& position, i64 & x);
	i64 Decodegamma(i64 &postion, i64 &x, i64 zero);
	void InitionalTables_Gamma();
	i64 ZerosRun_Gamma(i64 &position);
	u64 GetBits_Gamma(i64 position, i64 x);
	i64 GetValue(i64 i, i64 & position);


	void Append(i64 x);
	i64 DecodeDelta(i64& position, i64 & x);
	i64 DecodeDelta2(i64& position, i64 & x);
	i64 DecodeDelta3(i64&position, i64&x, i64 zero);
	i64 DecodeDeltaBlock(i64 position, i64 base, i64 num);
	u64 GetBits(i64 position, i64 x);
	void InitionalTables();
	i64 ZerosRun(i64 &position);
};


#endif