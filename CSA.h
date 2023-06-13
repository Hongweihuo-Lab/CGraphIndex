/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
#pragma once
#include"savekit.h"
#include"loadkit.h"
#include"InArray.h"
#include<string.h>
#include"Phi.h"
#include"BaseClass.h"
#include"parmaters.h"
#include"./divsufsort/divsufsort64.h"
#include"./divsufsort/divsufsort.h"
#include<fstream>
#include<iostream>
#include<time.h>
#include<vector>
#include<algorithm>
#include<time.h>
#include <unistd.h>
using namespace std;


class CSA
{
public:

	CSA(){};
	CSA(const char * sourcefile,integer L=256,integer D=256);
	~CSA();

	//bool existential(const uchar * Pattern);
	void counting(const uchar * Pattern, i64 &num);
	i64 * locating(const uchar * Pattern, i64 &num);
	uchar * extracting(i64 start, i64 len);
	/*
		GetRow函数返回id的信息起始位置和长度
	*/
	void GraphExt(string& res, i64 start, i64 len); // 从start开始提取长度为len的子串
	void GetInfo(string& res, u64& row); // 向外提供的接口函数，主要根据行号在VB中找到属性对应的起始位置，然后调用GraphExt
	i64 *GetLocV(const uchar* value, u64 l, u64 r, u64& num); // 目前不需要locate，可以忽略
	InArray* GetVB(); 

	integer save(const char * indexfile);
	integer load(const char * indexfile);

	integer getAlphabetsize();
	i64 getN();
	i64 sizeInByte();
	i64 sizeInByteForLocate();
	i64 sizeInByteForCount();

	double compressRatio();
	double compressRatioForCount();
	double compressRatioForLocate();
	double compressRatioForExt();	

	i64 n;

	void TestForCount(int count = 5000);
	void TestForExtract(int count = 5000);
	void TestForLocate(int count = 5000);
	void TestForIndex(int index_time = 5000);


	uchar *T;
	integer lookup(integer i);

	double compressRatioforEcodeInPhi(int type);
	double compressRatioforEcodeInAll(int type);
	double compressRationforSA();
	double compressRationforRank();
	double compressRationforRSD();
	double compressRatioforallsample();

private:
	CSA(const CSA &);
	CSA& operator=(const CSA & right);

	void CreateSupportStructer(parmaters * csa); // CSA构建的主要函数，会并行构建SA，压缩Phi等操作
	void CreateSupportStructer_loc(parmaters * csa);
	void newEnumerative2(i64 l, i64 r, i64 *&pos);
	i64 newInverse(i64 i);
	i64 newPhi_list(i64 i,i64 &Lpre,i64 &Rpre);
	i64 Character(i64 i);


	void Search(const uchar * Pattern, i64 & L, i64 &R);
	void Search2(const uchar * Pattern, i64 &L, i64 &R);

	uchar * GetFile(const char * filename);
	void statics(uchar *T);

	u64 blog(i64 x);

	void SplitString(const string& s, vector<string>& v, const string& c);
	int FileExist(const char* fname);

	integer SL,L,D,RD,NewD;
	InArray  * SAL;
	InArray * RankL;
	Phi * Phi0;
	i64 * code;
	i64 * incode;
	integer alphabetsize;
	i64 * start;
	uchar lastchar;  
    integer Blength;
	u64 maxone;

	InArray *ArrD;
	InArray *VB;
};
