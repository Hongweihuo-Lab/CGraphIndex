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

#include"Phi.h"
#include<sys/time.h>
#include<unistd.h>
#define MAXNUM 99999999
extern pthread_t sapid,vbpid;
Phi::Phi()
{
	type=0;
	phi=NULL;
}
Phi::Phi(parmaters *csa)
{
timeval bb, ee;
gettimeofday(&bb,NULL);
	phivalue = this->PreSpace(csa);
gettimeofday(&ee,NULL);
cout<<"Phi costs : "<<((ee.tv_sec * 1000000 + ee.tv_usec) - (bb.tv_sec * 1000000 + bb.tv_usec)) * 1.0 / 1000000<<" sec"<<endl;;
	int64_t numof1 = 0;
	int64_t run_1_num = 0;
	int64_t len_runs_1 = 0;
	int64_t len_1 = 0;
	for (int64_t i = 1; i < n; i++)
	{
		if((phivalue[i]-phivalue[i-1]) == 1 ){
			numof1++;
			len_1++;
		}else{
			if(len_1 != 0){
				len_runs_1 += len_1;
				run_1_num++;
				len_1 = 0;
			}
		}
	}


	// cout<<"1-gap-num: "<<numof1<<endl;
	cout<<"1_ratio: "<<numof1*(1.0)/n*100<<'%'<<endl;
	// cout<<"len_all_runs: "<<len_runs_1<<endl;
	cout<<"average_runs: "<<len_runs_1*(1.0)/run_1_num<<endl;


	double multi = 1.0;
	if((numof1*(1.0)/n) < 0.9) multi = 1.8;

	this->T = csa->T;
	this->code = csa->code;
	this->start = csa->start;
	this->lastchar = csa->lastchar;
	randchar(T);
	pthread_join(sapid,NULL);
	pthread_join(vbpid,NULL);
	delete [] T;
	delete [] csa->SA;
	T = NULL;
	csa->T = NULL;
	csa->SA = NULL;
	phi = new GEN_Phi(csa, phivalue,multi); 

}

i64 Phi::GetValue(i64 i)
{
	return phi->GetValue(i);
}

i64 Phi::RightBoundary(i64 r, i64 L, i64 R)
{
	return phi->RightBoundary(r,L,R);
}

i64 Phi::LeftBoundary(i64 l, i64 L, i64 R)
{
	return phi->LeftBoundary(l,L,R);
}

i64 Phi::Size()
{
	return phi->GetMemorySize();
}

integer Phi::write(savekit &s)
{
	for (int i = 0; i < 256; i++)
	{
		s.writei64array(doublec[i],256);
	}
	
	s.writeinteger(range_len);
	s.writei64array(range,range_len);
	phi->write(s);
	return 1;
}

integer Phi::load(loadkit &s)
{
	this->doublec = new i64*[256];
	for (int i = 0; i < 256; i++)
		doublec[i] = new i64[256];
	for (int i = 0; i < 256; i++)
	{
		s.loadi64array(doublec[i],256);
	}

	s.loadinteger(range_len);
	this->range = new i64[range_len];
	s.loadi64array(range,range_len);
	phi = new GEN_Phi();
	phi->load(s);
	return 0;
}

Phi::~Phi()
{
	if(phi!=NULL)
		delete phi;
	phi=NULL;
}


i64 Phi::blogsize(i64 x){
	i64 len = 0;
	while (x>0){
		x = (x >> 1);
		len++;
	}
	return len;
}

i64 Phi::puredelta(i64 v){
	i64 x = blogsize(v);
	i64 pre = 2 * blogsize(x) - 1;
	return pre + x - 1;
}


i64* Phi::PreSpace(parmaters*csa){

	i64 i, j;
	this->n = csa->n;
	i64*phivalue = new i64[n];
	memset(phivalue,0,8 * n);
	i64 *temp = new i64[csa->alphabetsize + 1];
	for (i64 i = 0; i<csa->alphabetsize + 1; i++)
		temp[i] = csa->start[i];
	i64 index = temp[csa->code[csa->lastchar]];
	temp[csa->code[csa->lastchar]]++;
	i64 h = 0;
	unsigned char c = 0;
	i64 pos = 0;
	for (i64 i = 0; i<n; i++)
	{
		pos = csa->SA[i];
		if (pos == 0)
		{
			h = i;
			continue;
		}
		c = csa->T[pos - 1];
		phivalue[temp[csa->code[c]]++] = i;
	}
	phivalue[index] = h;

	delete[] temp; 
	return phivalue;
}


void Phi::randchar(uchar*T){
	i64 times = 20000;
	i64 pos = 0;
	i64 num = 0;
	this->mini = 200;
	this->doublec = new i64*[256];
	for (i64 i = 0; i < 256; i++)
		doublec[i] = new i64[256];
	for (i64 i = 0; i < 256; i++)
		for (i64 j = 0; j < 256; j++)
			doublec[i][j] = -1;
	for (i64 i = 0; i < times; i++){
		pos = rand() % (this->n - 21);
		doublec[T[pos] - '\0'][T[pos + 1] - '\0']++;
	}

	for (i64 i = 0; i < 256; i++){
		for (i64 j = 0; j < 256; j++){
			if (doublec[i][j]>(mini - 1))
				doublec[i][j] = num++;
			else
				doublec[i][j] = -1;
		}
	}
	this->range = new i64[num * 2];
	this->range_len = num*2;
	num = 0;
	for (i64 i = 0; i < 256; i++)
		for (i64 j = 0; j < 256; j++)
			if (doublec[i][j] >= 0){
				num = doublec[i][j];
				range[2 * num] = left(i, j);
				range[2 * num + 1] = right(i, j);
			}

}

i64 Phi::left(i64 i, i64 j){
	i64 coding = this->code[j + '\0'];
	i64 L = start[coding];
	i64 R = start[coding + 1] - 1;

	coding = code[i + '\0'];
	i64 Left = start[coding];
	i64 Right = start[coding + 1] - 1;
	if (coding == code[lastchar])
		Left++;
	for (i64 k = Left; k <= Right; k++)
		if (phivalue[k] >= L){
			Left = k;
			break;
		}
	return Left;
}
i64 Phi::right(i64 i, i64 j){
	i64 coding = this->code[j + '\0'];
	i64 L = start[coding];
	i64 R = start[coding + 1] - 1;

	coding = code[i + '\0'];
	i64 Left = start[coding];
	i64 Right = start[coding + 1] - 1;
	if (coding == code[lastchar])
		Left++;
	i64 k = Left;
	while (phivalue[k] <= R){
		if (k == Right)
			return Right;
		k++;
	}
	Right = k - 1;
	return Right;
}


i64 Phi::getEcodeSpace(int type){
	return phi->sizeofEcode(type);
}
