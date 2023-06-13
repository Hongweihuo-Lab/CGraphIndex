/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
#include "InArray.h"
#include<string.h>
#include<stdlib.h>
#include<iostream>
#include<math.h>
using namespace std;


InArray::~InArray(void){
	if(rankflag == 0)
		delete [] data;
    else if(rankflag == 1)
	{
		delete Md;
		delete Sd;
		delete SBd;
		delete Bd;
	}
	else
	{
		delete [] data;
		delete Md;
		delete Sd;
		delete SBd;
		delete Bd;
	}
}

InArray::InArray(){
	rankflag = 0;
}

InArray::InArray(u64 len, u32 size) {
	rankflag = 0;
	if(len<0||size<=0)
	{	
		cout<<"InArray out"<<endl;
		cerr<<len<<"\t"<<size<<endl;
		exit(0);
	}
		
	else{
		this->datanum =len;
		this->datawidth =size;
		u64 totlesize = datanum*datawidth;
		if(totlesize%32==0)
			totlesize=totlesize/32+1;
		else
			totlesize=(totlesize/32)+2;
		this->data =new u32[totlesize+1];
		memset(data,0,4*totlesize);
		mask = (u64)pow(2, datawidth) - 1;
	}
}
u64 InArray::GetrankMemorySize() {
	return Md->GetMemorySize()+Sd->GetMemorySize()+SBd->GetMemorySize()+Bd->GetMemorySize();
}

int ablog(u64 x){
	int ans=0;
	x--;
	while(x>0){
		ans++;
		x=x>>1;
	}
	return ans;
}
void InArray::SetValue(u64 index, u64 v){
	if(index>datanum-1|| index<0){
		cerr<<"InArray:index out of boundary"<<endl;
		cerr<<"index : "<<index<<"\tdatanum : "<<datanum<<endl;
		exit(0) ;
	}
	else if(v>mask){
		cerr<<v<<"	"<<mask<<"InArray:value is out of boundary"<<endl;
		exit(0) ;
	}
	else{
		u64 value=v;
		u64 anchor=(index*datawidth)>>5;
		u64 temp1=data[anchor];
		u32 temp2=data[anchor+1];
		temp1=(temp1<<32)+temp2;
		i32 overloop=((anchor+2)<<5)-(index+1)*datawidth;
		if (overloop < 0){
			i32 overloop2 = datawidth + overloop - 32;
			value = (value >> (-overloop));
			temp1 = temp1 + value;
			data[anchor + 1] = (temp1&(0xffffffff));
			data[anchor] = (temp1 >> 32)&(0xffffffff);
			data[anchor + 2] = (v << (32 + overloop))&(0xffffffff);
		}
		else{
			value = (value << overloop);
			temp1 = temp1 + value;
			data[anchor + 1] = (temp1&(0xffffffff));
			data[anchor] = (temp1 >> 32)&(0xffffffff);
		}
	}
}

void InArray::SetBitZero(u64 index)
{
	if(index>datanum-1|| index<0){
		cerr<<"InArray:index out of boundary"<<endl;
		cerr<<"index : "<<index<<"\tdatanum : "<<datanum<<endl;
		exit(0) ;
	}
	else{
		u64 anchor = index >> 5;
		u64 mod = 31 - (index & 0x1f);
		data[anchor] &= (0xffffffff - (1 << mod));
	}
}

void InArray::SetValue64(u64 index, u64 v){
	if(index>datanum-1|| index<0){
		cerr<<"InArray:index out of boundary"<<endl;
		cerr<<"index : "<<index<<"\tdatanum : "<<datanum<<endl;
		exit(0) ;
	}
	else if(v>mask){
		cerr<<v<<"	"<<mask<<"InArray:value is out of boundary"<<endl;
		exit(0) ;
	}
	else{
		u64 value=v;
		u64 anchor=(index*datawidth)>>5;
		u64 temp1=data[anchor];
		u32 temp2=data[anchor+1];
		temp1=(temp1<<32)+temp2;
		i32 overloop=((anchor+2)<<5)-(index+1)*datawidth;
		if (overloop < 0){
			i32 overloop2 = datawidth + overloop - 32;
			value = (value >> (-overloop));
			temp1 = temp1 + value;
			data[anchor + 1] = (temp1&(0xffffffff));
			data[anchor] = (temp1 >> 32)&(0xffffffff);
			data[anchor + 2] += (v << (32 + overloop))&(0xffffffff);
		}
		else{
			value = (value << overloop);
			temp1 = temp1 + value;
			data[anchor + 1] = (temp1&(0xffffffff));
			data[anchor] = (temp1 >> 32)&(0xffffffff);
		}
	}
}


u64 InArray::GetNum(){
	return datanum;
}

u64 InArray::GetMemorySize() {
	if(rankflag == 0)
		return (datanum*datawidth)/8;
	else if(rankflag == 2)
		return (datanum*datawidth)/8 + GetrankMemorySize(); 
}

u32 InArray::GetDataWidth() {
	return datawidth;
}



i64 InArray::write(savekit & s){
	if(rankflag == 0)
	{
		s.writeu64(datanum);
		s.writeu32(datawidth);
		i64 len=(datanum*datawidth);
		if(len%32==0)
			len=len/32 + 2;
		else
			len=len/32 + 3;
		s.writei64(len);
		s.writeu32array(data,len);
		return 1;
	}
	else if(rankflag == 1)
	{
		s.writeinteger(Blength);
		s.writeinteger(SBlength);
		s.writei64(block_len);
		s.writei64(super_block_len);
		s.writeu64(datanum);
		Md->write(s);
		Sd->write(s);
		SBd->write(s);
		Bd->write(s);
		return 1;
	}
	else
	{
		s.writeu64(datanum);
		s.writeu32(datawidth);
		i64 len=(datanum*datawidth);
		if(len%32==0)
			len=len/32 + 2;
		else
			len=len/32 + 3;
		s.writei64(len);
		s.writeu32array(data,len);

		s.writeinteger(Blength);
		s.writeinteger(SBlength);
		s.writei64(block_len);
		s.writei64(super_block_len);
		Md->write(s);
		Sd->write(s);
		SBd->write(s);
		Bd->write(s);
		return 1;
	}
}

i64 InArray::load(loadkit & s,int flag){
	if(flag == 0)
	{
		s.loadu64(datanum);
		s.loadu32(datawidth);
		i64 len=0;
		s.loadi64(len);
		data=new u32[len];
		s.loadu32array(data,len);
		mask=(((u64)1<<datawidth)-1);
		rankflag = 0;
		return 1;
	}
	else if(flag == 1)
	{
		s.loadinteger(Blength);
		s.loadinteger(SBlength);
		s.loadi64(block_len);
		s.loadi64(super_block_len);
		s.loadu64(datanum);
		Md = new InArray();
		Md->load(s,0);
		Sd = new InArray();
		Sd->load(s,0);
		SBd = new InArray();
		SBd->load(s,0);
		Bd = new InArray();
		Bd->load(s,0);
		rankflag = 1;
		return 1;
	}
	else
	{
		s.loadu64(datanum);
		s.loadu32(datawidth);
		i64 len=0;
		s.loadi64(len);
		data=new u32[len];
		s.loadu32array(data,len);
		mask=(((u64)1<<datawidth)-1);

		s.loadinteger(Blength);
		s.loadinteger(SBlength);
		s.loadi64(block_len);
		s.loadi64(super_block_len);
		Md = new InArray();
		Md->load(s,0);
		Sd = new InArray();
		Sd->load(s,0);
		SBd = new InArray();
		SBd->load(s,0);
		Bd = new InArray();
		Bd->load(s,0);
		rankflag = 2;
		return 1;
	}
}


void InArray::constructrank(int Blength,int SBlength)
{
	if(rankflag == 0)
		rankflag=1;
	this->Blength=ablog(Blength);
	this->SBlength=ablog(SBlength);
	i64 totlesize=datanum*datawidth;
	if(totlesize%32==0)
		totlesize=totlesize/32;
	else
		totlesize=(totlesize/32)+1;
	u64 sum1=0;
	for(int i=0;i<totlesize;i++)
		sum1+=(u32)__builtin_popcount(data[i]);
	u64 Mdm;
	if(datanum*datawidth%Blength==0)Mdm=datanum*datawidth/Blength;
	else Mdm=datanum*datawidth/Blength+1;
	this->block_len = Mdm;
	Md=new InArray(Mdm,1);
	Sd=new InArray(sum1,ablog(Blength));
    SBd=new InArray(((datanum*datawidth/Blength+1)/SBlength+2),ablog(sum1+1));
	Bd=new InArray(Mdm+2,ablog(Blength*(SBlength-1)+1));
	u64 Sdcount=0;
	int tem;
	u64 now=0;
	for(u64 i=0;i<Mdm;i++)
	{
		int flag=0;
		for(int j=0;j<Blength;j++)
		{
			now=i*Blength+j;
			if(now>=datanum) break;
			tem=GetValue(now);
			if(tem==1)
			{
				flag=1;
				Sd->SetValue(Sdcount,j);
				Sdcount++;
			}
		}
		if(flag!=0)Md->SetValue(i,1);
		else Md->SetValue(i,0);
	}
	i64 SBnum;
	if(Mdm%SBlength==0)SBnum=Mdm/SBlength+1;
	else SBnum=Mdm/SBlength+2;
	this->super_block_len = SBnum;
	i64 SBsum1=0;
	now=0;
	for(i64 i=0;i<SBnum;i++)
	{
		SBd->SetValue(i,SBsum1);
		i64 Bdsum1=0;
		for(i64 j=0;j<SBlength;j++)
		{
			if(now>=datanum)break;
			Bd->SetValue(i*SBlength+j,Bdsum1);
			for(i64 k=0;k<Blength;k++)
			{
				now=i*(SBlength*Blength)+j*Blength+k;
				if(now>=datanum)break;
				else{
					int tem=GetValue(now);
					if(tem==1)
					{
						SBsum1++;
						Bdsum1++;
					}
				}
			}
			
		}

	}
}

u64 InArray::select(u64 i){
	if(i < 0 || i > datanum){
		cout<<"select out boundary"<<endl;
		exit(EXIT_FAILURE);
	}
	if(i == 0) return -1;
	u64 l = 1,h = super_block_len - 1;
	u64 spj = blockbinarySearch(SBd,i,l,h);
	u64 k1 = i - SBd->GetValue(spj);
	l = spj << this->SBlength;
	h = ((spj + 1) << this->SBlength) - 1;
	
	if(h >= this->block_len){
		h = this->block_len - 1;
	}
	u64 bpj = blockbinarySearch(Bd,k1,l + 1,h);

	return (bpj << this->Blength) + Sd->GetValue(i - 1);
}

u64 InArray::blockbinarySearch(InArray * B,u64 i,u64 l ,u64 r){
	if(B->GetValue(l)==i) return l-1;
	if(B->GetValue(r)==i) {
		while(B->GetValue(r)==i){
			r--;
		}
		return r;
	}
	if(B->GetValue(r)<i) return r;
	while(l<=r){
		u64 midpos=(l+r)>>1;
		if(midpos==l){
			if(B->GetValue(l)>=i){
				return l-1;
			}
			else{
				return l;
			}
		}
		u64 mid=B->GetValue(midpos);
		if(mid<i) l=midpos+1;
		else if(mid>=i) r=midpos;
	}

}

void InArray::ReSet()
{
	u64 totlesize = datanum*datawidth;
	if(totlesize%32==0)
		totlesize=totlesize/32+1;
	else
		totlesize=(totlesize/32)+2;
	memset(data,0,4*totlesize);
}









