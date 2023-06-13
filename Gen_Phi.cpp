/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
#include<math.h>
#include<ctime>
#include "Gen_Phi.h"
#include<fstream>
#include<iostream>
#include<time.h>
#include<sys/time.h>
#include <omp.h>
#define MAXNUM 999999999
using namespace std;

GEN_Phi::GEN_Phi()
{
}



GEN_Phi::GEN_Phi(parmaters*csa, i64*phivalue,double multi)
{
	this->phivalue = phivalue;
	this->tablewidth = 16;
	
	this->zerostable_gamma = new u16[1 << tablewidth];
	this->decodevaluenum_gamma = new u16[1 << tablewidth];
	this->decodebitsnum_gamma = new u16[1 << tablewidth];
	this->decoderesult_gamma = new u16[1 << tablewidth];
	
	this->zerostable = new u16[1 << tablewidth];
	this->decodevaluenum = new u16[1 << tablewidth];
	this->decodebitsnum = new u16[1 << tablewidth];
	this->decoderesult = new u16[1 << tablewidth];


	this->alpsize = csa->alphabetsize;
	this->n = csa->n;
	this->a = csa->SL;
	this->b = csa->L;
	this->index = 0;
	this->multi = multi;

	this->InitionalTables_Gamma();
	this->InitionalTables();



	long max_threads = (long)omp_get_max_threads();
timeval bb, ee;
gettimeofday(&bb, NULL);
	this->SamplingAndCoding(csa,max_threads - 3);
gettimeofday(&ee, NULL);
cout<<"Cmp Phi Costs : "<<((ee.tv_sec * 1000000 + ee.tv_usec) - (bb.tv_sec * 1000000 + bb.tv_usec)) * 1.0 / 1000000<<" sec"<<endl;

//	this->SamplingAndCoding(csa);
	cerr<<"phi arr cus"<<endl;
	delete[] phivalue;

}

GEN_Phi::~GEN_Phi(void)
{
	delete[] superoffset;
	delete samples;
	delete offsects;
	delete[] sequence;

	delete[] zerostable_gamma;
	delete[] decodevaluenum_gamma;
	delete[] decodebitsnum_gamma;
	delete[] decoderesult_gamma;

	delete[] zerostable;
	delete[] decodevaluenum;
	delete[] decodebitsnum;
	delete[] decoderesult;
}



void GEN_Phi::InitionalTables_Gamma()
{
	integer D = this->tablewidth;
	u16 * R = this->zerostable_gamma;
	for (integer i = 0; i<D; i++)
		for (integer j = (1 << i); j<(2 << i); j++)
			R[j] = D - 1 - i;
	R[0] = D;
	u16 * Rn = this->decodevaluenum_gamma;
	u16 * Rb = this->decodebitsnum_gamma;
	u16 *Rx = this->decoderesult_gamma;
	u32 tablesize = (1 << tablewidth);
	u32 B[3] = { 0xffffffff, 0xffffffff, 0xffffffff };
	u32 *temp = this->sequence;
	this->sequence = B;
	i64 b = 0;
	i64 num = 0;
	i64 x = 0;
	i64 d = 0;
	i64 preb = 0;

	for (u32 i = 0; i<tablesize; i++)
	{
		B[0] = (i << 16);
		b = 0;
		num = 0;
		x = 0;
		d = 0;
		while (1)
		{
			this->Decodegamma(b, d);
			if (b>tablewidth)
				break;
			x = x + d;
			num++;
			preb = b;
		}
		Rn[i] = num;
		Rb[i] = preb;
		Rx[i] = x;
	}
	this->sequence = temp;

}
i64 blogsize_out(i64 x);
void  GEN_Phi::Append_Gamma(i64 x)
{
	u64 y = x;
	i64 zeronums = blogsize(y) - 1;
	index = index + zeronums;
	i64 valuewidth = zeronums + 1;
	i64 anchor = (index >> 5);
	u64 temp1=sequence[anchor];
	u32 temp2=sequence[anchor+1];
	temp1=(temp1<<32)+temp2;

	i64 overloop = ((anchor + 2) << 5) - index - valuewidth;
	if (overloop < 0)
	{
		u64 value2 = (y >> (-overloop));
		temp1 = temp1 + value2;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
		sequence[anchor + 2] = (y << (32 + overloop))&(0xffffffff);

	}
	else
	{
		y = (y << overloop);
		temp1 = temp1 + y;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
	}
	index = index + valuewidth;

}
void Append_Gamma_out(i64 x,i64& index,u32 *sequence)
{
	u64 y = x;
	i64 zeronums = blogsize_out(y) - 1;
	index = index + zeronums;
	i64 valuewidth = zeronums + 1;
	i64 anchor = (index >> 5);
	u64 temp1=sequence[anchor];
	u32 temp2=sequence[anchor+1];
	temp1=(temp1<<32)+temp2;

	i64 overloop = ((anchor + 2) << 5) - index - valuewidth;
	if (overloop < 0)
	{
		u64 value2 = (y >> (-overloop));
		temp1 = temp1 + value2;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
		sequence[anchor + 2] = (y << (32 + overloop))&(0xffffffff);

	}
	else
	{
		y = (y << overloop);
		temp1 = temp1 + y;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
	}
	index = index + valuewidth;
}

void GEN_Phi::Test()
{
	integer i = 0;
	integer k = 0;
	for (i = 0; i<n; i++)
	{
		if (phivalue[i] != GetValue(i))
			k++;
	}
	if (k == 0)
		cout << "Phi is right" << endl;
	else
		cout << "Phi is Sorry" << endl;
}
void GEN_Phi::Testforgetphivalue(){

	 int c = 20;
	 for (i64 i = 0; i < n; i++){
	 	if (this->GetValue(i) != phivalue[i]){
	 		cout << "i is: " << i << (int)this->method->GetValue(i/b) <<" "<<this->GetValue(i) <<" "<< phivalue[i]<< endl;
	 		c--;
	 		if(!c) exit(0);
	 	}
	 }
}

i64 GEN_Phi::blogsize(i64 x)
{

	i64 len = 0;
	while (x>0)
	{
		x = x >> 1;
		len++;
	}
	return len;
}

i64 blogsize_out(i64 x)
{

	i64 len = 0;
	while (x>0)
	{
		x = x >> 1;
		len++;
	}
	return len;
}

i64 deltasize_out(i64 v){
	i64  x = blogsize_out(v);
	i64 pre = 2 * blogsize_out(x) - 2;
	return pre + x - 1;

}



void GEN_Phi::InitionalTables(){
	integer D = this->tablewidth;
	u16 * R = this->zerostable;
	for (integer i = 0; i<D; i++)
		for (integer j = (1 << i); j<(2 << i); j++)
			R[j] = D - 1 - i;
	R[0] = D;
	u16 * Rn = this->decodevaluenum;
	u16 * Rb = this->decodebitsnum;
	u16 * Rx = this->decoderesult;
	u32 tablesize = (1 << tablewidth);
	u32 B[4] = { 0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff };
	u32 *temp = this->sequence;
	this->sequence = B;
	i64 b = 0;
	i64 d = 0;
	i64 num = 0;
	i64 preb = 0;
	i64 x = 0;
	for (u32 i = 0; i<tablesize; i++){
		B[0] = (i << 16);
		b = num = x = d = 0;
		while (1){
			this->DecodeDelta(b, d);
			if (b>16)
				break;
			if (d % 2 == 0){ 
				x = x + d / 2;
				num = num + d / 2;
			}
			else{
				x = x + (d + 1) / 2;
				num++;
			}
			preb = b;
		}
		Rn[i] = num;
		Rx[i] = x;
		Rb[i] = preb;
	}
	this->sequence = temp;

}

u64 GEN_Phi::GetBits(i64 position, i64 num){
	u64 anchor=position>>5;
	u64 temp1=sequence[anchor];
	u32 temp2=sequence[anchor+1];
	u32 temp3 = sequence[anchor + 2];
	i32 overloop = ((anchor + 2) << 5) - (position + num);
	temp1=(temp1<<32)+temp2;
	if (overloop < 0){
		temp1 = (temp1 << (-overloop)) + (temp3 >> (32 + overloop));
		return temp1&((1ull<<num) - 1);
	}
	else
		return (temp1>>overloop)&((1ull<<num) - 1);
}


i64 GEN_Phi::DecodeDelta(i64 & position, i64 &value){
	i64 pos = position;
	i64 x = this->ZerosRun(position);
	i64 v = GetBits(position, x + 2);
	position = position + x + 2;

	if (v>32){
		position = (1 << 30);
		return (1 << 30);
	}
	value = (1 << (v - 1)) + GetBits(position, v - 1);
	position = position + v - 1;
	return position - pos;
}

i64 GEN_Phi::DecodeDeltaBlock(i64 position, i64 base, i64 num){
	i64 i = 0;
	i64 value = 0;
	while (i<num){
		DecodeDelta2(position, value);
		if (value % 2 == 0){
			if (i + value / 2 >= num)
				return (base + num - i) % n;
			base = (base + value / 2) % n;
			i = i + value / 2;
		}
		else{ 
			base = (base + (value + 1) / 2) % n;
			i++;
		}
	}
	return base;
}

i64 GEN_Phi::DecodeDelta2(i64& position, i64 & value){
	i64 pos = position;
	i64 x = this->ZerosRun(position);
	i64 v = GetBits(position, x + 2);
	position = position + x + 2;
	value = ((uint64_t)1 << (v - 1)) + GetBits(position, v - 1);
	position = position + v - 1;
	return position - pos;
}

i64 GEN_Phi::ZerosRun(i64 &position){
	i64 y = 0;
	i64 D = this->tablewidth;
	u64 x = GetBits(position, D);
	i64 w = y = zerostable[x];
	while (y == D){
		position = position + D;
		x = GetBits(position, D);
		y = zerostable[x];
		w = w + y;
	}
	position = position + y;
	return w;
}

i64 GEN_Phi::DecodeDelta3(i64 &position, i64&value, i64 zero){
	i64 pos = position;
	i64 v = GetBits(position + zero, zero + 2);
	position = position + 2 * zero + 2;
	value = ((uint64_t)1 << (v - 1)) + GetBits(position, v - 1);
	position = position + v - 1;
	return position - pos;
}
i64 GEN_Phi::deltasize(i64 v){
	i64  x = blogsize(v);
	i64 pre = 2 * blogsize(x) - 2;
	return pre + x - 1;

}

void GEN_Phi::Append(i64 x){

	i64 size = blogsize(x);

	u64 y = size;
	i64 zeronums = blogsize(size) - 2;
	index = index + zeronums;
	i64 valuewidth = zeronums + 2;
	i64 anchor = (index >> 5);
	u64 temp1=sequence[anchor];
	u32 temp2=sequence[anchor+1];
	temp1=(temp1<<32)+temp2;
	i64 overloop = ((anchor + 2) << 5) - index - valuewidth;
	if (overloop < 0)
	{
		u64 value2 = (y >> (-overloop));
		temp1 = temp1 + value2;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
		sequence[anchor + 2] = (y << (32 + overloop))&(0xffffffff);

	}
	else
	{
		y = (y << overloop);
		temp1 = temp1 + y;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
	}
	index = index + valuewidth;

	valuewidth = size - 1;
	y = x;
	y = (y ^ ((uint64_t)1 << valuewidth));
	anchor = (index >> 5);
	temp1=sequence[anchor];
	temp2=sequence[anchor+1];
	temp1=(temp1<<32)+temp2;
	overloop = ((anchor + 2) << 5) - index - valuewidth;
	if (overloop < 0)
	{
		u64 value2 = (y >> (-overloop));
		temp1 = temp1 + value2;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
		sequence[anchor + 2] = (y << (32 + overloop))&(0xffffffff);

	}
	else
	{
		y = (y << overloop);
		temp1 = temp1 + y;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
	}
	index = index + valuewidth;
}

void Append_out(i64 x,i64& index,u32 *sequence)
{
	i64 size = blogsize_out(x);

	u64 y = size;
	i64 zeronums = blogsize_out(size) - 2;
	index = index + zeronums;
	i64 valuewidth = zeronums + 2;
	i64 anchor = (index >> 5);
	u64 temp1=sequence[anchor];
	u32 temp2=sequence[anchor+1];
	temp1=(temp1<<32)+temp2;
	i64 overloop = ((anchor + 2) << 5) - index - valuewidth;
	if (overloop < 0)
	{
		u64 value2 = (y >> (-overloop));
		temp1 = temp1 + value2;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
		sequence[anchor + 2] = (y << (32 + overloop))&(0xffffffff);

	}
	else
	{
		y = (y << overloop);
		temp1 = temp1 + y;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
	}
	index = index + valuewidth;

	valuewidth = size - 1;
	y = x;
	y = (y ^ ((uint64_t)1 << valuewidth));
	anchor = (index >> 5);
	temp1=sequence[anchor];
	temp2=sequence[anchor+1];
	temp1=(temp1<<32)+temp2;
	overloop = ((anchor + 2) << 5) - index - valuewidth;
	if (overloop < 0)
	{
		u64 value2 = (y >> (-overloop));
		temp1 = temp1 + value2;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
		sequence[anchor + 2] = (y << (32 + overloop))&(0xffffffff);

	}
	else
	{
		y = (y << overloop);
		temp1 = temp1 + y;
		sequence[anchor + 1] = (temp1&(0xffffffff));
		sequence[anchor] = (temp1 >> 32)&(0xffffffff);
	}
	index = index + valuewidth;
}



i64 GEN_Phi::GetValue(i64 index){
	i64 position = 0;
	return GetValue(index, position);
}

i64 GEN_Phi::GetValue(i64 index, i64 & position)
{
	i64 index_b = index / b;
	i64 base = samples->GetValue(index_b);
	int overloop = index%b;
	if (overloop == 0)
		return base;
	position = superoffset[index / a] + offsects->GetValue(index_b);
	int m = this->method->GetValue(index_b);
	switch (m){
	case gamma:{
		i64 x = 0;
		i64 d = 0;

		i64 i = 0;
		i64 p = 0;
		i64 num = 0;
		i64 zero = 0;
		while (i < overloop)
		{
			p = this->GetBits_Gamma(position, 16);
			num = this->decodevaluenum_gamma[p];
			if (num == 0)
			{
				zero = this->zerostable_gamma[p];
				if (zero == 16)
					this->Decodegamma(position, d);
				else
					this->Decodegamma(position, d, zero);
				x = (x + d) % n;
				i++;
			}
			else
			{
				if (i + num > overloop)
					break;
				i = i + num;
				position = position + this->decodebitsnum_gamma[p];
				x = (x + this->decoderesult_gamma[p]) % n;
			}
		}

		for (; i < overloop; i++)
		{
			this->Decodegamma(position, d);
			x = x + d;
			x = x%n;
		}
		return (base + x) % n;
	}
	case bv:{
		u64 value = 0;
		int k = 0;
		int num1 = 0;
		i64 count = 0;
		while (num1 < overloop){
			value = GetBits_Gamma(position, 32);
			k = popcnt(value);
			num1 += k;
			position += 32;
			count++;
		}
		if (num1 >= overloop){
			num1 -= k;
			position -= 32;
			count--;
		}
		i64 off = select_in_word_big(value, overloop - num1) - 32;////////?????
		return (base + 32 * count + off) % n;
	}
	case rldelta:{
		return DecodeDeltaBlock(position,base, overloop);
	}
	case allone:{
		return (base + overloop) % n;
	}
	default:cout << "get value method erro" << endl; break;
	}
}
void getpartsize(i64 *ps,i64 *st,i64 num,i64 x)
{
	i64 base = x / num;
	i64 adition = x % num;
	st[0] = 0;
	for(int i = 0;i < num;++i)
	{
		if(adition > 0)
		{
			ps[i] = base + 1;
			--adition;
		}
		else
			ps[i] = base;
		if(i < num - 1)
			st[i + 1] = st[i] + ps[i];
	}
}

struct mutle_phi
{
	i64 n;
	i64 a;
	int b;
	i64* phivalue;
	i64 partsize;
	i64 start;
	double multi;
	int id;

	i64 *bsize;//小块的个数
	i64 *tlen;//每个线程压缩后的总长度
	i64 *bmlen;//最长的小块的长度
	i64 *index_res;//每个线程压缩后的总长度
	i64 *glsboff;//每个线程的超块的第一个数值的起始值
	InArray **mtd;//编码方法
	InArray **boff;//boffset
	InArray **first;//每个小块的头元素
	u32 **seq;//编码序列
	i64 **sboff;//sboffset

};
void* work_phi(void *x)
{
	mutle_phi* arg = (mutle_phi*)x;
	i64 i, j;
	i64 pre = 0;
	i64 totallen = 0;
	i64 maxlen = 1;
	i64 len = 0;

	i64 len_g = 0;
	i64 len_bv = 0;
	i64 len_all = 0;
	i64 len_rldelta = 0;
	i64 num_g = 0;
	i64 num_bv = 0;
	i64 num_all = 0;
	i64 num_rldelta = 0;
	i64 gap = 0;
	i64 beg = arg->start;
	i64 partsize = arg->partsize;
	i64 end = arg->start + partsize;	
	i64 a = arg->a;
	i64 b = arg->b;
	i64 n = arg->n;
	i64 *phivalue = arg->phivalue;
	i64 med_idx = 0;
	double multi = arg->multi;
	InArray *method = new InArray(a / b * partsize, 2);
	for (i = beg; i<end; i++)
	{
		for(j = i*a; j<(i + 1)*a&&j<n; j++)
		{
			if (j % b == 0)
			{
				pre = phivalue[j];
				if ((j / b + 1)*b - 1 < n){
					len_bv = phivalue[(j / b + 1)*b - 1] - phivalue[j];
				}
				else{
					len_bv = MAXNUM;
				}
				len_g = 0;
				len_all = 0;
				len_rldelta = 0;
				continue;
			}
			gap = phivalue[j] - pre;
			if (gap < 0){
				gap = n + gap;
				len_bv = MAXNUM;
			}
			if(gap == 1) {
				len_all++;
			}else{
				if (len_all != 0){
					len_rldelta = len_rldelta + deltasize_out(2 * len_all);
				}
				len_rldelta = len_rldelta + deltasize_out(2 * gap - 1);
				len_all = 0;
			}
			len_g = len_g + blogsize_out(gap) * 2 - 1;
			pre = phivalue[j];
			if (j + 1 == n){
				method->SetValue(med_idx++, 0); //gamma
				len += len_g;
				num_g++;
			}

			else{
				if ((j + 1) % b == 0){
					if (len_all > 0){
						len_rldelta = len_rldelta + deltasize_out(2 * len_all);
					}
					if(len_all == (b - 1)){
						method->SetValue(med_idx++,2);//allone
						num_all++;
						len+=0;
					}
					else if ((len_g * multi) < len_bv && len_g < len_rldelta){
						method->SetValue(med_idx++, 0);//gamma
						len += len_g;
						num_g++;
					}
					else if((len_rldelta * multi) < len_bv){
						method->SetValue(med_idx++,3);//rldelta
						len += len_rldelta;
						num_rldelta++;
					}
					else{
						method->SetValue(med_idx++, 1);//bv
						len += len_bv;
						num_bv++;
					}
				}
			}
		}
		if (len>maxlen)
			maxlen = len;

		totallen = totallen + len;
		len = 0;
	}
	arg->bsize[arg->id] = med_idx;
	arg->tlen[arg->id] = totallen;
	arg->bmlen[arg->id] = maxlen;

	i64 lenofsequence = totallen / 32 + 3;
	u32 *sequence = new u32[lenofsequence];
	for (i = 0; i<lenofsequence; i++)
		sequence[i] = 0;
	InArray *offsects = new InArray(med_idx, blogsize_out(maxlen));
	InArray *samples = new InArray(med_idx, blogsize_out(n));
	i64 lenofsuperoffset = partsize;
	i64 *superoffset = new i64[lenofsuperoffset];

	i64	bvsum = 0;
	i64	gamasum = 0;
	i64	rldatasum = 0;
	i = 0;
	j = 0;
	gap = 0;
	i64 index1 = 0;
	i64 index2 = 0;
	i64 len1 = 0;
	i64 len2 = 0;
	i64 m = 0;
	u64 value = 0;
	i64 sam = 0;
	u32 u = 1;
	i64 runs = 0;
	i64 index = 0;
	beg = a * arg->start;
	end = beg + partsize * a;
	med_idx = 0;
	for (i64 i = beg; i < end && i< n; i++)
	{
		if (i%a == 0){
			len2 = len1;
			superoffset[index2] = len2;
			index2++;
		}
		
		if (i%b == 0){
			samples->SetValue(index1, phivalue[i]);
			offsects->SetValue(index1, len1 - len2);
			index1++;
			pre = phivalue[i];
			sam = phivalue[i];

			m = method->GetValue(med_idx++);
			if (m > 3 || m <0)
				cout << "WRONG " << method << " " << i / b << endl;
			continue;
		}
		gap = phivalue[i] - pre;
		if (gap<0)
			gap = n + gap;
		pre = phivalue[i];

		switch (m){
		case 0://gamma
		{
			len1 = len1 + blogsize_out(gap) * 2 - 1;
			gamasum +=  blogsize_out(gap) * 2 - 1;
			Append_Gamma_out(gap,index,sequence); 
			break;
		}
		case 1://bv
		{
			len1 = len1 + phivalue[i] - sam;
			bvsum += phivalue[i] - sam;
			index += (phivalue[i] - sam);
			sam = phivalue[i];
			u = (u << (31 - (len1 - 1) % 32));
			sequence[(len1 - 1) / 32] |= u;
			u = 1;
			break;
		}
		case 3://rldelta
		{
			if (gap == 1){
				runs++;
				if (((i + 1) % b == 0)||(i+1)==n){
					len1 = len1 + deltasize_out(2 * runs);
					rldatasum += deltasize_out(2 * runs);
					Append_out(2 * runs,index,sequence);
					runs = 0;
				}
			}
			else{
				if (runs != 0){
					len1 = len1 + deltasize_out(2 * runs);
					rldatasum += deltasize_out(2 * runs);
					Append_out(2 * runs,index,sequence);
				}

				len1 = len1 + deltasize_out(2 * gap - 1);
				rldatasum += deltasize_out(2 * gap - 1);
				Append_out(2 * gap - 1,index,sequence);
				runs = 0;
			};
			break;
		}
		case 2:
			break;
		default:cout << "erro method" << endl; break;
		}
		if(i == end - 1)
		{
			len2 = len1;
			arg->glsboff[arg->id + 1] = len2;
		}
	}
	arg->index_res[arg->id] = index;
	arg->mtd[arg->id] = method;
	arg->boff[arg->id] = offsects;
	arg->first[arg->id] = samples;
	arg->seq[arg->id] = sequence;
	arg->sboff[arg->id] = superoffset;
/*
	{
		MutexLock tmpmut(mut);
		++pnums;
	}
	*/
	pthread_exit(NULL);
}
struct wboff
{
	i64 num;
	i64 *bsize;
	InArray *offsects;
	InArray **boff;
};
void* work_boff(void *x)
{
	wboff* arg = (wboff*)(x);
	i64 idx = 0;
	for(i64 i = 0;i < arg->num;++i)
	{
		for(i64 j = 0;j < arg->bsize[i];++j)
		{
			i64 value = arg->boff[i]->GetValue(j);
			arg->offsects->SetValue(idx++,value);
		}
		delete arg->boff[i];	
	}
	/*
	{
		MutexLock tmpmut(mut);
		++pnums;
	}
	*/
	pthread_exit(NULL);
}	

struct wfirst
{
	i64 num;
	i64 *bsize;
	InArray *samples;
	InArray **first;
};
void* work_first(void *x)
{
	wfirst* arg = (wfirst*)(x);
	i64 idx = 0;
	for(i64 i = 0;i < arg->num;++i)
	{
		for(i64 j = 0;j < arg->bsize[i];++j)
		{
			i64 value = arg->first[i]->GetValue(j);
			arg->samples->SetValue(idx++,value);
		}
		delete arg->first[i];	
	}
	/*
	{
		MutexLock tmpmut(mut);
		++pnums;
	}
	*/
	pthread_exit(NULL);
}	
struct wsboff
{
	i64 num;
	i64 *partsize;
	i64 *superoffset;
	i64 **sboff;
	i64 *glsboff;
};
void* work_sboff(void *x)
{
	wsboff* arg = (wsboff*)(x);
	i64 idx = 0;
	for(i64 i = 0;i < arg->num;++i)
	{
		for(i64 j = 0;j < arg->partsize[i];++j)
			arg->superoffset[idx++] = arg->sboff[i][j] + arg->glsboff[i];
		delete arg->sboff[i];	
	}
	/*
	{
		MutexLock tmpmut(mut);
		++pnums;
	}
	*/
	pthread_exit(NULL);
}	

struct wseq
{
	i64 num;
	i64 *index_res;
	u32 *sequence;
	u32 **seq;
};
void BitCopy(u32 * temp,i64 & index,u32 value)
{
	if(index%32!=0)
	{
		int first = 32 - index % 32;
		int second = 32 - first;
		temp[index/32] = (temp[index/32] | (value>>second));
		temp[index/32 + 1] = (temp[index/32+1] | (value<<first));
	}
	else
		temp[index/32]  = value;
	index = index +32;
}
u64 GetBits(u32 * buff,i64 index,int bits)
{

	if((index & 0x1f) + bits < 33)
		return (buff[index>>5]<<(index &0x1f))>>(32-bits);

	int first = 32 - (index &0x1f);
	int second = bits - first;
	u64 high = (buff[index>>5] & ((1<<first)-1)) << second;
	return high + (buff[(index>>5)+1]>>(32-second));


}
void BitCopy1(u32 * temp,i64 &index,u32 *data,i64 index1,i64 len)
{
	while(len>=32){
		u32 value=GetBits(data,index1,32);
		BitCopy(temp,index,value);
		len-=32;
		index1+=32;
	}
	if(len!=0){
		u32 value=GetBits(data,index1,len);
		if(index%32!=0){
			int first = index % 32;
			if(first+len<33){
				int overloop=32-(first+len);
				value=(value<<overloop);
				temp[index/32]=(temp[index/32]|value);
			}
			else{
				first=32-first;
				int second=len-first;
				int third=32-second;
				temp[index/32]=(temp[index/32]|(value>>second));
				temp[index/32+1]=temp[index/32+1]|(value<<third);
			}
		}
		else{
			int overloop=32-len;
			value=(value<<overloop);
			temp[index/32]=value;
		}
		index = index+len;
	}
}
void* work_seq(void *x)
{
	wseq* arg = (wseq*)(x);
	i64 idx = 0;
	for(i64 i = 0;i < arg->num;++i)
	{
		BitCopy1(arg->sequence, idx, arg->seq[i], 0, arg->index_res[i]);
		delete arg->seq[i];
	}
/*
	{
		MutexLock tmpmut(mut);
		++pnums;
	}
	*/
	pthread_exit(NULL);
}	



void GEN_Phi::SamplingAndCoding(parmaters *csa,int num)
{
	//hzt
	//hzt
	pthread_t *tid = new pthread_t[num];
	pthread_attr_t attr;
	pthread_attr_init(&attr);
//	pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);
	i64 x = n / a + 1;
	i64 *partsize = new i64[num]; //每个线程的块大小
	i64 *start = new i64[num]; //每个线程的起始块
	getpartsize(partsize,start,num,x);
	i64 bsize[num];//小块的个数
	i64 tlen[num];//每个线程压缩后的总长度
	i64 bmlen[num];//最长的小块的长度
	i64 index_res[num];//每个线程压缩后的总长度
	i64 tmpnum = num + 1;
	i64 glsboff[tmpnum];//每个线程的超块的第一个数值的起始值
	glsboff[0] = 0;
	InArray *mtd[num];//编码方法
	InArray *boff[num];//boffset
	InArray *first[num];//每个小块的头元素
	u32 *seq[num];//编码序列
	i64 *sboff[num];//sboffset
	mutle_phi tmparg[num];
	for(i64 i = 0;i < num;++i)
	{	
		tmparg[i].id = i;
		tmparg[i].a = a;
		tmparg[i].b = b;
		tmparg[i].n = n;
		tmparg[i].multi = multi;
		tmparg[i].partsize = partsize[i];
		tmparg[i].start = start[i];
		tmparg[i].phivalue = phivalue;

		tmparg[i].bsize = bsize;
		tmparg[i].tlen = tlen;
		tmparg[i].bmlen = bmlen;
		tmparg[i].index_res = index_res;
		tmparg[i].glsboff = glsboff;
		tmparg[i].mtd = mtd;
		tmparg[i].boff = boff;
		tmparg[i].first = first;
		tmparg[i].seq = seq;
		tmparg[i].sboff = sboff;
	}
	//hzt
	//hzt
	for(i64 i = 0;i < num;++i)
	{
		int ret = pthread_create(&tid[i], &attr, work_phi,&tmparg[i]);
		if(ret != 0)
		{
			cerr<<"pthread_create error!!!"<<endl;
			exit(1);
		}	
	}
	//主线程一直在等
	for(u64 i = 0;i < num;++i)
		pthread_join(tid[i], NULL);
	/*
	while(1)
	{
		{
			MutexLock tmpmut(mut);
			if(pnums == num)
				break;
		}
	}
	*/
	for(i64 i = 1;i < num + 1;++i)
		glsboff[i] += glsboff[i - 1];



/*
	i64 maxlen = 1;
	for(i64 i = 0;i < num;++i)
	{
		if(bmlen[i] > maxlen)
			maxlen = bmlen[i];
	}
	offsects = new InArray(n / b + 1, blogsize(maxlen));
	i64 idx = 0;
	for(i64 i = 0;i < num;++i)
	{
		for(i64 j = 0;j < bsize[i];++j)
		{
			i64 value = boff[i]->GetValue(j);
			offsects->SetValue(idx++,value);
		}
		delete boff[i];	
	}

	samples = new InArray(n / b + 1, blogsize(n));

	idx = 0;
	for(i64 i = 0;i < num;++i)
	{
		for(i64 j = 0;j < bsize[i];++j)
		{
			i64 value = first[i]->GetValue(j);
			samples->SetValue(idx++,value);
		}
		delete first[i];	
	}

	lenofsequence = 0;
	for(i64 i = 0;i < num;++i)
		lenofsequence += tlen[i];
	lenofsequence = lenofsequence / 32 + 3;
	lenofsuperoffset = n / a + 1;
	sequence = new u32[lenofsequence];
	for (int i = 0; i < lenofsequence; i++)
		sequence[i] = 0; 
	superoffset = new i64[lenofsuperoffset];
	memset(superoffset,0,8*lenofsuperoffset);
	idx = 0;
	for(i64 i = 0;i < num;++i)
	{
		for(i64 j = 0;j < partsize[i];++j)
			superoffset[idx++] = sboff[i][j] + glsboff[i];
		delete sboff[i];	
	}

	i64 idx = 0;
	for(i64 i = 0;i < num;++i)
	{
		BitCopy1(sequence, idx, seq[i], 0, index_res[i]);
		delete seq[i];
	}

	method = new InArray(n / b + 1, 2);///////////////////////////////	
	i64 idx = 0;
	for(i64 i = 0;i < num;++i)
	{
		for(i64 j = 0;j < bsize[i];++j)
		{
			i64 value = mtd[i]->GetValue(j);
			method->SetValue(idx++,value);
		}	
		delete mtd[i];	
	}
	for(i64 i = 0;i < num;++i)
		index += index_res[i];
*/


	pthread_t tid_[4];
	int ret;
	wboff arg_wboff;
	arg_wboff.bsize = bsize;
	arg_wboff.num = num;
	arg_wboff.boff = boff;
	i64 maxlen = 0;
	for(i64 i = 0;i < num;++i)
	{
		if(bmlen[i] > maxlen)
			maxlen = bmlen[i];
	}
	offsects = new InArray(n / b + 1, blogsize(maxlen));
	arg_wboff.offsects = offsects;
	ret = pthread_create(&tid_[0], &attr, work_boff,&arg_wboff);
	if(ret != 0)
	{
		cerr<<"pthread_create error!!!"<<endl;
		exit(1);
	}	


	samples = new InArray(n / b + 1, blogsize(n));
	wfirst arg_wfirst;
	arg_wfirst.num = num;
	arg_wfirst.bsize = bsize;
	arg_wfirst.samples = samples;
	arg_wfirst.first = first;
	ret = pthread_create(&tid_[1], &attr, work_first,&arg_wfirst);
	if(ret != 0)
	{
		cerr<<"pthread_create error!!!"<<endl;
		exit(1);
	}	


	lenofsequence = 0;
	for(i64 i = 0;i < num;++i)
		lenofsequence += tlen[i];
	lenofsequence = lenofsequence / 32 + 3;
	lenofsuperoffset = n / a + 1;
	sequence = new u32[lenofsequence];
	for (int i = 0; i < lenofsequence; i++)
		sequence[i] = 0; 
	superoffset = new i64[lenofsuperoffset];
	memset(superoffset,0,8*lenofsuperoffset);
	wsboff arg_wsboff;
	arg_wsboff.num = num;
	arg_wsboff.partsize = partsize;
	arg_wsboff.superoffset = superoffset;
	arg_wsboff.sboff = sboff;
	arg_wsboff.glsboff = glsboff;
	ret = pthread_create(&tid_[2], &attr, work_sboff,&arg_wsboff);
	if(ret != 0)
	{
		cerr<<"pthread_create error!!!"<<endl;
		exit(1);
	}	

	wseq arg_wseq;
	arg_wseq.num = num;
	arg_wseq.index_res = index_res;
	arg_wseq.sequence = sequence;
	arg_wseq.seq = seq;
	ret = pthread_create(&tid_[3], &attr, work_seq,&arg_wseq);
	if(ret != 0)
	{
		cerr<<"pthread_create error!!!"<<endl;
		exit(1);
	}	

	
	method = new InArray(n / b + 1, 2);///////////////////////////////	
	i64 idx = 0;
	for(i64 i = 0;i < num;++i)
	{
		for(i64 j = 0;j < bsize[i];++j)
		{
			i64 value = mtd[i]->GetValue(j);
			method->SetValue(idx++,value);
		}	
		delete mtd[i];	
	}
	for(i64 i = 0;i < num;++i)
		index += index_res[i];

	for(u64 i = 0;i < 4;++i)
		pthread_join(tid_[i], NULL);
/*
	while(1)
	{
		{
			MutexLock tmpmut(mut);
			if(pnums == 4)
				break;
		}
	}
	*/
}

void GEN_Phi::SamplingAndCoding(parmaters *csa)
{
	i64 i, j;

	i64 pre = 0;
	i64 totallen = 0;
	i64 maxlen = 0;
	i64 len = 0;
	i64 x = n / a;
	method = new InArray(n / b + 1, 2);

	i64 len_g = 0;
	i64 len_bv = 0;
	i64 len_all = 0;
	i64 len_rldelta = 0;
	i64 num_g = 0;
	i64 num_bv = 0;
	i64 num_all = 0;
	i64 num_rldelta = 0;
	i64 gap = 0;
	for (i = 0; i<x + 1; i++)
	{
		for (j = i*a; j<(i + 1)*a&&j<n; j++)
		{
			if (j%b == 0)
			{
				pre = phivalue[j];
				if ((j / b + 1)*b - 1 < n){
					len_bv = phivalue[(j / b + 1)*b - 1] - phivalue[j];
				}
				else{
					len_bv = MAXNUM;
				}
				len_g = 0;
				len_all = 0;
				len_rldelta = 0;
				continue;
			}
			gap = phivalue[j] - pre;
			if (gap < 0){
				gap = n + gap;
				len_bv = MAXNUM;
			}
			if(gap == 1) {
				len_all++;
			}else{
				if (len_all != 0){
					len_rldelta = len_rldelta + deltasize(2 * len_all);
				}
				len_rldelta = len_rldelta + deltasize(2 * gap - 1);
				len_all = 0;
			}
			len_g = len_g + blogsize(gap) * 2 - 1;
			pre = phivalue[j];

			if (j + 1 == n){
				method->SetValue(j / b, gamma); 
				len += len_g;
				num_g++;
			}
			else{
				if ((j + 1) % b == 0){
					
					if (len_all > 0){
						len_rldelta = len_rldelta + deltasize(2 * len_all);
					}
					if(len_all == (b - 1)){
						method->SetValue(j/b,allone);
						num_all++;
						len+=0;
					}
					else if ((len_g * this->multi) < len_bv && len_g < len_rldelta){
						method->SetValue(j / b, gamma);
						len += len_g;
						num_g++;
					}
					else if((len_rldelta * this->multi) < len_bv){
						method->SetValue(j / b,rldelta);
						len += len_rldelta;
						num_rldelta++;
					}
					else{
						method->SetValue(j / b, bv);
						len += len_bv;
						num_bv++;
					}
				}
			}
		}


		if (len>maxlen)
			maxlen = len;

		totallen = totallen + len;
		len = 0;
	}

	this->lenofsequence = totallen / 32 + 3;
	this->sequence = new u32[lenofsequence];
	for (i = 0; i<lenofsequence; i++)
		sequence[i] = 0; 
	this->offsects = new InArray(n / b + 1, blogsize(maxlen));
	samples = new InArray(n / b + 1, blogsize(n));

	this->lenofsuperoffset = n / a + 1;
	superoffset = new i64[lenofsuperoffset];

	#ifdef TESTDATA
	cout<<endl<<"ecode block ratio"<<endl;
	cout << "block_gamma: " <<1.0* num_g / (num_g + num_bv + num_all  + num_rldelta) << endl;
	cout << "block_bv: " << 1.0* num_bv / (num_g + num_bv + num_all  + num_rldelta) << endl;
	cout << "block_allone: " << 1.0* num_all / (num_g + num_bv + num_all  + num_rldelta) << endl;
	cout << "block_rldelta: " << 1.0* num_rldelta / (num_g + num_bv + num_all  + num_rldelta) << endl;
	#endif
	bvsum = 0;
	gamasum = 0;
	rldatasum = 0;
	i = 0;
	j = 0;

	gap = 0;
	i64 index1 = 0;
	i64 index2 = 0;
	i64 len1 = 0;
	i64 len2 = 0;
	i64 m = 0;
	u64 value = 0;
	i64 sam = 0;
	u32 u = 1;
	i64 runs = 0;


	for (i64 i = 0; i<n; i++)
	{
		assert(len1 == this->index);

		if (i%a == 0){
			len2 = len1;
			superoffset[index2] = len2;
			index2++;
		}
		if (i%b == 0){
			samples->SetValue(index1, phivalue[i]);
			offsects->SetValue(index1, len1 - len2);
			index1++;
			pre = phivalue[i];
			sam = phivalue[i];

			m = method->GetValue(i / b);
			if (m > 3 || m <0)
				cout << "WRONG " << method << " " << i / b << endl;
			continue;
		}
		gap = phivalue[i] - pre;
		if (gap<0)
			gap = n + gap;
		pre = phivalue[i];

		switch (m){
		case gamma:
		{
			len1 = len1 + blogsize(gap) * 2 - 1;
			gamasum +=  blogsize(gap) * 2 - 1;
			Append_Gamma(gap); 
			break;
		}
		case bv:
		{
			len1 = len1 + phivalue[i] - sam;
			bvsum += phivalue[i] - sam;
			this->index += (phivalue[i] - sam);
			sam = phivalue[i];
			u = (u << (31 - (len1 - 1) % 32));
			this->sequence[(len1 - 1) / 32] |= u;
			u = 1;
			break;
		}
		case rldelta:
		{
			if (gap == 1){
				runs++;
				if (((i + 1) % b == 0)||(i+1)==n){
					len1 = len1 + deltasize(2 * runs);
					rldatasum += deltasize(2 * runs);
					this->Append(2 * runs);
					runs = 0;
				}
			}
			else{
				if (runs != 0){
					len1 = len1 + deltasize(2 * runs);
					rldatasum += deltasize(2 * runs);
					this->Append(2 * runs);
				}

				len1 = len1 + deltasize(2 * gap - 1);
				rldatasum += deltasize(2 * gap - 1);
				this->Append(2 * gap - 1);
				runs = 0;
			};

			break;
		}
		case allone:
			break;
		default:cout << "erro method" << endl; break;
		}

	}

}
i64 GEN_Phi::GetMemorySize()
{
	return samples->GetMemorySize() + this->offsects->GetMemorySize() + this->lenofsequence * 4 + this->lenofsuperoffset * 8+method->GetMemorySize();
}
i64 GEN_Phi::GetOneBit(i64 i)
{
	i64 anchor = i / 32;
	i64 position = i % 32;
	return (sequence[anchor] & 1 << (31 - position)) >> (31 - position);
}

i64  GEN_Phi::Decodegamma(i64 & position, i64 &x)
{
	i64 a = this->ZerosRun_Gamma(position);
	x = this->GetBits_Gamma(position, a + 1);
	position = position + a + 1;
	return 2 * a + 1;
}

i64 GEN_Phi::Decodegamma(i64 &position, i64&x, i64 zero){
	x = this->GetBits_Gamma(position + zero, zero + 1);
	position += (2 * zero + 1);
	return 2 * zero + 1;
}

i64 GEN_Phi::ZerosRun_Gamma(i64 &position)
{
	i64 y = 0;
	i64 D = this->tablewidth;
	i64 x = this->GetBits_Gamma(position, D);
	i64 w = y = this->zerostable_gamma[x];
	while (y == D)
	{
		position = position + D;
		x = this->GetBits_Gamma(position, D);
		y = this->zerostable_gamma[x];
		w = w + y;
	}
	position = position + y;
	return w;
}

u64 GEN_Phi::GetBits_Gamma(i64 position, i64 num){
	
	u64 anchor=position>>5;
	u64 temp1=sequence[anchor];
	u32 temp2=sequence[anchor+1];
	u32 temp3 = sequence[anchor + 2];
	i32 overloop = ((anchor + 2) << 5) - (position + num);
	temp1=(temp1<<32)+temp2;
	if (overloop < 0){
		temp1 = (temp1 << (-overloop)) + (temp3 >> (32 + overloop));
		return temp1&((1ull<<num) - 1);
	}
	else
		return (temp1>>overloop)&((1ull<<num) - 1);
	
	
}



i64 GEN_Phi::RightBoundary(i64 pr, i64 l, i64 r)
{
	i64 ans = 0;
	i64 SL = this->a;
	i64 L = this->b;
	i64 lb = (l + L - 1) / L;
	i64 rb = r / L;
	i64 b = lb - 1;
	i64 m = 0;
	i64 x = 0;
	while (lb <= rb)
	{
		m = (lb + rb) >> 1;
		x = this->samples->GetValue(m);
		if (x == pr)
		{
			b = m;
			break;
		}
		if (x<pr)
		{
			b = m;
			lb = m + 1;
		}
		else
			rb = m - 1;
	}
	if (b<0)
		return 0;
	x = this->samples->GetValue(b);
	ans = l - 1;
	if (r>(b + 1)*L - 1)
		r = (b + 1)*L - 1;
	m = b*L;

	i64 methods = this->method->GetValue(b);

	switch (methods){
		case rldelta:
		{
			i64 d = 0;
			i64 position = this->superoffset[m / SL] + this->offsects->GetValue(m / L);
			i64 p = 0;
			i64 num = 0;
			i64 bits = 0;
			i64 zero = 0;


			bool loop = false;
			while (x <= pr && m<r){
				loop = true;
				p = this->GetBits(position, 16);
				num = this->decodevaluenum[p];
				if (num == 0){
					zero = this->zerostable[p];
					bits = DecodeDelta3(position, d,zero);
					if (d % 2 == 0){
						m = m + d / 2;
						x = (x + d / 2) % n;
					}
					else{
						m++;
						x = (x + (d + 1) / 2) % n;
					}
				}
				else{
					m = m + num;
					position = position + this->decodebitsnum[p];
					x = (x + this->decoderesult[p]) % n;
				}
			}
			if (loop){
				if (num != 0){
					x = (n + x - this->decoderesult[p]) % n;
					position = position - this->decodebitsnum[p];
					m = m - num;
				}
				else{
					if (d % 2 == 0){
						m = m - d / 2;
						x = (x - d / 2 + n) % n;
					}
					else{
						m = m - 1;
						x = (n + x - (d + 1) / 2) % n;
					}
					position = position - bits;
				}
			}

			i64 run = 0;
			while (1){
				if (m >= l && x > pr)
					break;
				ans = m;
				m++;
				if (m>r)
					break;
				if (run>0){
					x = (x + 1) % n;
					run--;
				}
				else{
					DecodeDelta2(position, d);
					if (d % 2 == 0){
						run = d / 2 - 1;
						x = (x + 1) % n;
					}
					else
						x = (x + (d + 1) / 2) % n;
				}
			}
			return ans;
			break;
		}
		case gamma:{
			i64 d = 0;
			i64 position = this->superoffset[m / SL] + this->offsects->GetValue(m / L);
			while (m<l)
			{
				this->Decodegamma(position, d);
				x = (x + d) % n;
				m++;
			}

			i64 p = 0;
			i64 num = 0;
			i64 bits = 0;
			i64 zero = 0;
			bool loop = false;
			while (x <= pr && m<r)
			{
				loop = true;
				p = this->GetBits_Gamma(position, 16);
				num = this->decodevaluenum_gamma[p];
				if (num == 0)
				{
					m++;
					zero = this->zerostable_gamma[p];
					if (zero==16)
						bits = this->Decodegamma(position, d);
					else
						bits = this->Decodegamma(position, d, zero);
					x = x + d;
				}
				else
				{
					m = m + num;
					position = position + this->decodebitsnum_gamma[p];
					x = mod(x + this->decoderesult_gamma[p]);
				}

			}

			if (loop)
			{
				if (num != 0)
				{
					x = mod(x - this->decoderesult_gamma[p]);
					position = position - this->decodebitsnum_gamma[p];
					m = m - num;
				}
				else
				{
					m = m - 1;
					x = mod(x - d);
					position = position - bits;
				}
			}
			while (1)
			{
				if (x>pr)
					break;
				ans = m;
				m++;
				if (m>r)
					break;
				Decodegamma(position, d);
				x = (x + d) % n;
			}
			return ans;
		}


		case bv:{
			i64 position = this->superoffset[m / SL] + this->offsects->GetValue(m / L);
			i64 u = this->superoffset[(m / L + 1)*L / SL] + this->offsects->GetValue(m / L + 1) - position;

			if (x + u <= pr){
				ans = r;
				return ans;
			}

			if (pr == x){
				if (m >= l){
					ans = m;
					return ans;
				}
				else
					return ans;
			}

			i64 pos = position;
			u64 value = 0;
			i64 k = 0;
			i64 target = pr - x + position;
			while (pos < target ){
				value = GetBits_Gamma(pos, 32);
				k=popcnt(value);
				m += k;
				pos += 32;
			}
			if (pos > target ){
				m -= k;
				pos -= 32;
			}

			if (pos == target){
				if (m >= l){
					ans = m;
					return ans;
				}
				else{
					ans = l - 1;
					return ans;
				}
			}

			value = GetBits_Gamma(pos, target - pos);
			k = popcnt(value);
			m += k;

			if (m >= l){
				ans = m;
				return ans;
			}
			else{
				ans = l - 1;
				return ans;
			}
		}
		case allone:{
			if(x > pr)
				return ans;
			if (pr == x){
				if (m >= l){
					ans = m;
					return ans;
				}
				else
					return ans; 
			}
			int target = pr - x;
			if(m + target >= r){
				return r;
			}
			m += target;
			if (m >= l){
				ans = m;
				return ans;
			}
			else
				return ans; 
		}
		default:cout << "right boundary method erro" << endl; break;

	}
}


i64 GEN_Phi::LeftBoundary(i64 pl, i64 l, i64 r)
{
	i64 ans = 0;
	i64 SL = this->a;
	i64 L = this->b;
	i64 lb = (l + L - 1) / L;
	i64 rb = r / L;
	i64 b = rb + 1;
	i64 m = 0;
	i64 x = 0;

	while (lb <= rb)
	{
		m = (lb + rb) >> 1;
		x = this->samples->GetValue(m);
		if (x == pl)
		{
			b = m;
			break;
		}
		if (x>pl)
		{
			b = m;
			rb = m - 1;
		}
		else
			lb = m + 1;
	}
	if (b <= 0)
		return 0;
	x = this->samples->GetValue(b - 1);
	if (r>b*L - 1)
		r = b*L - 1;
	ans = r + 1;
	m = (b - 1)*L;


	i64 methods = this->method->GetValue(b - 1);

	switch (methods)
	{
		case rldelta:
		{
			i64 d = 0;
			i64 position = this->superoffset[m / SL] + this->offsects->GetValue(m / L);
			i64 p = 0;
			i64 num = 0;
			i64 bits = 0;
			i64 zero = 0;

			bool loop = false;
			while (x <= pl && m<r){
				loop = true;
				p = this->GetBits(position, 16);
				num = this->decodevaluenum[p];
				if (num == 0){
					bits = DecodeDelta2(position, d);//hzt
					if (d % 2 == 0){
						m = m + d / 2;
						x = (x + d / 2) % n;
					}
					else{
						m++;
						x = (x + (d + 1) / 2) % n;
					}
				}
				else{
					m = m + num;
					position = position + this->decodebitsnum[p];
					x = (x + this->decoderesult[p]) % n;
				}
			}
			if (loop){
				if (num != 0){
					x = (n + x - this->decoderesult[p]) % n;
					position = position - this->decodebitsnum[p];
					m = m - num;
				}
				else{
					if (d % 2 == 0){
						m = m - d / 2;
						x = (x - d / 2 + n) % n;
					}
					else{
						m = m - 1;
						x = (n + x - (d + 1) / 2) % n;
					}
					position = position - bits;
				}
			}
			i64 run = 0;
			while (1){
				if (m >= l && x > pl)
					break;
				ans = m;
				m++;
				if (m>r)
					break;
				if (run>0){
					x = (x + 1) % n;
					run--;
				}
				else{
					DecodeDelta2(position, d);
					if (d % 2 == 0){
						run = d / 2 - 1;
						x = (x + 1) % n;
					}
					else
						x = (x + (d + 1) / 2) % n;
				}
			}
			return ans;
			break;
		}
		case gamma:{
			i64 position = this->superoffset[m / SL] + this->offsects->GetValue(m / L);
			i64 d = 0;
			while (m<l)
			{
				this->Decodegamma(position, d);
				x = (x + d) % n;
				m++;
			}

			i64 p = 0;
			i64 num = 0;
			i64 bits = 0;
			bool loop = false;
			while (x<pl && m<r)
			{
				loop = true;
				p = this->GetBits_Gamma(position, 16);
				num = this->decodevaluenum_gamma[p];
				if (num != 0)
				{
					m = m + num;
					position = position + this->decodebitsnum_gamma[p];
					x = (x + this->decoderesult_gamma[p]) % n;

				}
				else
				{
					m++;
					bits = this->Decodegamma(position, d);
					x = x + d;

				}
			}
			if (loop)
			{
				if (num != 0)
				{
					x = mod(x - this->decoderesult_gamma[p]);
					position = position - this->decodebitsnum_gamma[p];
					m = m - num;
				}
				else
				{
					m = m - 1;
					x = mod(x - d);
					position = position - bits;
				}
			}

			while (1)
			{
				if (x >= pl)
				{
					ans = m;
					break;
				}
				m++;
				if (m>r)
					break;
				Decodegamma(position, d);
				x = (x + d) % n;
			}

			return ans;
		}

		case bv:{

			i64 position = this->superoffset[m / SL] + this->offsects->GetValue(m / L);
			i64 u = this->superoffset[(m / L + 1)*L / SL] + this->offsects->GetValue(m / L + 1) - position;

			if (x + u < pl)
				return ans;

			if (pl == x){
				if (m >= l){
					ans = m;
					return ans;
				}
				else{
					ans = l;
					return ans;
				}
			}

			i64 pos = position;
			u64 value = 0;
			i64 k = 0;
			i64 target = pl - x + position;
			i64 lastbit = 0;
			while (pos < target){
				value = GetBits_Gamma(pos, 32);
				k = popcnt(value);
				m += k;
				pos += 32;
			}
			if (pos > target){
				m -= k;
				pos -= 32;
			}

			if (pos == target){
				lastbit = (value & 0x0001);
				if (lastbit == 0)
					m++;
				if (m >= l){
					ans = m;
					return ans;
				}
				else{
					ans = l;
					return ans;
				}
			}

			value = GetBits_Gamma(pos, target - pos);
			k = popcnt(value);
			m += k;
			lastbit = (value & 0x0001);
			if (lastbit == 0)
				m++;
			if (m >= l){
				ans = m;
				return ans;
			}
			else{
				ans = l;
				return ans;
			}

		}
		case allone:{
			int tagert = pl - x;
			if((m + tagert) > r){
				ans = r+1;
				return ans;
			}
			if (pl == x){
				if (m >= l){
					ans = m;
					return ans;
				}
				else{
					ans = l;
					return ans;
				}
			}
			m += tagert;
			if (m >= l){
				ans = m;
				return ans;
			}
			else{
				ans = l;
				return ans;
			}

		}
		default:cout << "left boundary methods erro" << endl; break;
	}


}

i64 GEN_Phi::mod(i64 x)
{
	if (x<0)
		return x + n;
	else
		return x%n;
}

i64 GEN_Phi::sizeofEcode(int type){
	if (type==bv){
		return bvsum/8;
	}
	else if (type == gamma)
	{
		return gamasum/8;
	}else if(type == rldelta){
		return rldatasum/8;
	}
	else{
		return 0;
	}
}


integer GEN_Phi::write(savekit & s)
{
	s.writei64(n);
	s.writeinteger(a);
	s.writeinteger(b);
	s.writeinteger(tablewidth);
	s.writei64(lenofsuperoffset);
	s.writei64array(superoffset, lenofsuperoffset);

	offsects->write(s);
	samples->write(s);
	method->write(s);
	s.writei64(lenofsequence);
	s.writeu32array(sequence, lenofsequence);

	return 1;
}

integer GEN_Phi::load(loadkit &s)
{
	s.loadi64(this->n);
	s.loadinteger(this->a);
	s.loadinteger(this->b);
	s.loadinteger(tablewidth);

	this->zerostable_gamma = new u16[1 << tablewidth];
	this->decodevaluenum_gamma = new u16[1 << tablewidth];
	this->decodebitsnum_gamma = new u16[1 << tablewidth];
	this->decoderesult_gamma = new u16[1 << tablewidth];
	this->InitionalTables_Gamma();

	this->zerostable = new u16[1 << tablewidth];
	this->decodevaluenum = new u16[1 << tablewidth];
	this->decodebitsnum = new u16[1 << tablewidth];
	this->decoderesult = new u16[1 << tablewidth];
	this->InitionalTables();
	
	s.loadi64(lenofsuperoffset);
	superoffset = new i64[lenofsuperoffset];
	s.loadi64array(superoffset, lenofsuperoffset);

	offsects = new InArray();
	offsects->load(s,0);

	samples = new InArray();
	samples->load(s,0);

	method = new  InArray();
	method->load(s,0);

	s.loadi64(lenofsequence);
	sequence = new u32[lenofsequence];
	s.loadu32array(sequence, lenofsequence);
	return 1;
}





