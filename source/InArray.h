#ifndef _Inarray
#define _Inarray
#include"BaseClass.h"
#include"savekit.h"
#include"loadkit.h"
class InArray
{
public:
	InArray();
	InArray(u64 data_num,u32 data_width);
	~InArray(void);

	void SetFlag()
	{
		rankflag = 2;
	}

	u64 GetValue(u64 index){
		if(index>datanum-1||index<0){
			cerr<<"InArray:GetValue: index out of boundary"<<endl;
			exit(0);
		}
		u64 anchor=(index*datawidth)>>5;
		u64 temp1=data[anchor];
		u32 temp2=data[anchor+1];
		u32 temp3 = data[anchor + 2];
		i32 overloop = ((anchor + 2) << 5) - (index + 1)*datawidth;
		temp1=(temp1<<32)+temp2;
		if (overloop < 0){
			temp1 = (temp1 << (-overloop)) + (temp3 >> (32 + overloop));
			return temp1&mask;
		}
		else
			return (temp1>>overloop)&mask;
	}
	void SetValue(u64 index, u64 value);
	void SetValue64(u64 index, u64 value);
	void SetBitZero(u64 index);
	void constructrank(u64 data_num, u64 *positions, u64 num, int b_length, int sb_length);
	void constructrank(int Blength,int SBlength);
	u64 GetNum();
	u32 GetDataWidth();
	u64 GetMemorySize();
	u64 GetValue2(u64 index);
    u64 GetrankMemorySize();
	integer getD(u64 i){
		if(i<0||i>=datanum)
		{
			cout<<"GetD: Index out of boundary"<<i<<"  "<<datanum<<endl;
			exit(0);
		}
		u64 Mdnum=i>>Blength;
		if(Md->GetValue(Mdnum)==0)
			return 0;
		else
		{
			u64 SBnum=Mdnum>>SBlength;
			u64 Sbdbefore=SBd->GetValue(SBnum);
			u64 Sdnowi=Sbdbefore+Bd->GetValue(Mdnum);
			u64 Sdnow=Sd->GetValue(Sdnowi);
			if(((Mdnum<<Blength)+Sdnow)>i)return 0;
			else if(((Mdnum<<Blength)+Sdnow)==i)return 1;
			else
			{
				u64 temnum1=0;
				if(Mdnum!=Md->GetNum()-1)
				{
					
					u64 nextBd=Bd->GetValue(Mdnum+1);
					if(nextBd==0)temnum1=SBd->GetValue(SBnum+1)-Sbdbefore-Bd->GetValue(Mdnum);
					else
						temnum1=nextBd-Bd->GetValue(Mdnum);
				}else
				{
					temnum1=Sd->GetNum()-Sdnowi;
				}
				for(u64 j=1;j<temnum1;j++)
				{
					Sdnow=Sd->GetValue(Sdnowi+j);
					if(((Mdnum<<Blength)+Sdnow)>i)return 0;
					else if(((Mdnum<<Blength)+Sdnow)==i)return 1;
				}
				return 0;
			}
			
		}
	}
	u64 rank(u64 i){
		u64 Mdnum=i>>Blength;
		u64 SBnum=Mdnum>>SBlength;
		u64 Sbdbefore=SBd->GetValue(SBnum);
		u64 Sdnowi=Sbdbefore+Bd->GetValue(Mdnum);
		while(1)
		{
			u64 Sdnow=Sd->GetValue(Sdnowi);
			if(((Mdnum<<Blength)+Sdnow)==i)break;
			Sdnowi++;
		}
		return Sdnowi+1;
	}  

	u64 rank2(u64 i){
		u64 Mdnum=i>>Blength;
		u64 SBnum=Mdnum>>SBlength;
		u64 Mdnum2 = Mdnum + 1;
		u64 SBnum2 = Mdnum2 >> SBlength;
		u64 Sbdbefore=SBd->GetValue(SBnum);
		u64 Sdnowi=Sbdbefore+Bd->GetValue(Mdnum);
		u64 Sdnowi2 = SBd->GetValue(SBnum2) + Bd->GetValue(Mdnum2);
		if(Sdnowi == Sdnowi2)
			return Sdnowi;
		u64 nums = Sdnowi2 - Sdnowi;
		for(u64 j = 0;j < nums;++j){
			u64 Sdnow = Sd->GetValue(Sdnowi);
			if(((Mdnum << Blength) + Sdnow) == i)
				return Sdnowi + 1;
			else if(((Mdnum << Blength) + Sdnow) > i)
				return Sdnowi;
			else 
				++Sdnowi;
		}
		return Sdnowi;
	} 

    u64 select(u64 i);
	u64 select2(u64 i);
	u64 blockbinarySearch(InArray * B,u64 k,u64 l ,u64 h);
	void ReSet();

	i64 write(savekit & s);
	i64 load(loadkit & s,int flag);
private:
	u32 * data;
	u64 datanum;
	u32 datawidth;
	u64 mask;
    int rankflag;
    InArray* Md;
	InArray* Sd;
	InArray* SBd;
	InArray* Bd;
	int Blength;
	int SBlength;
	i64 block_len;
	i64 super_block_len;
};
#endif
