/*============================================
# Filename: ???
# Ver:   Date: 2023-05-XX
# Copyright (C)  Zongtao He, Hongwei Huo, and Jeffrey S. Vitter
#
This program is free software; you can redistribute it and/or modify it 
under the terms of the GNU Lesser General Public License as published 
by the Free  Software Foundation.
#
# Description: ???
=============================================*/
#include"AdjList.h"

TInArray::TInArray(u64 data_num,u32 data_width1, u32 data_width2, u32 data_width3)
{
    if(data_num < 0 || data_width1 <= 0 || data_width2 <= 0 || data_width3 <= 0)
        cerr<<"TInArray out"<<endl;
    else
    {
        this->datanum = data_num;
        this->datawidth = new u32[3];
        this->datawidth[0] = data_width1;
        this->datawidth[1] = data_width2;
        this->datawidth[2] = data_width3;
        this->totalwidth = data_width1 + data_width2 + data_width3;
        u64 totalsize = this->totalwidth * datanum;
        if(totalsize % 64 == 0)
            totalsize = totalsize / 64 + 1;
        else
            totalsize = totalsize / 64 + 2;
        this->data = new u64[totalsize];
        memset(data, 0, 8 * totalsize);
        this->mask = new u64[3];
        mask[0] = (1ull << data_width1) - 1;
        mask[1] = (1ull << data_width2) - 1;
        mask[2] = (1ull << data_width3) - 1;
    }
}

TInArray::~TInArray()
{
    if(data != NULL)
        delete [] data;
    if(datawidth != NULL)
        delete [] datawidth;
    if(mask != NULL)
        delete [] mask;
}

void TInArray::SetValueOne(u64 index, u64 v, u64 which)
{
    if(index > datanum - 1 || index < 0)
    {
        cerr<<"TInArray:index out of boundary"<<endl;
		exit(0) ;
    }
    else if((which == 1 && v > mask[0]) || (which == 2 && v > mask[1]) || (which == 3 && v > mask[2]))
    {
        cerr<<"TInArray:value is out of boundary"<<endl;
        cerr<<v<<'\t'<<mask[0]<<'\t'<<mask[1]<<'\t'<<mask[2]<<endl;
		exit(0) ;
    }
    else
    {
        u64 y = v;
        index = index * totalwidth;
        for(int i = 0;i < which - 1;++i)
            index += datawidth[i];
        u64 onesnum = datawidth[which - 1];
        u64 anchor = index >> 6;
        u64 mod = index & 0x3f;
        if(mod + onesnum < 65)
        {
            data[anchor] = (data[anchor] | (y << (64 - (mod + onesnum))));
        }
        else
        {
            int first = 64 - mod;
            int second = onesnum - first;
            data[anchor] = (data[anchor] | (y >> second));
            data [anchor + 1] = (data[anchor + 1] | (y << (64 - second)));
        }
    }
}


void TInArray::SetValueAll(u64 index, u64 v1, u64 v2, u64 v3)
{
    if(index > datanum - 1 || index < 0)
    {
        cerr<<"TInArray:index out of boundary"<<endl;
		exit(0) ;
    }
    else if(v1 > mask[0] || v2 > mask[1] || v3 > mask[2])
    {
        cerr<<"InArray:value is out of boundary"<<endl;
        cerr<<v1<<'\t'<<mask[0]<<endl;
        cerr<<v2<<'\t'<<mask[1]<<endl;
        cerr<<v3<<'\t'<<mask[2]<<endl;
		exit(0) ;
    }
    else
    {
        SetValueOne(index,v1,1);
        SetValueOne(index,v2,2);
        SetValueOne(index,v3,3);
    }
}


void TInArray::SetValueAll(u64 index, u64* v)
{
    if(index > datanum - 1 || index < 0)
    {
        cerr<<"TInArray:index out of boundary"<<endl;
		exit(0) ;
    }
    else if(v[0] > mask[0] || v[1] > mask[1] || v[2] > mask[2])
    {
        cerr<<"InArray:value is out of boundary"<<endl;
		exit(0) ;
    }
    else
    {
        SetValueOne(index,v[0],1);
        SetValueOne(index,v[1],2);
        SetValueOne(index,v[2],3);
    }
}

u64 TInArray::blog(u64 x)
{
    u64 res = 0;
    while(x > 0)
    {
        x = x >> 1;
        ++res;
    }
    return res;
}

u64 TInArray::GetNum()
{
	return datanum;
}

u64 TInArray::GetMemorySizeByte() 
{
	return (datanum * totalwidth) / 8;
}

void TInArray::GetDataWidth(u32& w1, u32& w2, u32& w3)
{
    w1 = datawidth[0];
    w2 = datawidth[1];
    w3 = datawidth[2];
}

void TInArray::GetDataWidth(u32* w)
{
    for(int i = 0;i < 3;++i)
    {
        w[i] = datawidth[i];
    }  
}

void TInArray::GetDataWidth(u32& w,u64 which)
{
    w = datawidth[which - 1];  
}

i64 TInArray::write(savekit & s)
{
    s.writeu64(datanum);
    s.writeu32array(datawidth,3);
    u64 totalsize = (datanum * totalwidth);
    if(totalsize % 64==0)
		totalsize = totalsize / 64 + 1;
	else
		totalsize = totalsize / 64 + 2;
    s.writeu64(totalsize);
    s.writeu64array(data,totalsize);
    return 1;
}

i64 TInArray::load(loadkit & s)
{
    s.loadu64(datanum);
    if(datawidth == NULL)
        datawidth = new u32[3];
    s.loadu32array(datawidth, 3);
    u64 totalsize = 0;
    s.loadu64(totalsize);
    data = new u64[totalsize];
    s.loadu64array(data, totalsize);
    totalwidth = 0;
    if(mask == NULL)
        mask = new u64[3];
    for(int i = 0; i < 3;++i)
    {
        mask[i] = (1ull << datawidth[i]) - 1;
        totalwidth += datawidth[i];
    }   
    return 1;
}

void TInArray::GetValueOne(const u64& index, u64& v, const u64& which)
{
    if(index > datanum - 1 || index < 0)
    {
		cerr<<"InArray:GetValue: index out of boundary"<<endl;
		exit(0);
	}
    else
    {
        u64 idx = index * totalwidth;
        for(int i = 0;i < which - 1;++i)
            idx += datawidth[i];
        u32 bits = datawidth[which - 1];
        u64 anchor = idx >> 6;
        u64 mod = idx & 0x3f;
        if(mod + bits < 65)
		    v = (data[anchor] << mod) >> (64 - bits);
        else
        {
            int first = 64 - mod;
            int second = bits - first;
            u64 high = (data[anchor] & ((0x01ull << first) - 1)) << second;
            v = high + (data[anchor + 1] >> (64 - second));
        }
    }
}


u64 TInArray::GetValueOne(const u64& index, const u64& which)
{
    if(index > datanum - 1 || index < 0)
    {
		cerr<<"InArray:GetValue: index out of boundary"<<endl;
		exit(0);
	}
    else
    {
        u64 idx = index * totalwidth;
        for(int i = 0;i < which - 1;++i)
            idx += datawidth[i];
        u32 bits = datawidth[which - 1];
        u64 anchor = idx >> 6;
        u64 mod = idx & 0x3f;
        if(mod + bits < 65)
		   return (data[anchor] << mod) >> (64 - bits);
        int first = 64 - mod;
        int second = bits - first;
        u64 high = (data[anchor] & ((0x01ull << first) - 1)) << second;
        return high + (data[anchor + 1] >> (64 - second));
    }
}

void TInArray::GetValueAll(const u64& index, u64& v1, u64& v2, u64& v3)
{
    if(index > datanum - 1 || index < 0)
    {
		cerr<<"InArray:GetValue: index out of boundary"<<endl;
		exit(0);
	}
    else
    {
        u64 v[3];
        u64 idx = index * totalwidth;
        for(int i = 0;i < 3;++i)
        {
            u32 bits = datawidth[i];
            u64 anchor = idx >> 6;
            u64 mod = idx & 0x3f;
            if(mod + bits < 65)
                v[i] = (data[anchor] << mod) >> (64-bits);
            else
            {
                int first = 64 - mod;
                int second = bits - first;
                u64 high = (data[anchor] & ((0x01ull << first) - 1)) << second;
                v[i] = high + (data[anchor + 1] >> (64 - second));
            }
            idx += datawidth[i];
        }
        v1 = v[0];
        v2 = v[1];
        v3 = v[2];
    }
}


void TInArray::GetValueAll(const u64& index, u64* v)
{
    if(index > datanum - 1 || index < 0)
    {
		cerr<<"InArray:GetValue: index out of boundary"<<endl;
		exit(0);
	}
    else
    {
        u64 idx = index * totalwidth;
        for(int i = 0;i < 3;++i)
        {
            u32 bits = datawidth[i];
            u64 anchor = idx >> 6;
            u64 mod = idx & 0x3f;
            if(mod + bits < 65)
                v[i] = (data[anchor] << mod) >> (64-bits);
            else
            {
                int first = 64 - mod;
                int second = bits - first;
                u64 high = (data[anchor] & ((0x01ull << first) - 1)) << second;
                v[i] = high + (data[anchor + 1] >> (64 - second));
            }
            idx += datawidth[i];
        }
    }
}

void TInArray::GetBitsSam(const u64& index, u64& ofs, u64& bits, u64& st, u64& sam, u64& sum)
{
    if(index > datanum - 1 || index < 0)
    {
		cerr<<"InArray:GetValue: index out of boundary"<<endl;
		exit(0);
	}
    else
    {
        u64 v[5];
        u64 idx = index * totalwidth;
        for(int i = 0;i < 3;++i)
        {
            u32 bits = datawidth[i];
            u64 anchor = idx >> 6;
            u64 mod = idx & 0x3f;
            if(mod + bits < 65)
                v[i] = (data[anchor] << mod) >> (64-bits);
            else
            {
                int first = 64 - mod;
                int second = bits - first;
                u64 high = (data[anchor] & ((0x01ull << first) - 1)) << second;
                v[i] = high + (data[anchor + 1] >> (64 - second));
            }
            idx += datawidth[i];
        }
        for(int i = 0;i < 2;++i)
        {
            u32 bits = datawidth[i];
            u64 anchor = idx >> 6;
            u64 mod = idx & 0x3f;
            if(mod + bits < 65)
                v[i + 3] = (data[anchor] << mod) >> (64-bits);
            else
            {
                int first = 64 - mod;
                int second = bits - first;
                u64 high = (data[anchor] & ((0x01ull << first) - 1)) << second;
                v[i + 3] = high + (data[anchor + 1] >> (64 - second));
            }
            idx += datawidth[i];
        }
        ofs = v[0];
        sum = v[4] - v[1];
        if(sum > 1)
            bits =(v[3] - v[0]) / (sum - 1);
        st = v[1];
        sam = v[2];
    }
}


void AdjList::ReadAdjTable()
{
    FILE *fp;
    fp = fopen(fn.c_str(), "rb");
    if(fp==NULL)
	{
		cout<<"Be sure that the file is available"<<endl;
		exit(0);
	}
	fseeko(fp, 0, SEEK_END); 
    adjlen = (ftello(fp) - 16) / 8;
    fseeko(fp , 0, SEEK_SET);
    adj = new u64[adjlen];
    memset(adj, 0, adjlen * 8);
    u64 res = fread(adj, 8, adjlen, fp);
    if(res != adjlen)
    {
        cerr<<"read adjlist error! "<<res<<'\t'<<adjlen<<endl;
        exit(1);
    }
    res = fread(&Vnums, 8, 1, fp);
    res = fread(&Enums, 8, 1, fp);
    fclose(fp);
    cerr<<"TmpAdjTable.myg Read is OK!!!"<<endl;
}

void AdjList::Construct() 
{
    InArray *maxb = new InArray(Vnums + 2, 6);
    outlist = new Adjdata();
    vector<u64> tcnt(Vnums, 0);//////////////////////////////////
    inmax = 0;
    outmax = 0;
    for(u64 i = 0;i < adjlen;)
    {
        u64 curv = adj[i];
        u64 vlen = adj[i + 1];
        u64 bmax = 1;
        i += 2;
        outmax = max(outmax, vlen);
        if(vlen == 0) /* 当前节点无出度 */
            maxb->SetValue(curv, 1);
        else
        {
            ++tcnt[adj[i]];
            for(u64 j = 1;j < vlen;++j)
            {
                ++tcnt[adj[j + i]];
                bmax = max(bmax, blog(adj[j + i] - adj[j + i - 1]));    
                if(adj[j + i] < adj[j + i - 1])
                {
                    cerr<<"AdjList's outlist Error!"<<endl;
                    exit(1);
                }
            }
                                
            maxb->SetValue(curv, bmax);
            outlist->datalen += ((vlen - 1) * bmax);
            i += vlen;
        }
    }
    u64 tlen;
    if(outlist->datalen % 64 == 0)
        tlen = outlist->datalen / 64 + 1;
    else
        tlen = outlist->datalen / 64 + 2;
    outlist->data = new u64[tlen];
    memset(outlist->data, 0 , 8 * tlen);
    u32 bdl = outlist->datalen == 0 ? 1 : blog(outlist->datalen);
    u32 e_v = Enums == 0 ? 1 : blog(Enums);
    u32 bv = Vnums == 0 ? 1 : blog(Vnums);
    outlist->ThreeB = new TInArray(Vnums + 1, bdl, e_v, bv);

    vector<vector<u64>* > ine(Vnums);
    for(u64 i = 0;i < Vnums;++i)
    {
        ine[i] = new vector<u64>;
        ine[i]->reserve(tcnt[i]);
    }
        

    u64 preidx = 0;
    u64 pregap = 0;
    u64 idx = 0;
    u64 curv;
    for(u64 i = 0;i < adjlen;)
    {
        curv = adj[i];
        u64 vlen = adj[i + 1];
        i += 2;
        if(vlen == 0) /* 当前节点无出度 */
            outlist->ThreeB->SetValueAll(curv, preidx, pregap, 0);
        else
        {
            ine[adj[i]]->push_back(curv);
            u64 onesnum = maxb->GetValue(curv);
            for(u64 j = 1;j < vlen;++j)
            {
                ine[adj[j + i]]->push_back(curv);
                u64 y = adj[j + i] - adj[j + i - 1];
                u64 anchor = idx >> 6;
                u64 mod = idx & 0x3f;
                if(mod + onesnum < 65)
                    outlist->data[anchor] = (outlist->data[anchor] | (y << (64 - (mod + onesnum))));
                else
                {
                    int first = 64 - mod;
                    int second = onesnum - first;
                    outlist->data[anchor] = (outlist->data[anchor] | (y >> second));
                    outlist->data[anchor + 1] = (outlist->data[anchor + 1] | (y << (64 - second)));
                }
                idx += onesnum;
            } 
            outlist->ThreeB->SetValueAll(curv, preidx, pregap, adj[i]);
            pregap += vlen;
            preidx += ((vlen - 1) * onesnum);
            i += vlen;
        }
    }
    outlist->ThreeB->SetValueAll(++curv, preidx, pregap, 0);
    
    if(adj != NULL)
    {
        delete [] adj;
        adj = NULL;
    }

    /* 入度 */
    
    maxb->ReSet();
    inlist = new Adjdata();
    for(u64 i = 0;i < Vnums;++i)
    {
        u64 len = ine[i]->size();
        u64 bmax = 1;
        inmax = max(inmax, len);
        if(len == 0)
            maxb->SetValue(i, 1);
        else
        {
            for(u64 j = 1;j < len;++j)
            {
                bmax = max(bmax, blog((*ine[i])[j] - (*ine[i])[j - 1]));
                if((*ine[i])[j] < (*ine[i])[j - 1])
                {
                    cerr<<"AdjList's inlist Error!"<<endl;
                    exit(1);
                }
            }

            maxb->SetValue(i, bmax);
            inlist->datalen += ((len - 1) * bmax);
        }
    }
    if(inlist->datalen % 64 == 0)
        tlen = inlist->datalen / 64 + 1;
    else
        tlen = inlist->datalen / 64 + 2;
    inlist->data = new u64[tlen];
    memset(inlist->data, 0 , 8 * tlen);
    bdl = inlist->datalen == 0 ? 1 : blog(inlist->datalen);
    inlist->ThreeB = new TInArray(Vnums + 1, bdl, e_v, bv);

    preidx = 0;
    pregap = 0;
    idx = 0;
    for(u64 i = 0;i < Vnums;++i)
    {
        u64 len = ine[i]->size();
        if(len == 0)
            inlist->ThreeB->SetValueAll(i, preidx, pregap, 0);
        else
        {
            u64 onesnum = maxb->GetValue(i);
            for(u64 j = 1;j < len;++j)
            {
                u64 y = (*ine[i])[j] - (*ine[i])[j - 1];
                u64 anchor = idx >> 6;
                u64 mod = idx & 0x3f;
                if(mod + onesnum < 65)
                    inlist->data[anchor] = (inlist->data[anchor] | (y << (64 - (mod + onesnum))));
                else
                {
                    int first = 64 - mod;
                    int second = onesnum - first;
                    inlist->data[anchor] = (inlist->data[anchor] | (y >> second));
                    inlist->data[anchor + 1] = (inlist->data[anchor + 1] | (y << (64 - second)));
                }
                idx += onesnum;
            } 
            inlist->ThreeB->SetValueAll(i, preidx, pregap, (*ine[i])[0]);
            pregap += len;
            preidx += ((len - 1) * onesnum);
        }
    }
    inlist->ThreeB->SetValueAll(Vnums, preidx, pregap, 0);

    delete maxb;
}


void AdjList::GetNextVid(const u64& id, u64 *&res, u64& nums, u64& st)
{
    if(id >= Vnums)
    {
        cerr<<"Max V is:"<<'\t'<<Vnums<<'\t'<<id<<endl;
        exit(1);
    }
    u64 ofs, bits, sam;
    outlist->ThreeB->GetBitsSam(id, ofs, bits, st, sam, nums);
    if(nums == 0)
        return ;
    if(res != NULL) 
        delete [] res;
    res = new u64[nums];
    res[0] = sam;
    for(u64 i = 1;i < nums;++i)
    {
        u64 anchor = ofs >> 6;
        u64 mod = ofs & 0x3f;
        u64 v;
        if(mod + bits < 65)
		   v = (outlist->data[anchor] << mod) >> (64 - bits);
        else
        {
            int first = 64 - mod;
            int second = bits - first;
            u64 high = (outlist->data[anchor] & ((0x01ull << first) - 1)) << second;
            v = high + (outlist->data[anchor + 1] >> (64 - second));
        }
        res[i] = res[i - 1] + v;
        ofs += bits;
    }
}

void AdjList::GetPrevVid(const u64& id, u64 *&res, u64& nums, u64& st)
{
    if(id >= Vnums)
    {
        cerr<<"Max V is:"<<'\t'<<Vnums<<'\t'<<id<<endl;
        exit(1);
    }
    u64 ofs, bits, sam;
    inlist->ThreeB->GetBitsSam(id, ofs, bits, st, sam, nums);
    if(nums == 0)
        return ;
    if(res != NULL) 
        delete [] res;
    res = new u64[nums];
    res[0] = sam;
    for(u64 i = 1;i < nums;++i)
    {
        u64 anchor = ofs >> 6;
        u64 mod = ofs & 0x3f;
        u64 v;
        if(mod + bits < 65)
		   v = (inlist->data[anchor] << mod) >> (64 - bits);
        else
        {
            int first = 64 - mod;
            int second = bits - first;
            u64 high = (inlist->data[anchor] & ((0x01ull << first) - 1)) << second;
            v = high + (inlist->data[anchor + 1] >> (64 - second));
        }
        res[i] = res[i - 1] + v;
        ofs += bits;
    }
}

u64 AdjList::GetOutNums(const u64& id)
{
    if(id >= Vnums)
    {
        cerr<<"Max V is:"<<'\t'<<Vnums<<'\t'<<id<<endl;
        exit(1);
    }
    u64 ofs, bits, sam, st, nums;
    outlist->ThreeB->GetBitsSam(id, ofs, bits, st, sam, nums);
    return nums;
}

u64 AdjList::GetInNums(const u64& id)
{
    if(id >= Vnums)
    {
        cerr<<"Max V is:"<<'\t'<<Vnums<<'\t'<<id<<endl;
        exit(1);
    }
    u64 ofs, bits, sam, st, nums;
    inlist->ThreeB->GetBitsSam(id, ofs, bits, st, sam, nums);
    return nums;
}

u64 AdjList::blog(u64 x)
{
    u64 res = 0;
    while(x > 0)
    {
        x = x >> 1;
        ++res;
    }
    return res;
}

u64 AdjList::GetVnum()
{
    return Vnums;
}

u64 AdjList::GetEnum()
{
    return Enums;
}

u64 AdjList::GetOutMax()
{
    return outmax;
}

u64 AdjList::GetInMax()
{
    return inmax;
}

u64 AdjList::GetOutSizeByte()
{
    u64 res = outlist->datalen / 8 + outlist->ThreeB->GetMemorySizeByte();
    return res;
}
u64 AdjList::GetInSizeByte()
{
    u64 res = inlist->datalen / 8 + inlist->ThreeB->GetMemorySizeByte();
    return res;
}

u64 AdjList::GetMemorySizeByte()
{
    u64 res = outlist->datalen / 8 + outlist->ThreeB->GetMemorySizeByte()
                + inlist->datalen / 8 + inlist->ThreeB->GetMemorySizeByte();
    return res;
}

double AdjList::GetOutSizeRadio()
{
    return GetOutSizeByte() * 1.0 / GetMemorySizeByte() * 100;
}

double AdjList::GetInSizeRadio()
{
    return GetInSizeByte() * 1.0 / GetMemorySizeByte() * 100;
}

double AdjList::GetMemorySizeRadio()
{
    u64 srcSize = (Vnums * blog(Vnums) + (Vnums + Enums) *blog(Enums)) / 8;
    return GetMemorySizeByte() * 1.0 / srcSize * 100;
}

u64 AdjList::GetSSizeByte()
{
    return outlist->datalen / 8 + inlist->datalen / 8;
}

u64 AdjList::GetXSizeByte()
{
    u32 xBitsOut, xBitsIn;
    outlist->ThreeB->GetDataWidth(xBitsOut, 1);
    inlist->ThreeB->GetDataWidth(xBitsIn, 1);
    return outlist->ThreeB->GetNum() * xBitsOut / 8 + inlist->ThreeB->GetNum() * xBitsIn / 8;
}

u64 AdjList::GetNgapSizeByte()
{
    u32 nGapBits;
    outlist->ThreeB->GetDataWidth(nGapBits, 2);
    return outlist->ThreeB->GetNum() * nGapBits / 8 * 2;//出入度nGapBits相同
}


u64 AdjList::GetSamSizeByte()
{
    u32 samBits;
    outlist->ThreeB->GetDataWidth(samBits, 3);
    return outlist->ThreeB->GetNum() * samBits / 8 * 2;//出入度samBits相同
}

double AdjList::GetSSizeRadio()
{
    return GetSSizeByte() * 1.0 / GetMemorySizeByte() * 100;
}

double AdjList::GetXSizeRadio()
{
    return GetXSizeByte() * 1.0 / GetMemorySizeByte() * 100;
}

double AdjList::GetNgapSizeRadio()
{
    return GetNgapSizeByte() * 1.0 / GetMemorySizeByte() * 100;
}

double AdjList::GetSamSizeRadio()
{
    return GetSamSizeByte() * 1.0 / GetMemorySizeByte() * 100;
}

i64 AdjList::Write(savekit & s)
{
    s.writeu64(Vnums);
    s.writeu64(Enums);

    s.writeu64(outlist->datalen);
    u64 len = outlist->datalen;
    if(len % 64 == 0)
        len = len / 64 + 1;
    else
        len = len / 64 + 2;
    if(outlist->data != NULL)
        s.writeu64array(outlist->data, len);
    if(outlist->ThreeB != NULL)
        outlist->ThreeB->write(s);

    s.writeu64(inlist->datalen);
    len = inlist->datalen;
    if(len % 64 == 0)
        len = len / 64 + 1;
    else
        len = len / 64 + 2;
    if(inlist->data != NULL)
        s.writeu64array(inlist->data, len);
    if(inlist->ThreeB != NULL)
        inlist->ThreeB->write(s);
    return 1;
}


i64 AdjList::Load(loadkit & s)
{
    s.loadu64(Vnums);
    s.loadu64(Enums);

    outlist = new Adjdata();
    s.loadu64(outlist->datalen);
    u64 len = outlist->datalen;
    if(len % 64 == 0)
        len = len / 64 + 1;
    else
        len = len / 64 + 2;
    outlist->data = new u64[len];
    s.loadu64array(outlist->data, len);
    outlist->ThreeB = new TInArray();
    outlist->ThreeB->load(s);

    inlist = new Adjdata();
    s.loadu64(inlist->datalen);
    len = inlist->datalen;
    if(len % 64 == 0)
        len = len / 64 + 1;
    else
        len = len / 64 + 2;
    inlist->data = new u64[len];
    s.loadu64array(inlist->data, len);
    inlist->ThreeB = new TInArray();
    inlist->ThreeB->load(s);
    return 1;
}


AdjList::~AdjList()
{
    // adj 在construct时已delete
    if(outlist != NULL)
        delete outlist; 
    if(inlist != NULL)
        delete inlist; 
}