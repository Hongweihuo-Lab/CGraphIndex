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
#pragma once
#include"InArray.h"
#include<stdlib.h>
#include<string.h>
#include<iostream>
#include<random>
#include<unistd.h>
using namespace std;

class TInArray // 与InArray相同，输入下标idx可以得到三个数据元素
{
public:
	TInArray():data(NULL), datawidth(NULL), mask(NULL){};
	TInArray(u64 data_num,u32 data_width1, u32 data_width2, u32 data_width3);
    /*只是在未设置值的index上设置值，
    无法更改/覆盖已经有的数值*/
    void SetValueAll(u64 index, u64 v1, u64 v2, u64 v3);
    void SetValueAll(u64 index, u64* v);
    void SetValueOne(u64 index, u64 v, u64 which);

    /*ofs、bits、st、sam和sum分别返回偏移量、当前块定长编码长度、
    边在边集的偏移量（中间值）、块采样值和解码个数*/
    void GetBitsSam(const u64& index, u64& ofs, u64& bits, u64& st, u64& sam, u64& sum);

    void GetValueAll(const u64& index, u64& v1, u64& v2, u64& v3);
    void GetValueAll(const u64& index, u64* v);
    void GetValueOne(const u64& index, u64& v, const u64& which);
    u64 GetValueOne(const u64& index, const u64& which);



    u64 GetNum();
    void GetDataWidth(u32& w1, u32& w2, u32& w3);
    void GetDataWidth(u32* w);
    void GetDataWidth(u32& w, u64 which);
    u64 GetMemorySizeByte();

    i64 write(savekit & s);
	i64 load(loadkit & s);
	~TInArray();
private:
    u64 blog(u64 x);

	u64 *data;
	u64 datanum;
	u32 *datawidth, totalwidth;
	u64 *mask;
};



struct Adjdata // 用于封装，gStruct和gInStruct的数据结构分别对应一个Adjdata对象
{
    Adjdata():ThreeB(NULL), data(NULL), datalen(0){};
    ~Adjdata()
    {
        if(ThreeB)
            delete ThreeB;
        if(data)
            delete [] data;
    }
    TInArray *ThreeB;
    u64* data;
    u64 datalen; /* bit */
};


class AdjList // 压缩邻接表类
{
public:
    AdjList():outlist(NULL), inlist(NULL), adj(NULL){};
    AdjList(string s):outlist(NULL), inlist(NULL), adj(NULL), fn(s){};
    void ReadAdjTable(); // 读取临时邻接表，临时邻接表在Init中生成
    /* 正常的输入应该是一个二维数组，行数等于
     边的数量，列数等于2*/
    void Construct();
    // void Construct(int _overloop);
    void GetNextVid(const u64& id, u64 *&res, u64& nums, u64& st);// 得到顶点id的所有外邻邻接顶点，存放在res中，数量存放在nums中
    void GetPrevVid(const u64& id, u64 *&res, u64& nums, u64& st);// 得到顶点id的所有入邻邻接顶点，存放在res中，数量存放在nums中
    u64 GetOutNums(const u64& id); // 仅得到顶点id的外邻邻接顶点数量
    u64 GetInNums(const u64& id); // 仅得到顶点id的入邻邻接顶点数量

    u64 GetVnum(); // 顶点数量
    u64 GetEnum(); // 边数量
    u64 GetOutMax(); // 最大出度
    u64 GetInMax(); // 最大入度
    u64 GetOutSizeByte(); // gStruct大小
    u64 GetInSizeByte(); // gInStruct大小
    u64 GetMemorySizeByte();
    u64 GetSSizeByte();
    u64 GetXSizeByte();
    u64 GetNgapSizeByte();
    u64 GetSamSizeByte();

    double GetOutSizeRadio();
    double GetInSizeRadio();
    double GetMemorySizeRadio();
    double GetSSizeRadio();
    double GetXSizeRadio();
    double GetNgapSizeRadio();
    double GetSamSizeRadio();
    i64 Write(savekit & s); // 保存到磁盘
	i64 Load(loadkit & s); // 从磁盘读取
    ~AdjList();
private:
    string fn;
    u64 blog(u64 x);
    Adjdata *outlist;
    Adjdata *inlist;
    u64* adj;
    u64 adjlen;
    u64 Vnums; /*顶点数*/
    u64 Enums; /*边数*/
    u64 outmax;
    u64 inmax;
};