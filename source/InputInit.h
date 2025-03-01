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

#pragma once
#include"BaseClass.h"
#include"./wheel/threadpool.hpp"
#include"BooPHF.h"
#include"InArray.h"
#include<string>
#include <sys/types.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <dirent.h>
#include <cstddef>
#include <vector>
#include <map>
#include <unordered_map>
#include <cstring>
#include <algorithm>
using namespace std;
typedef boomphf::SingleHashFunctor<uint64_t>  hasher_t;
typedef boomphf::mphf<  uint64_t, hasher_t  > boophf_t;

struct Bstring
{
    string s1; /* 顶点属性 */
    string s2; /* 文件名 */
};

struct Fstring
{
    string s1; /* 顶点1的属性 */
    string s2; /* 顶点2的属性 */
    string s3; /* 边的属性 */
    string s4; /* 文件名 */ /* 边说明表头 */ 
    u64 n;     /* 边数 */ 
};

struct Tstring
{
    string s1; /* 顶点2的属性 */
    string s2; /* 边的属性 */
    string s3; /* 文件名 */
};

struct Keyvalue
{
    u64 id;
    string s;
};

struct BigramS
{
    u64 v1;
    u64 v2;
    string s; /* 边上的信息*/
};

struct Bigram
{
    u64 v1; /* 真实顶点*/
    u64 v2; /* 映射顶点 */
    
};

struct Vdetail
{
    string head; /* 表头 */
    u64 rows; /* 有几列 */
    unordered_map<string, Bigram> hmap; /* 每一栏的内容对应哪一列和数据类型 */ 
    u64 n; /* 顶点个数 */
    u64 st; /* 起始位置 */
};

struct Especial
{
    string head; /* 说明的表头 */
    u64 rows; /* 有几列 */
    unordered_map<string, Bigram> hmap; /* 每一栏的内容对应哪一列和数据类型 */ 
    u64 hasexp; /* bool变量，表示是否有残余属性，u64为了对齐 */
    u64 n; /* 边数 */
};

struct Edetail
{
    unordered_map<string, Especial> detail;
    u64 n; /* 边到边的种类数 */
};

struct VertexMap
{
    boophf_t *bphf; /* 完美哈希 */ 
    u64 st; /* 偏移量 */
};

struct EdgeMap
{
    vector<BigramS> vec;
    MutexLock lock;
};


/* 归并排序中flag = 0 为顶点，flag = 1 为边 */
void MergeSort(vector<Keyvalue> &kv, u64 left, u64 right);
void MergeHelp(vector<Keyvalue> &kv, u64 left, u64 mid, u64 right);

void MergeSort(vector<BigramS> &bs, u64 left, u64 right);
void MergeHelp(vector<BigramS> &bs, u64 left, u64 mid, u64 right);

void Merge(vector<Keyvalue> &kv1,const vector<Keyvalue> &kv2);


enum TypeMap{int32type, int64type, stringtype, datetype, datetimetype, booltype, flout32type, flout64type};
class PreProcess
{
public:
    PreProcess();
    PreProcess(string tmpdir); // 读取LDBC目录下所有文件进行分类，对顶点标签进行排序
    void InitRun(string tmpobj = "", u64 size_for_GB = 10); // 预处理主要函数，tmpobj是处理后存放的路径，size_for_GB是文本切分的大小，
                                                            // 以GB为单位，当处理LDBC300煷LDBC1000时，顶点/边属性文本很大，不能正常
                                                            // 构建GeCSA，这个值最大不要超过50，即50GB
    void BanamaInit(string tmpdir, string tmpsuf, string tmpobj = "", u64 size_for_GB = 10); // 针对banama数据的预处理，与LDBC无关
    ~PreProcess(){};
private:
    void GetFiles(string tmpdir, vector<string> &res);
    void ToSmall(string& s); /* 将s变为小写字母 */
    void GetHMap(unordered_map<string, Bigram>& hmap, string& s, char op); /* 每一栏的内容对应哪一列和数据类型 */ 
    void MapType(); // 每一列的数据类型
    void MapTypeBanama(); // 针对banama数据的每一列的数据类型
    void CreateFiles(vector<Fstring>& tedgefiles);
    void HandleVertex(); // 处理顶点属性
    void HandleRow(vector<string *>& obj, boophf_t* bphf, string& head); // 处理person类型顶点时，需要加上mail和language
    void HandleEdge();  // 处理边属性
    void HandleVertexBanama(); // 针对banama数据的顶点属性
    void HandleEdgeBanama(); // 针对banama数据的边属性
    

    /* 保存文件部分 */
    void SaveVrow();
    void SaveErow();
    void SaveVkinds();
    void SaveEkinds();
    void SaveVmap();
    void SaveEmap();
    void SaveEtype();
    void SaveEHasdes();
    u64 blog(u64 x);

    string rootdir; /* 文件所在根目录 */
    string suffix; /* 文件后缀，默认.csv */
    string objdir; /* 输出文件所在根目录，默认等于rootdir */
    vector<Bstring> vertexfiles; 
    map<string, vector<Tstring> > edgefiles; 
    vector<Bstring> rowfiles; /* 列附加的，暂时没做考虑 */
    vector<Bstring> updatefiles; /* 行附加的，暂时没做考虑 */
    u64 cutsize;  /* 字符集分块大小 */
    u64 vnums; /* 顶点总个数 */
    u64 vlength; /* 顶点集总长度 */
    u64 enums; /* 边总个数 */
    u64 elength; /* 边集总长度 */
    u64 etypesum; /* 边种类数总和 */
    u64 max_threads;
    vector<u64> vindexrow; 
    vector<u64> eindexrow; /* 分块后每块包含的行数 */
    map<string, Vdetail> vkinds; /* 记录顶点属性的四元组 */
    map<string, Edetail> ekinds; /* 记录边属性的四元组 */
    map<string, VertexMap> vmap; /* 记录顶点映射的二元组 */
    map<string, u64> emap; /* 边类型映射表 */
    unordered_map<string, u64> t_map; /* 处理类型的临时表 */
    InArray *hasdes;
    InArray *etype;
    threadpool *pool;
};

inline u64 GetCostTime(timeval& btime,timeval& etime)
{
	return (etime.tv_sec * 1000000 + etime.tv_usec) - (btime.tv_sec * 1000000 + btime.tv_usec);
}