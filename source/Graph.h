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

#include<iostream>
#include<unordered_map>
#include<unordered_set>
#include<queue>
#include<unistd.h>
#include<sys/time.h>
#include<random>
#include"CSA.h"
#include"AdjList.h"
#include"InputInit.h"
#include"wheel/threadpool.hpp"

#define MAX_USERS 99 
#define KNOWS_THRESHOLD (1ull << 40) /* 在FindFriendsFun中大于此阈值使用多线程，小于使用单线程 */
#define ARRAY_REDUNDANT 200 /* 在FindFriendsFun中大于此阈值使用多线程，小于使用单线程 */
#define MAX_QUEUE_SIZE (1 << 28) // 默认1 << 28，查询队列的最大长度
// #define MAX_QUEUE_SIZE (1 << 23)
#define IC_5_PERSON_MAX 300
#define BI_3_PERSON_MAX 300
#define IC_5_THREADS_MAX 20
#define PAGERANK_NUMS_ONETHREAD 2000
#define BFS_NUMS_ONETHREAD 2000
#define GLOBAL_VEC_SIZE (1 << 24) // 默认1 << 24
#define GLOBAL_TVEC_SIZE (1 << 23) // 默认1 << 23

class ResQueue;

struct Distance
{
    u64 id;
    string vtype;
    string einfo;
};

struct MultiGNode
{
    u64 n;
    u64 idx;
    u64 kinds; /* 类似关系型数据库中，该链表join的是什么，可以根据查询语句映射成数字 */
    MutexLock *multilock;
    struct GNode** vec;
    MultiGNode():n(0), idx(0), vec(NULL){}
};

struct CondData // 条件查询类，比如查找顶点/边某一列大于/等于/小于给定数值等
{
    // 顶点
    Bigram vdk; // 哪一列；列的数据类型
    u64 vop; // 操作符 : 0 等于 -1 小于等于 -2 小于 1 大于等于 2大于 3 不等于
    u64 vneed; // 是否需要ext出来
    string vobj; // 要求是什么

    // 边
    Bigram edk;
    string etype; // 边的关系属性
    u64 eop; // 操作符 : 0 等于 -1 小于等于 -2 小于 1 大于等于 2大于 3 不等于
    u64 eneed; // 是否需要ext出来
    u64 edsp; // 是否需要边残余属性
    string eobj; // 要求是什么
    
    CondData():vobj(""), etype(""), eobj(""){}
};


struct GNode // 每一个顶点的实体类
{
    u64 vid;  /* 顶点号 */
    u64 eid;  /* 得到此顶点涉及到的边的边号 */
    u64 level;  /* 层次/距离 */  
    string vinfo; // 顶点属性
    string vtype; // 顶点标签
    string einfo; // 边属性
    GNode* prev; // 指向前一个顶点，就是此顶点由哪一个顶点引出来的就指向谁
    MultiGNode* multi; /* 指向出度的指针数组，有时会用到 */
    u64 multin; /* 类似关系型数据库中join不同类型的数量，配合multi使用 */
    u64 valid; /* 用于各种标记 */
    u64 previdx; /* 在前驱的第几个后继数组中 */
    u64 effe; /* 有效位 */
    u64 row; /* 在结果集中的第几行 */
    u64 oldid; // 顶点属性中的id

    CondData *cond; // 该顶点需要的条件是什么

    GNode():vinfo(""), vtype(""), einfo(""), prev(NULL), multi(NULL), multin(0), cond(NULL){}
};

struct NorQue // 查询队列，存放查询涉及到的所有顶点
{
    u64 hidx;  /* 队头下标 */
    u64 tidx;  /* 队尾下标 */
    GNode* vec;  /* 队列本体 */
};

struct PtrQue // 查询队列，存放指向NorQue队列中某些数据元素的指针，与NorQue配合使用
{
    u64 hidx;  /* 队头下标 */
    u64 tidx;  /* 队尾下标 */
    GNode** ptrv;  /* 队列本体 */
};

struct GlobalVec // 辅助数组，有时辅助查询
{
    u64 *vec;
    u64 **tVec;
    GlobalVec():vec(NULL), tVec(NULL){}
};

/* 用户查询队列结构体，每个用户分配一个，方便以后拓展多用户查询 */
struct UserData
{
    NorQue *SchQ;  
    PtrQue *ResQ;  
    GlobalVec *gVec;
    
    MutexLock *tasklock;  /* 完成任务数目的锁 */
    MutexLock *reslock;  /* ResQ的锁 */

    InArray *vis;  /* 访问数组 */
};

struct BISch_16_Vec;

class Graph
{
    /*
        此类是整个图数据库类，以后整合的时候在此类添加数据成员和成员函数
    */
    friend class GraphRe;
public:
    /*
        friend函数都是多线程的工作函数
    */
    friend void BISch_3_Fun(void* x);
    friend void FriendFind_Fun(void* x);
    friend void BISch_10_Fun_1(void* x);
    friend void Ext_Tag_Fun(void* x);
    friend void BISch_10_Fun_2(void* x);
    friend void BISch_16_Fun(void* x);
    friend void BISch_16_Fun_2(void* x);
    friend void Get_Id_Fun(void* x);

    AdjList &GetAdjList() { return *Adj; }
    
    Graph();
    void LoadFiles(string dir); /* 装载所有文件 */

    void Run(); /* 死循环，使CGraphIndex一直运行 */

    void TestRun(const std::string &query_name, const string &input_filename, const string &output_filename, size_t max_num, 
                 const std::string &tmp_dir, bool print=false); // 用于批量测试
    void TestExtVertex(u64 times, u64 len, string fn); // 测试extract顶点属性
    void TestExtEdge(u64 times, u64 len, string fn); // 测试extract边属性

    /* 都没有提取换行符，输出时需要自己添加 */
    void GetVInfo(string& res, u64& id); // 根据顶点id得到顶点属性
    u64 GetVLoc(const string& type, const string& col, const string& value); // loacte，暂不使用

    void GetEInfo(string& res, u64& id); // 根据边id得到边属性
    void GetEType(string& res, u64& id); // 根据边id得到边关系属性
    void GetEDesp(string& res, u64& id); // 根据边id得到边残余属性

    bool DataCmp(const string& src, string& cond, Bigram& row, u64& op);  // 数值比大小，比如是否大于/小于/相等
    void GetVKindsById(string& vkinds, u64& vid); // 根据id得到顶点标签
    ~Graph();

    u64 GetRealId(const std::string &type, u64 id);

private:
    void handleSerch(); 

    void TestSearch(int mod, string obj); // 用于批量测试

    void GetFiles(vector<string>& vfn, vector<string>& efn, string& vmapfn,
                    string& vkindsfn, string& ekindsfn, string& vrowsfn, 
                    string& erowsfn, string& adjfn, string& hasdesfn,
                    string& emapfn, string& etypefn); // 方便加载文件        
    
    /* 进行查询的关键函数 */
    void GetRealId(const string& type, const u64& id, u64& realid); // 根据顶点标签和顶点原始id得到我们的realid
    void GetRealId(const string& type, const string& value, const string& row, u64& realid); // 根据顶点属性中某一列的内容得到我们的realid
    vector<string> GetParams(string& obj); // 用于处理复杂的输入参数
    /* FindFriendsFun没有处理GNode中的prev，valid，previdx信息，调用PathTracing前需要再处理 */
    void FindFriendsFun(u64 startPersonId, CondData* condArg, u64 minDistance, u64 maxDistance, u64& minDistanceSt); // 找到minDistance-maxDistance跳好友
    void TypeExt(const string& type, const string& value, const string& col, u64& realId); // 用于测试
    void TypeLoc(const string& type, const string& value, const string& col, u64& realId); // 用于测试

    void NewSearch();
    void NormalSearch(); /* HopK */

    /*
        多线程查询的接口函数。threads_work是通用的，将SchQ队列中shidx-stidx的顶点的相关信息(顶点属性/边属性)按照要求提取出来，并且将符合条件的放到ResQ中
        其他函数都是针对特定查询实现的
    */
    void threads_work(NorQue* SchQ, PtrQue* ResQ, volatile u64& shidx, volatile u64& stidx, int threads, MutexLock* tasklock, MutexLock* reslock);
    void threads_work_BI_3(NorQue* SchQ, volatile u64& shidx, volatile u64& stidx, int threads, MutexLock* tasklock, MutexLock* reslock, InArray* vis, GlobalVec* gVec);
    void threads_work_FindFriends(NorQue* SchQ, volatile u64& shidx, volatile u64& stidx, int threads, MutexLock* tasklock, MutexLock* reslock, CondData* condArg, InArray* vis);
    void threads_work_BI_10_1(NorQue* SchQ, PtrQue* ResQ, volatile u64& shidx, volatile u64& stidx, int threads, MutexLock* tasklock, MutexLock* reslock, InArray* vis);
    void threads_ext_tag(unordered_map<u64, string>& tagMap, int threads, MutexLock* tasklock);
    void threads_work_BI_10_2(PtrQue* ResQ, int threads, MutexLock* tasklock, InArray* vis, vector<unordered_map<u64, u64> >& ptrPerson);
    void threads_work_BI_16(int threads, MutexLock* tasklock, InArray* vis, vector<BISch_16_Vec>& personVec, u64 personVecSize, u64 maxKnowsLimit);
    void threads_work_BI_16_2(int threads, MutexLock* tasklock, InArray* visA, InArray* visB, vector<BISch_16_Vec>& personVec, u64 personVecSize, u64 maxKnowsLimit);
    void threads_work_getRealId(const string& type, const string& value, const string& row, u64& realid);

    void DisplayDistance(GNode* x, string& res);  /* 打印路径 */
    void PathTracing(PtrQue *res); // 用于追溯路径的内存分配
    void MultiFree(PtrQue *res); // 用于追溯路径的内存释放
    void GNodeClear(GNode* vec, u64 size);
    void GetRowInfo(string& res, const string& head, const string& src, const string& type); /* 返回顶点属性中对应列的信息 */
    string GetRowInfo(const string& head, const string& src, const string& type); /* 返回顶点属性中对应列的信息 */
    /* 打印输入格式信息 */
    void PrintHelp();

    /* 装载辅助文件 */
    void LoadVmap(string& fn);
    void LoadVkinds(string& fn);
    void LoadVrows(string& fn);
    void LoadEkinds(string& fn);
    void LoadErows(string& fn);
    void LoadEHasdes(string& fn);
    void LoadEmap(string& fn);
    void LoadEtype(string& fn);

    /* 装载索引文件 */
    void LoadVindex(vector<string>& vfn);
    void LoadEindex(vector<string>& efn);

    /* 装载邻接表 */
    void LoadAdjList(string& adjfn);

    /* 查询在哪个索引文件中，目前是线性遍历 */
    void FindVertexFile(u64& realid, u64 &idx, u64& rows);
    void FindEdgeFile(u64& realid, u64 &idx, u64& rows);

    /* 主要测试函数 */
    void FindInOutNums();
    void Search_IC_0(string obj);
    void Search_IC_1(string obj);
    void Search_IC_2(string obj);
    void Search_IC_5(string obj);
    void Search_IC_9(string obj);
    void Search_IC_13(string obj);
    void Search_BI_3(string obj);
    void Search_BI_10(string obj);
    void Search_BI_16(string obj);
    void Search_BI_16_2(string obj);
    void KHops(string obj);
    void KHopsFriends(string obj);
    void NormalSearch_threads(string obj); /* HopK多线程 */
    void NormalSearch_InOut_threads(string obj);

    void TypeExtTest(string obj);
    void TypeLocTest(string obj);

    CSA** Vertex_csa;   
    u64 Vcsa_cnt;
    CSA** Edge_csa;
    u64 Ecsa_cnt;
    AdjList *Adj;
    vector<Keyvalue> VidMapKinds;
    
    unordered_map<string, Vdetail> vkinds;
    unordered_map<string, Edetail> ekinds;
    unordered_map<string, VertexMap> vmap;
    unordered_map<string, u64> emap;
    unordered_map<u64, string> iemap;
    InArray *hasdes;
    InArray *etype;
    
    vector<u64> Vertex_rows; /* 每个索引的起始id */
    vector<u64> Edge_rows;
    string rootdir;

    threadpool *pool;  /* 所有用户公用线程池 */
    UserData *Que[MAX_USERS];  /* 用户独享访问队列 */

    u64 total_v;
    u64 total_e;
    i64 max_threads;
};


/* 所有的inline函数 */
inline void Graph::FindVertexFile(u64& realid, u64 &idx, u64& rows) // 根据id寻找顶点属性在哪个索引文件中，并且返回对应的行号
{
    idx = 0;
    for(;idx < Vcsa_cnt;++idx)
    {
        if(Vertex_rows[idx + 1] > realid)
            break;
    }  
    rows = realid - Vertex_rows[idx];
}

inline void Graph::FindEdgeFile(u64& realid, u64 &idx, u64& rows) // 根据id寻找边属性在哪个索引文件中，并且返回对应的行号
{
    idx = 0;
    for(;idx < Ecsa_cnt;++idx)
    {
        if(Edge_rows[idx + 1] > realid)
            break;
    }  
    rows = realid - Edge_rows[idx];
}


inline void Graph::GetRealId(const string& type, const u64& id, u64& realid)
{
    if(vmap.count(type) == 0)
    {
        cerr<<"Type error!!!"<<endl;
        exit(1);
    }
    realid = vmap[type].st + vmap[type].bphf->lookup(id);
}

inline void Graph::GetRealId(const string& type, const string& value, const string& row, u64& realid)
{
    if(vkinds[type].n < 10000){
        string tmpValue;
        for(u64 i = vmap[type].st;i < vmap[type].st + vmap[type].bphf->nbKeys();++i){
            string tmpRes;
            GetVInfo(tmpValue, i);
            GetRowInfo(tmpRes, row, tmpValue, type);
            if(tmpRes == value){
                realid = i;
                break;
            }
        }
    }
    else{
        threads_work_getRealId(type, value, row, realid);
    }
}

inline void Graph::GetVKindsById(string& vkinds, u64& vid)
{
    u64 i = 0;
    while(VidMapKinds[i].id <= vid)++i;
    vkinds = VidMapKinds[i - 1].s;
}

inline u64 Graph::GetRealId(const std::string &type, u64 id) {
    u64 realId;
    GetRealId(type, id, realId);
    return realId;
}

inline vector<string> Graph::GetParams(string& obj)
{
    string tmp = "";
    vector<string> res;
    for(u64 i = 0;i < obj.size();++i){
        if(obj[i] == ' '){
            res.push_back(tmp);
            tmp = "";
        }
        else
            tmp += obj[i]; 
    }
    res.push_back(tmp);
    return res;
}
