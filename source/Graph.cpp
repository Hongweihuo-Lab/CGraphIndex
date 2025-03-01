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

#include"Graph.h"

#include <chrono>
#include <iomanip>

#include "oneapi/tbb/global_control.h"
#include "par/par.hpp"

#include "benchmark/bfs_benchmark.hpp"
#include "benchmark/page_rank_benchmark.hpp"
#include "benchmark/bi2_benchmark.hpp"
#include "benchmark/bi12_benchmark.hpp"
#include "benchmark/bi19_benchmark.hpp"
#include "benchmark/ic2_benchmark.hpp"
#include "benchmark/ic9_benchmark.hpp"
#include "benchmark/ic10_benchmark.hpp"
#include "benchmark/bi3_benchmark.hpp"

/* 工作函数部分开始 */
/* 查询时我们可以根据广度优先的方法，分析语句，构建一个简单的查询图
   每个节点包含标号和该节点对边和顶点的要求；查询时就按照查询图进行
   查询；当判断条件在下N跳中，如果不符合条件，则通过prev指针将前面结
   点的有效位置0 */
struct NorSch_1  /* 通用查询的工作参数，只支持一元过滤条件 */
{
    NorQue* p;
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    MutexLock *reslock;
    PtrQue *ResQ;
    u64 st;
    u64 n;

    //Fun_Ptr = NULL  //指向过滤条件的函数的指针 
    NorSch_1():p(NULL), ptr(NULL), cnt(NULL), tasklock(NULL), reslock(NULL), ResQ(NULL){}
    NorSch_1(NorQue* tp, Graph* tptr, volatile u64 *tcnt, MutexLock *ttasklock, MutexLock *treslock, PtrQue *tResQ)
        :p(tp), ptr(tptr), cnt(tcnt), tasklock(ttasklock), reslock(treslock), ResQ(tResQ){}
};

void NorSch_1_Fun(void* x)  /* 通用查询的工作函数 */
{
    NorSch_1* arg = (NorSch_1*)x;
    bool flag;

    for(u64 i = arg->st;i < arg->st + arg->n;++i)
    {
        flag = 1;
        if(arg->p->vec[i].cond->eneed)
        {   
            arg->ptr->GetEType(arg->p->vec[i].einfo, arg->p->vec[i].eid);
            if(flag && arg->p->vec[i].cond->etype != "" && arg->p->vec[i].cond->etype != arg->p->vec[i].einfo)
                flag = 0;
            if(flag && arg->p->vec[i].cond->edsp)
            {
                string scr("");
                arg->ptr->GetEDesp(scr, arg->p->vec[i].eid);
                arg->p->vec[i].einfo = arg->p->vec[i].einfo + (string)"|" + scr;
            }
            if(flag && arg->p->vec[i].cond->eobj != "")
            {
                string scr = arg->p->vec[i].einfo.substr(arg->p->vec[i].einfo.find_first_of('|') + 1);
                if(!arg->ptr->DataCmp(scr, arg->p->vec[i].cond->eobj, arg->p->vec[i].cond->edk, arg->p->vec[i].cond->eop))
                    flag = 0;
            }
        }  

        if(flag && arg->p->vec[i].cond->vneed)
        {
            arg->ptr->GetVInfo(arg->p->vec[i].vinfo, arg->p->vec[i].vid);
            if(arg->p->vec[i].cond->vobj != "" && !arg->ptr->DataCmp(arg->p->vec[i].vinfo, arg->p->vec[i].cond->vobj, arg->p->vec[i].cond->vdk, arg->p->vec[i].cond->vop))
                flag = 0;
            arg->p->vec[i].oldid = strtoull(arg->p->vec[i].vinfo.substr(0, arg->p->vec[i].vinfo.find_first_of('|')).c_str(), NULL, 10);
        }

        if(flag)
        {
            arg->ptr->GetVKindsById(arg->p->vec[i].vtype, arg->p->vec[i].vid); 
            arg->p->vec[i].effe = 1;
        }
        else
            arg->p->vec[i].effe = 0;
            
    }

    {
        MutexLockGuard tmut(*(arg->reslock));
        for(u64 i = arg->st;i < arg->st + arg->n;++i)
        {
            if(arg->p->vec[i].effe == 1)
                arg->ResQ->ptrv[arg->ResQ->tidx++] = &arg->p->vec[i];
        }
    }

    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}

struct IC_5_Arg
{
    PtrQue *ResQ;
    Graph* ptr;
    AdjList* adj;
    InArray* vis;
    MutexLock *tasklock;
    volatile u64 *cnt;
    u64 st;
    u64 n;
    u64 start1;
    u64 start3;
    u64 end3;
};

void IC_5_Fun(void* x)
{
    IC_5_Arg* arg = (IC_5_Arg*)x;
    u64 *res = NULL, *res2 = NULL;
    u64 nums, nums2, st, st2;

    for(u64 i = arg->st;i < arg->st + arg->n;++i)
    {
        arg->adj->GetNextVid(arg->ResQ->ptrv[i]->vid, res, nums, st);
        for(u64 j = 0;j < nums;++j)
        {
            if(res[j] >= arg->start3 && res[j] < arg->end3)
            {
                arg->adj->GetNextVid(res[j], res2, nums2, st2);
                u64 l = 0;
                while(res2[l] < arg->start1)
                    ++l;
                if(arg->vis->GetValue(res2[l]) == 1)
                    ++arg->ResQ->ptrv[i]->row;
            }
        }
    }
    if(res) delete [] res;
    if(res2) delete [] res2;
    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */

}

struct IC_13_Arg
{
    u64 *vec;
    u64 *tvec;
    AdjList* adj;
    uchar* vis;
    MutexLock *tasklock;
    u64 personSt;
    u64 *vecidx;
    volatile u64 *cnt;
    u64 st;
    u64 n;
    u64 start;
    u64 end;
    IC_13_Arg(u64* _vec, u64* _tvec, AdjList* _adj, uchar* _vis, MutexLock *_tasklock, u64 _personSt, u64 *_vecidx, volatile u64 *_cnt, u64 _st, u64 _n, u64 _start, u64 _end)
    :vec(_vec), tvec(_tvec), adj(_adj), vis(_vis), tasklock(_tasklock), personSt(_personSt), vecidx(_vecidx), cnt(_cnt), st(_st), n(_n), start(_start), end(_end){}; 
};

void IC_13_Fun(void* x)
{
    IC_13_Arg* arg = (IC_13_Arg*)x;
    u64 *res = NULL;
    u64 nums, st, idx = 0;
    for(u64 i = arg->st;i < arg->st + arg->n;++i)
    {
        // if(!arg->vis->GetValue(arg->vec[i]))
        {
            arg->adj->GetNextVid(arg->vec[i], res, nums, st);
            for(u64 j = 0;j < nums;++j)
            {
                if(res[j] >= arg->start && res[j] < arg->end && !arg->vis[res[j] - arg->personSt])
                {
                    arg->tvec[idx] = res[j];
                    ++idx;
                    // arg->vis[res[j]] = 1;
                }
                if(res[j] >= arg->end)
                    break;
            }
        }
    }
    arg->tvec[idx] = -1;//标记结束
    if(res) delete [] res;
    // {
    //     MutexLockGuard tmut(*(arg->veclock));
    //     for(u64 i = 0;i < idx;++i)
    //         arg->vec[(*arg->vecidx)++] = arg->tvec[i];
    // }  

    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  

    
    delete arg;
}

struct BISch_3
{   
    NorQue* p;
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    InArray *vis;
    GlobalVec *gVec;
    u64 st;
    u64 n;
    u64 id;
    BISch_3():p(NULL), ptr(NULL), cnt(NULL), tasklock(NULL), vis(NULL){}
    BISch_3(NorQue* tp, Graph* tptr, volatile u64 *tcnt, MutexLock *ttasklock, InArray *_vis, GlobalVec *_gVec)
        :p(tp), ptr(tptr), cnt(tcnt), tasklock(ttasklock), vis(_vis), gVec(_gVec){}

};

void BISch_3_Fun(void* x)
{
    BISch_3* arg = (BISch_3*)x;
    u64 *tmpVec = arg->gVec->tVec[arg->id];
    u64 postCondSt = arg->ptr->vkinds["post"].st, postCondEnd = arg->ptr->vkinds["post"].n + postCondSt;
    u64 tagCondSt = arg->ptr->vkinds["tag"].st, tagCondEnd = arg->ptr->vkinds["tag"].n + tagCondSt; 
    u64 mesCondSt = arg->ptr->vkinds["comment"].st, mesCondEnd = arg->ptr->vkinds["comment"].n + mesCondSt;
    u64 *mesRes = NULL, *postRes = NULL;
    u64 mesNums, mesSt, postNums, postSt;
    for(u64 i = arg->st;i < arg->st + arg->n;++i){
        u64 tmpVecHead = 0, tmpVecTail = 0, totalRes = 0;
        arg->ptr->Adj->GetNextVid(arg->p->vec[i].vid, postRes, postNums, postSt);
        for(u64 j = 0;j < postNums;++j){
            if(postRes[j] >= postCondSt && postRes[j] < postCondEnd)
                tmpVec[tmpVecTail++] = postRes[j];
            else if(postRes[j] >= postCondEnd)
                break;
        }
        while(tmpVecHead < tmpVecTail){
            arg->ptr->Adj->GetNextVid(tmpVec[tmpVecHead], mesRes, mesNums, mesSt);
            for(u64 j = 0;j < mesNums;++j){
                if(mesRes[j] >= tagCondSt && mesRes[j] < tagCondEnd && arg->vis->GetValue(mesRes[j])){
                    ++totalRes;
                    break;
                }
                else if(mesRes[j] >= tagCondEnd)
                    break;
            }
            arg->ptr->Adj->GetPrevVid(tmpVec[tmpVecHead], mesRes, mesNums, mesSt);
            for(u64 j = 0;j < mesNums;++j){
                if(mesRes[j] >= mesCondSt && mesRes[j] < mesCondEnd)
                    tmpVec[tmpVecTail++] = mesRes[j];
                else if(mesRes[j] >= mesCondEnd)
                    break;
            }
            ++tmpVecHead;
        }
        arg->p->vec[i].valid = totalRes;
    }
    if(mesRes) delete [] mesRes;
    if(postRes) delete [] postRes;
    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}

struct BISch_10_1
{   
    NorQue* p;
    PtrQue *ResQ;
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    MutexLock *reslock;
    InArray *vis;
    u64 st;
    u64 n;
    BISch_10_1():p(NULL), ptr(NULL), cnt(NULL), tasklock(NULL), reslock(NULL), vis(NULL), ResQ(NULL){}
    BISch_10_1(NorQue* tp, PtrQue *_ResQ, Graph* tptr, volatile u64 *tcnt, MutexLock *ttasklock, MutexLock *treslock, InArray *_vis)
        :p(tp), ResQ(_ResQ), ptr(tptr), cnt(tcnt), tasklock(ttasklock), reslock(treslock),vis(_vis){}

};

void BISch_10_Fun_1(void* x)
{
    BISch_10_1* arg = (BISch_10_1*)x;
    u64 placeCondSt = arg->ptr->vkinds["place"].st, placeCondEnd = arg->ptr->vkinds["place"].n + placeCondSt;
    u64 *countryRes = NULL;
    u64 countryNums, countrySt;
    for(u64 i = arg->st;i < arg->st + arg->n;++i){
        arg->p->vec[i].effe = 0;
        arg->ptr->Adj->GetNextVid(arg->p->vec[i].vid, countryRes, countryNums, countrySt);
        for(u64 j = 0;j < countryNums;++j){
            if(countryRes[j] >= placeCondSt && countryRes[j] < placeCondEnd && arg->vis->GetValue(countryRes[j])){
                arg->p->vec[i].effe = 1;
                arg->ptr->GetVInfo(arg->p->vec[i].vinfo, arg->p->vec[i].vid);
                arg->ptr->GetVKindsById(arg->p->vec[i].vtype, arg->p->vec[i].vid); 
            }
            else if(countryRes[j] >= placeCondEnd)
                break;
        }
    }
    if(countryRes) delete [] countryRes;

    {
        MutexLockGuard tmut(*(arg->reslock));
        for(u64 i = arg->st;i < arg->st + arg->n;++i)
        {
            if(arg->p->vec[i].effe == 1)
                arg->ResQ->ptrv[arg->ResQ->tidx++] = &arg->p->vec[i];
        }
    }

    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}

struct Ext_Tag_Arg
{
    unordered_map<u64, string>* tagMap;
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    u64 st;
    u64 n;
    Ext_Tag_Arg():tagMap(NULL), ptr(NULL), cnt(NULL), tasklock(NULL){}
    Ext_Tag_Arg(unordered_map<u64, string>* _tagMap, Graph* _ptr, volatile u64 *_cnt, MutexLock *_tasklock)
                :tagMap(_tagMap), ptr(_ptr), cnt(_cnt), tasklock(_tasklock){}
};

void Ext_Tag_Fun(void* x)
{
    Ext_Tag_Arg* arg = (Ext_Tag_Arg*)x;
    for(u64 i = arg->st;i < arg->n + arg->st;++i){
        string tmpVInfo;
        arg->ptr->GetVInfo(tmpVInfo, i);
        arg->ptr->GetRowInfo((*arg->tagMap)[i], "name", tmpVInfo, "tag");
    }

    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}

struct Get_Id_Arg
{
    u64 *realId;
    string row;
    string type;
    string value;
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    u64 st;
    u64 n;
    Get_Id_Arg():realId(NULL), ptr(NULL), cnt(NULL), tasklock(NULL){}
    Get_Id_Arg(u64 * _realId, Graph* _ptr, volatile u64 *_cnt, MutexLock *_tasklock)
                :realId(_realId), ptr(_ptr), cnt(_cnt), tasklock(_tasklock){}
};

void Get_Id_Fun(void* x)
{
    Get_Id_Arg* arg = (Get_Id_Arg*)x;
    string tmpVInfo;
    for(u64 i = arg->st;i < arg->n + arg->st;++i){
        string tmpRes;
        arg->ptr->GetVInfo(tmpVInfo, i);
        arg->ptr->GetRowInfo(tmpRes, arg->row, tmpVInfo, arg->type);
        if(tmpRes == arg->value){
            *(arg->realId) = i;
        }
    }

    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}

struct BISch_10_2
{
    PtrQue *ResQ;
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    InArray *vis;
    vector<unordered_map<u64, u64> > *ptrPerson;
    u64 st;
    u64 n;
    BISch_10_2():ResQ(NULL), ptr(NULL), cnt(NULL), tasklock(NULL), vis(NULL), ptrPerson(NULL){}
    BISch_10_2(PtrQue *_ResQ, Graph* _ptr, volatile u64 *_cnt, MutexLock *_tasklock, InArray *_vis, vector<unordered_map<u64, u64> > *_ptrPerson)
        :ResQ(_ResQ), ptr(_ptr), cnt(_cnt), tasklock(_tasklock), vis(_vis), ptrPerson(_ptrPerson){};
};

void BISch_10_Fun_2(void* x)
{
    BISch_10_2* arg = (BISch_10_2*)x;
    u64 postSt = arg->ptr->vkinds["post"].st, postEnd = arg->ptr->vkinds["post"].n + postSt;
    u64 commentSt = arg->ptr->vkinds["comment"].st, commentEnd = arg->ptr->vkinds["comment"].n + commentSt;
    u64 *messageRes = NULL, *tagRes = NULL;
    u64 messageNums, messageSt, tagNums, tagSt;
    u64 tagCondSt = arg->ptr->vkinds["tag"].st, tagCondEnd = arg->ptr->vkinds["tag"].n + tagCondSt;
    for(u64 i = arg->st;i < arg->st + arg->n;++i){
        GNode *t = arg->ResQ->ptrv[i];
        arg->ptr->Adj->GetPrevVid(t->vid, messageRes, messageNums, messageSt);
        for(u64 j = 0;j < messageNums;++j){
            if((messageRes[j] >= postSt && messageRes[j] < postEnd) || (messageRes[j] >= commentSt && messageRes[j] < commentEnd)){
                arg->ptr->Adj->GetNextVid(messageRes[j], tagRes, tagNums, tagSt);
                u64 flag = 0;
                for(u64 k = 0;k < tagNums;++k){
                    if(tagRes[k] >= tagCondSt && tagRes[k] < tagCondEnd && arg->vis->GetValue(tagRes[k])){
                        flag = 1;
                        break;
                    }
                }
                if(flag){
                    for(u64 k = 0;k < tagNums;++k){
                        if(tagRes[k] >= tagCondSt && tagRes[k] < tagCondEnd)
                            ++(*arg->ptrPerson)[i][tagRes[k]];
                        else if(tagRes[k] >= tagCondEnd)
                            break;
                    }
                }
            }
        }
    }
    if(messageRes) delete [] messageRes;
    if(tagRes) delete [] tagRes;
    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}

struct BISch_16_Vec
{
    u64 oldId;
    u64 realId;
    u32 mesCountA;
    u32 mesCountB;
    BISch_16_Vec():oldId(-1), realId(0), mesCountA(0), mesCountB(0){}
};

struct BISch_16
{
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    InArray *vis;
    vector<BISch_16_Vec> *personVec;
    u64 st;
    u64 n;
    u64 maxKnowsLimit;
    BISch_16():ptr(NULL), cnt(NULL), tasklock(NULL), vis(NULL), personVec(NULL){}
    BISch_16(Graph* _ptr, volatile u64 *_cnt, MutexLock *_tasklock, InArray *_vis, vector<BISch_16_Vec> *_personVec)
        :ptr(_ptr), cnt(_cnt), tasklock(_tasklock), vis(_vis), personVec(_personVec){};

};

void BISch_16_Fun(void* x)
{
    BISch_16* arg = (BISch_16*)x;
    u64 personCondSt = arg->ptr->vkinds["person"].st, personCondEnd = arg->ptr->vkinds["person"].n + personCondSt;
    u64 *personRes = NULL;
    u64 personNums, st;
    for(u64 i = arg->st;i < arg->st + arg->n;++i){
        if(arg->vis->GetValue((*arg->personVec)[i].realId)){
            u64 knowsCnt = 0;
            arg->ptr->Adj->GetNextVid((*arg->personVec)[i].realId, personRes, personNums, st);
            for(u64 j = 0;j < personNums;++j){
                if(personRes[j] >= personCondSt && personRes[j] < personCondEnd && arg->vis->GetValue(personRes[j]))
                    ++knowsCnt;
                else if(knowsCnt > arg->maxKnowsLimit || personRes[j] > personCondEnd)
                    break;
            }
            if(knowsCnt <= arg->maxKnowsLimit){
                string personId;
                arg->ptr->GetVInfo(personId, (*arg->personVec)[i].realId);
                (*arg->personVec)[i].oldId = strtoull(personId.substr(0, personId.find_first_of('|')).c_str(), NULL, 10);
            }
            else{
                (*arg->personVec)[i].mesCountA = 0;
                (*arg->personVec)[i].mesCountB = 0;
            }
        }   
    }
    if(personRes) delete [] personRes;
    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}

struct BISch_16_2
{
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    InArray *visA;
    InArray *visB;
    vector<BISch_16_Vec> *personVec;
    u64 st;
    u64 n;
    u64 maxKnowsLimit;
    BISch_16_2():ptr(NULL), cnt(NULL), tasklock(NULL), visA(NULL), personVec(NULL){}
    BISch_16_2(Graph* _ptr, volatile u64 *_cnt, MutexLock *_tasklock, InArray *_visA, InArray *_visB, vector<BISch_16_Vec> *_personVec)
        :ptr(_ptr), cnt(_cnt), tasklock(_tasklock), visA(_visA), visB(_visB), personVec(_personVec){};
};

void BISch_16_Fun_2(void* x)
{
    BISch_16_2* arg = (BISch_16_2*)x;
    u64 personCondSt = arg->ptr->vkinds["person"].st, personCondEnd = arg->ptr->vkinds["person"].n + personCondSt;
    u64 *personRes = NULL;
    u64 personNums, st;
    for(u64 i = arg->st;i < arg->st + arg->n;++i){
        u64 flag = 1;
        arg->ptr->Adj->GetNextVid((*arg->personVec)[i].realId, personRes, personNums, st);
        u64 knowsCnt = 0;
        for(u64 j = 0;j < personNums;++j){
            if(personRes[j] >= personCondSt && personRes[j] < personCondEnd && arg->visA->GetValue(personRes[j] - personCondSt))
                ++knowsCnt;
            else if(knowsCnt > arg->maxKnowsLimit || personRes[j] > personCondEnd)
                break;
        }
        if(knowsCnt > arg->maxKnowsLimit)
            flag = 0;
        if(flag){
            knowsCnt = 0;
            for(u64 j = 0;j < personNums;++j){
                if(personRes[j] >= personCondSt && personRes[j] < personCondEnd && arg->visB->GetValue(personRes[j] - personCondSt))
                    ++knowsCnt;
                else if(knowsCnt > arg->maxKnowsLimit || personRes[j] > personCondEnd)
                    break;
            }
        }
        if(knowsCnt > arg->maxKnowsLimit)
            flag = 0;
        if(flag){
            string personId;
            arg->ptr->GetVInfo(personId, (*arg->personVec)[i].realId);
            (*arg->personVec)[i].oldId = strtoull(personId.substr(0, personId.find_first_of('|')).c_str(), NULL, 10);
        }
        else{
            (*arg->personVec)[i].mesCountA = 0;
            (*arg->personVec)[i].mesCountB = 0;
        }
    }
    if(personRes) delete [] personRes;
    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}

struct FriendFind
{
    NorQue* p;
    Graph* ptr;
    volatile u64 *cnt;  /* 完成的数目 */
    MutexLock *tasklock;
    MutexLock *veclock;
    GNode *ownVec;
    InArray *vis;
    CondData *condArg;
    u64 st;
    u64 n;
    u64 ownVecSize;
    FriendFind():p(NULL), ptr(NULL), cnt(NULL), tasklock(NULL), veclock(NULL), ownVec(NULL), vis(NULL), condArg(NULL){}
    FriendFind(NorQue* tp, Graph* tptr, InArray *_vis, volatile u64 *tcnt, MutexLock *ttasklock, MutexLock *tveclock, GNode *_ownVec, CondData *_condArg, u64 _ownVecSize)
        :p(tp), ptr(tptr), vis(_vis), cnt(tcnt), tasklock(ttasklock), veclock(tveclock), ownVec(_ownVec), condArg(_condArg), ownVecSize(_ownVecSize){}

};

void FriendFind_Fun(void* x)
{   
    FriendFind* arg = (FriendFind*)x;
    u64 personSt = arg->ptr->vkinds["person"].st, personEnd = personSt + arg->ptr->vkinds["person"].n;
    u64 *res = NULL;
    u64 nums, st, ownVecIdx = 0;
    for(u64 i = arg->st;i < arg->st + arg->n;++i){
        arg->ptr->Adj->GetNextVid(arg->p->vec[i].vid, res, nums, st);
        for(u64 j = 0;j < nums;++j){
            if(!arg->vis->GetValue(res[j]) && res[j] >= personSt && res[j] < personEnd){
                arg->ownVec[ownVecIdx].vid = res[j];
                arg->ownVec[ownVecIdx].eid = st;
                arg->ownVec[ownVecIdx].level = arg->p->vec[i].level + 1;
                arg->ownVec[ownVecIdx].prev = &arg->p->vec[i];
                arg->ownVec[ownVecIdx].cond = arg->condArg;
                ++ownVecIdx;
                arg->vis->SetValue(res[j], 1);
                if(ownVecIdx == arg->ownVecSize){
                    MutexLockGuard tmpMutex(*arg->tasklock);
                    for(u64 k = 0;k < ownVecIdx;++k)
                        arg->p->vec[arg->p->tidx++] = arg->ownVec[k];
                    ownVecIdx = 0;
                }
            }
            else if(res[j] >= personEnd)
                break;
            ++st;
        }
        arg->ptr->Adj->GetPrevVid(arg->p->vec[i].vid, res, nums, st);
        for(u64 j = 0;j < nums;++j){
            if(!arg->vis->GetValue(res[j]) && res[j] >= personSt && res[j] < personEnd){
                arg->ownVec[ownVecIdx].vid = res[j];
                arg->ownVec[ownVecIdx].eid = st;
                arg->ownVec[ownVecIdx].level = arg->p->vec[i].level + 1;
                arg->ownVec[ownVecIdx].prev = &arg->p->vec[i];
                arg->ownVec[ownVecIdx].cond = arg->condArg;
                ++ownVecIdx;
                arg->vis->SetValue(res[j], 1);
                if(ownVecIdx == arg->ownVecSize){
                    MutexLockGuard tmpMutex(*arg->tasklock);
                    for(u64 k = 0;k < ownVecIdx;++k)
                        arg->p->vec[arg->p->tidx++] = arg->ownVec[k];
                    ownVecIdx = 0;
                }
            }
            else if(res[j] >= personEnd)
                break;
            ++st;
        }
    }   
    if(ownVecIdx > 0){
        MutexLockGuard tmpMutex(*arg->tasklock);
        for(u64 k = 0;k < ownVecIdx;++k)
            arg->p->vec[arg->p->tidx++] = arg->ownVec[k];
    }
    if(res) delete [] res;
    {
        MutexLockGuard tmut(*(arg->tasklock));
        ++(*arg->cnt);
    }  
    delete arg;  /* 要不要使用free ? */
}  


/* 工作函数部分结束 */


Graph::Graph():Vertex_csa(NULL), Edge_csa(NULL), Adj(NULL), hasdes(NULL), etype(NULL), rootdir(""), pool(NULL)
{
    for(u64 i = 0;i < MAX_USERS;++i)
        Que[i] = NULL;
}

void Graph::GetFiles(vector<string>& vfn, vector<string>& efn, string& vmapfn,
                     string& vkindsfn, string& ekindsfn, string& vrowsfn, 
                     string& erowsfn, string& adjfn, string& hasdesfn,
                     string& emapfn, string& etypefn)         
{
    DIR *dir;
    struct dirent *ptr;

    if ((dir = opendir(rootdir.c_str())) == NULL)
    {
        perror("Open dir error...");
        exit(1);
    }

    /*
    * 文件(8)、目录(4)、链接文件(10)
    */

    while ((ptr = readdir(dir)) != NULL)
    {
        if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0)
            continue;
        else if (ptr->d_type == 8)
        {
            string tmp(ptr->d_name);
            u64 pos = tmp.find_last_of('.');
            if(pos != string::npos)
            {
                if(tmp == "Vmap.myg")
                    vmapfn = rootdir + tmp;
                else if(tmp == "Vkinds.myg")
                    vkindsfn = rootdir + tmp;
                else if(tmp == "Vrows.myg")
                    vrowsfn = rootdir + tmp;
                else if(tmp == "Ekinds.myg")
                    ekindsfn = rootdir + tmp;
                else if(tmp == "Erows.myg")
                    erowsfn = rootdir + tmp;   
                else if(tmp == "AdjList.myg") 
                    adjfn = rootdir + tmp;
                else if(tmp == "EHasdes.myg") 
                    hasdesfn = rootdir + tmp;
                else if(tmp == "Emap.myg") 
                    emapfn = rootdir + tmp;
                else if(tmp == "Etype.myg") 
                    etypefn = rootdir + tmp;    
                else if(tmp.substr(pos) == ".geindex" && tmp.substr(0, tmp.find_first_of('_')) == "Vertex")
                    vfn.push_back(rootdir + tmp);
                else if(tmp.substr(pos) == ".geindex" && tmp.substr(0, tmp.find_first_of('_')) == "Edge")
                    efn.push_back(rootdir + tmp);
            }
        }
    }
    sort(vfn.begin(), vfn.end(), [](const string &x, const string &y)
                                        {
                                            int a1 = x.find_last_of('_') + 1;
                                            int b1 = y.find_last_of('_') + 1;
                                            return atoi(x.substr(a1).c_str())
                                                    <= atoi(y.substr(b1).c_str());
                                        }   );
    sort(efn.begin(), efn.end(), [](const string &x, const string &y)
                                        {
                                            int a1 = x.find_last_of('_') + 1;
                                            int b1 = y.find_last_of('_') + 1;
                                            return atoi(x.substr(a1).c_str())
                                                    <= atoi(y.substr(b1).c_str());
                                        }   );
    closedir(dir);
}

void Graph::LoadFiles(string dir)
{
    rootdir = dir;
    vector<string> vfn, efn; 
    string vmapfn, vkindsfn,  ekindsfn,  vrowsfn,  erowsfn, adjfn, hasdesfn, emapfn, etypefn;
    GetFiles(vfn, efn, vmapfn, vkindsfn, ekindsfn, vrowsfn, erowsfn, adjfn, hasdesfn, emapfn, etypefn); /* 处理好了所有的文件名字 */
    cerr<<"Load Files : "<<endl;
    cerr<<vmapfn<<endl;
    cerr<<vkindsfn<<endl;
    cerr<<ekindsfn<<endl;
    cerr<<vrowsfn<<endl;
    cerr<<erowsfn<<endl;
    cerr<<adjfn<<endl;
    cerr<<hasdesfn<<endl;
    cerr<<emapfn<<endl;
    cerr<<etypefn<<endl;
    for(u64 i = 0;i < vfn.size();++i)
        cerr<<vfn[i]<<endl;
    for(u64 i = 0;i < efn.size();++i)
        cerr<<efn[i]<<endl;
    
    /*
        TODO
    */
    /* 装载辅助文件 */
    LoadVmap(vmapfn);
    LoadVkinds(vkindsfn);
    LoadEkinds(ekindsfn);
    LoadVrows(vrowsfn);
    LoadErows(erowsfn);
    LoadEHasdes(hasdesfn);
    LoadEmap(emapfn);
    LoadEtype(etypefn);
    /*
        TODO
    */
    LoadVindex(vfn);
    LoadEindex(efn);    
    LoadAdjList(adjfn);  

}

void Graph::LoadVmap(string& fn)
{
    ifstream fin;
    fin.open(fn.c_str(), ios::in);
    string tmp("");
    getline(fin, tmp);
    int size = atoi(tmp.c_str());
    for(int i = 0;i < size;++i)
    {
        pair<string, VertexMap> t;
        getline(fin, t.first);
        getline(fin, tmp);
        t.second.st = strtoull(tmp.c_str(), NULL, 10);
        t.second.bphf = new boophf_t;
        t.second.bphf->load(fin);
        vmap.insert(t);
    }
    fin.close();
} 

void Graph::LoadVkinds(string& fn)
{
    ifstream fin;
    fin.open(fn.c_str(), ios_base::in);
    string tmp("");
    u64 total = 0;
    Keyvalue tkv;
    while(getline(fin, tmp))
    {   
        pair<string, Vdetail> t;

        tkv.s = tmp;
        tkv.id = total;
        VidMapKinds.push_back(tkv);

        t.first = tmp;
        getline(fin, t.second.head);
        getline(fin, tmp);
        t.second.rows = atoi(tmp.c_str());
        for(u64 j = 0;j < t.second.rows;++j)
        {
            pair<string, Bigram> tt;
            getline(fin, tt.first);
            getline(fin, tmp);
            tt.second.v1 = strtoull(tmp.c_str(), NULL, 10);
            getline(fin, tmp);
            tt.second.v2 = strtoull(tmp.c_str(), NULL, 10);
            t.second.hmap.insert(tt);
        }
        getline(fin, tmp);
        t.second.n = strtoull(tmp.c_str(), NULL, 10);
        getline(fin, tmp);
        t.second.st = strtoull(tmp.c_str(), NULL, 10);
        vkinds.insert(t);

        total += t.second.n;
    }
    tkv.id = total;
    VidMapKinds.push_back(tkv);
    fin.close();
    cerr<<"Vkinds.myg read is OK!"<<endl;
}

void Graph::LoadVrows(string& fn)
{
    ifstream fin;
    fin.open(fn, ios_base::in);
    string tmp("");
    while(getline(fin, tmp))
        Vertex_rows.push_back(strtoull(tmp.c_str(), NULL, 10));  
    fin.close();
    cerr<<"Vrows.myg read is OK!"<<endl;
}

void Graph::LoadEkinds(string& fn)
{
    ifstream fin;
    fin.open(fn, ios_base::in);
    string tmp("");
    
    while(getline(fin, tmp))
    {
        pair<string, Edetail> t;
        t.first = tmp;
        getline(fin, tmp);
        t.second.n = strtoull(tmp.c_str(), NULL, 10);
        for(u64 j = 0;j < t.second.n;++j)
        {
            pair<string, Especial> tt;
            getline(fin, tt.first);
            getline(fin, tt.second.head);
            getline(fin, tmp);
            tt.second.hasexp = atoi(tmp.c_str());
            getline(fin, tmp);
            tt.second.n = strtoull(tmp.c_str(), NULL, 10);
            if(tt.second.hasexp)
            {
                getline(fin, tmp);
                tt.second.rows = atoi(tmp.c_str());
                for(u64 k = 0;k < tt.second.rows;++k)
                {
                    pair<string, Bigram> ttt;
                    getline(fin, ttt.first);
                    getline(fin, tmp);
                    ttt.second.v1 = strtoull(tmp.c_str(), NULL, 10);
                    getline(fin, tmp);
                    ttt.second.v2 = strtoull(tmp.c_str(), NULL, 10);
                    tt.second.hmap.insert(ttt);
                }
            }
            else
                getline(fin, tmp);
            t.second.detail.insert(tt);
        }
        ekinds.insert(t);
    }

    fin.close();
    cerr<<"Ekinds.myg read is OK!"<<endl;
}

void Graph::LoadErows(string& fn)
{
    ifstream fin;
    fin.open(fn, ios_base::in);
    string tmp("");
    while(getline(fin, tmp))
        Edge_rows.push_back(strtoull(tmp.c_str(), NULL, 10));  
    fin.close();
    cerr<<"Erows.myg read is OK!"<<endl;
}

void Graph::LoadVindex(vector<string>& vfn)
{
    u64 len = vfn.size();
    Vertex_csa = new CSA*[len];
    for(u64 i = 0;i < len;++i)
    {
        Vertex_csa[i] = new CSA();
        Vertex_csa[i]->load(vfn[i].c_str());
        cerr<<vfn[i]<<" load is OK!"<<endl;
    }
    Vcsa_cnt = len;
}

void Graph::LoadEindex(vector<string>& efn)
{
    u64 len = efn.size();
    Edge_csa = new CSA*[len];
    for(u64 i = 0;i < len;++i)
    {
        Edge_csa[i] = new CSA();
        Edge_csa[i]->load(efn[i].c_str());
        cerr<<efn[i]<<" load is OK!"<<endl;
    }
    Ecsa_cnt = len;
}

void Graph::LoadAdjList(string& adjfn)
{
    Adj = new AdjList();
    loadkit s(adjfn.c_str());
    Adj->Load(s);
    total_e = Adj->GetEnum();
    total_v = Adj->GetVnum();
    cerr<<"AdjList.myg read is OK!"<<endl;
}

void Graph::LoadEHasdes(string& fn)
{
    loadkit s(fn.c_str());
    hasdes = new InArray();
    hasdes->load(s, 1);
    cerr<<"Ehasdes.myg read is OK!"<<endl;
}

void Graph::LoadEmap(string& fn)
{
    ifstream fin;
    fin.open(fn, ios_base::in);
    string tmp("");
    while(getline(fin, tmp))
    {
        pair<string, u64> t;
        pair<u64, string> it;
        t.first = tmp;
        it.second = tmp;

        getline(fin, tmp);
        t.second = strtoull(tmp.c_str(), NULL, 10);
        it.first = t.second;
        emap.insert(t);
        iemap.insert(it);
    }
    fin.close();
    cerr<<"Emap.myg read is OK!"<<endl;
}

void Graph::LoadEtype(string& fn)
{
    loadkit s(fn.c_str());
    etype = new InArray();
    etype->load(s, 0);
    cerr<<"Etype.myg read is OK!"<<endl;
}

void Graph::Run()
{
    /* 其他预处理 */
    max_threads = par::get_max_threads() - 1;
    pool = new threadpool(max_threads);
    pool->creat_thread();
    /* 接收连接命令 (待实现) */
    /* 每建立一个连接就创建一个线程去服务 (待实现) */
    u64 Q_size = 3 << 22;
    Que[0] = new UserData;
    Que[0]->SchQ = new NorQue;
    Que[0]->SchQ->vec = new GNode[Q_size];
    memset(Que[0]->SchQ->vec, 0 , Q_size * sizeof(GNode));
    Que[0]->ResQ = new PtrQue;
    Que[0]->ResQ->ptrv = new GNode*[Q_size];
    memset(Que[0]->ResQ->ptrv, 0 , Q_size * sizeof(GNode*));
    Que[0]->tasklock = new MutexLock;
    Que[0]->reslock = new MutexLock;
    Que[0]->vis = new InArray(total_v, 1);
    while(1)
    {
        handleSerch();
        Que[0]->vis->ReSet();
        memset(Que[0]->SchQ->vec, 0 , Q_size * sizeof(GNode));
        memset(Que[0]->ResQ->ptrv, 0 , Q_size * sizeof(GNode*));
    }
}


void Graph::GetVInfo(string& res, u64& id)
{
    u64 rows, idx;
    FindVertexFile(id, idx, rows);
    Vertex_csa[idx]->GetInfo(res, rows);
}

void Graph::GetEInfo(string& res, u64& id)
{
    u64 rows, idx;
    res = iemap[etype->GetValue(id)];
    if(hasdes->getD(id))
    {
        res += '|';
        u64 tid = hasdes->rank(id) - 1;
        FindEdgeFile(tid, idx, rows);
        string tres("");
        Edge_csa[idx]->GetInfo(tres, rows);
        res += tres;
    }
}

void Graph::GetEType(string& res, u64& id)
{
    res = iemap[etype->GetValue(id)];
}

void Graph::GetEDesp(string& res, u64& id)
{
    u64 tid = hasdes->rank(id) - 1;
    u64 rows, idx;
    FindEdgeFile(tid, idx, rows);
    Edge_csa[idx]->GetInfo(res, rows);
}

u64 Graph::GetVLoc(const string& type, const string& col, const string& value){
    u64 typeSt = vkinds[type].st, typeEnd = vkinds[type].st + vkinds[type].n - 1;
    /* 找到起始行号 */
    u64 stVIdx = 0, stRow = typeSt;
    for(;stVIdx < Vcsa_cnt;++stVIdx){
        if(stRow < Vertex_rows[stVIdx + 1])
            break;
    }
    /* 找到结束行号 */
    u64 endVIdx = 0, endRow = typeEnd;
    for(;endVIdx < Vcsa_cnt;++endVIdx){
        if(endRow < Vertex_rows[endVIdx + 1])
            break;
    }
    /* locate */
    for(u64 i = stVIdx;i <= endVIdx;++i){
        u64 num, lRow = typeSt - Vertex_rows[i], rRow = typeEnd > Vertex_rows[i + 1] ? Vertex_rows[i + 1] - Vertex_rows[i] : typeEnd - Vertex_rows[i];
        u64 *pos = (u64*)Vertex_csa[i]->GetLocV((uchar*)value.c_str(), lRow, rRow + 1, num);
        for(u64 j = 0;j < num;++j){
            string res, tmpRes;
            Vertex_csa[i]->GetInfo(res, pos[j]);
            GetRowInfo(tmpRes, col, res, type);
            if(tmpRes == value)
                return pos[j] + Vertex_rows[i];
        }
    }
    return -1;
}


bool Graph::DataCmp(const string& src, string& cond, Bigram& row, u64& op)
{
    /* row从1开始 */
    u64 st = 0, end, len = src.length(), cnt = 1;
    string res("");
    res.reserve(32);
    for(;st < len;++st)
    {
        if(src[st] == '|')
            ++cnt;
        else if(cnt == row.v1)
            res += src[st];
        else if(cnt > row.v1)
            break;
    }
    switch (row.v2)
    {
        case TypeMap::booltype:
        case TypeMap::int32type :
        case TypeMap::int64type :
        {
            switch (op)
            {
                case 0 :
                    return strtoull(res.c_str(), NULL, 10) == strtoull(cond.c_str(), NULL, 10);
                case -1ull :
                    return strtoull(res.c_str(), NULL, 10) <= strtoull(cond.c_str(), NULL, 10);
                case -2ull :
                    return strtoull(res.c_str(), NULL, 10) < strtoull(cond.c_str(), NULL, 10);
                case 1ull :
                    return strtoull(res.c_str(), NULL, 10) >= strtoull(cond.c_str(), NULL, 10);
                case 2ull :
                    return strtoull(res.c_str(), NULL, 10) > strtoull(cond.c_str(), NULL, 10);
                case 3ull :
                    return strtoull(res.c_str(), NULL, 10) != strtoull(cond.c_str(), NULL, 10);
            }
            break;
        }
        case TypeMap::flout32type :
        {
            switch (op)
            {
                case 0 :
                    return strtof(res.c_str(), NULL) == strtof(cond.c_str(), NULL);
                case -1ull :
                    return strtof(res.c_str(), NULL) <= strtof(cond.c_str(), NULL);
                case -2ull :
                    return strtof(res.c_str(), NULL) < strtof(cond.c_str(), NULL);
                case 1ull :
                    return strtof(res.c_str(), NULL) >= strtof(cond.c_str(), NULL);
                case 2ull :
                    return strtof(res.c_str(), NULL) > strtof(cond.c_str(), NULL);
                case 3ull :
                    return strtof(res.c_str(), NULL) != strtof(cond.c_str(), NULL);
            }
            break;
        }
        case TypeMap::flout64type :
        {
            switch (op)
            {
                case 0 :
                    return strtof64(res.c_str(), NULL) == strtof64(cond.c_str(), NULL);
                case -1ull :
                    return strtof64(res.c_str(), NULL) <= strtof64(cond.c_str(), NULL);
                case -2ull :
                    return strtof64(res.c_str(), NULL) < strtof64(cond.c_str(), NULL);
                case 1ull :
                    return strtof64(res.c_str(), NULL) >= strtof64(cond.c_str(), NULL);
                case 2ull :
                    return strtof64(res.c_str(), NULL) > strtof64(cond.c_str(), NULL);
                case 3ull :
                    return strtof64(res.c_str(), NULL) != strtof64(cond.c_str(), NULL);
            }
            break;
        }
        case TypeMap::datetimetype :
        {
            switch (op)
            {
                case 0 :
                    return res.substr(0, 10) == cond;
                case -1ull :
                    return res.substr(0, 10) <= cond;
                case -2ull :
                    return res.substr(0, 10) < cond;
                case 1ull :
                    return res.substr(0, 10) >= cond;
                case 2ull :
                    return res.substr(0, 10) > cond;
                case 3ull :
                    return res.substr(0, 10) != cond;
            }
            break;
        }
        case TypeMap::datetype :
        case TypeMap::stringtype : 
        {
            switch (op)
            {
                case 0 :
                    return res == cond;
                case -1ull :
                    return res <= cond;
                case -2ull :
                    return res < cond;
                case 1ull :
                    return res >= cond;
                case 2ull :
                    return res > cond;
                case 3ull :
                    return res != cond;
            }
            break;
        }
    }
}

void Graph::handleSerch()
{
    /* 输入部分，可以按照查询的需求选择需要的函数接口 */  
    /* 过滤条件形如json */
    PrintHelp();
    string mod;
    cin>>mod;
}

/* 单线程搞定 */
void Graph::NormalSearch() 
{
    string type;
    u64 id, jns, realid;
    cerr<<"Format : VertexType VertexId JumpNums"<<endl;
    cin>>type>>id>>jns;
clock_t bb, ee;
bb = clock();

    u64 nums, st, i;
    NorQue *Q = Que[0]->SchQ; /* 下标是用户的id */
    InArray *vis = Que[0]->vis;
    u64 &hidx = Q->hidx, &tidx = Q->tidx;
    hidx = 0;
    tidx = 0;
    u64* res = NULL;
    /* 得到真实顶点 */
    GetRealId(type, id, realid);
    /* 第一个顶点单独处理 */
    GNode fv;
    fv.level = 0;
    fv.vid = realid;
    fv.vtype = type;
    GetVInfo(fv.vinfo, realid);
    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    fv.oldid = strtoull(fv.vinfo.substr(0, fv.vinfo.find_first_of('|')).c_str(), NULL, 10);
        /* 第一跳入队 */
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]))
        {
            Q->vec[tidx].vid = res[i];
            Q->vec[tidx].eid = st;
            Q->vec[tidx].level = 1;
            Q->vec[tidx].prev = &fv;
            ++tidx;
            vis->SetValue(res[i], 1);
        }
        ++st;
    }

    while(hidx < tidx)
    {
        GNode &t = Q->vec[hidx];   
        GetEInfo(t.einfo, t.eid);
        GetVInfo(t.vinfo, t.vid);
        t.oldid = strtoull(t.vinfo.substr(0, t.vinfo.find_first_of('|')).c_str(), NULL, 10);
        GetVKindsById(t.vtype, t.vid);
        ++hidx;
        if(t.level < jns)
        {
            Adj->GetNextVid(t.vid, res, nums, st);
            for(i = 0;i < nums;++i)
            {
                if(!vis->GetValue(res[i]))
                {
                    GNode& tnode = Q->vec[tidx];
                    tnode.vid = res[i];
                    tnode.eid = st;
                    tnode.prev = &t;
                    tnode.level = t.level + 1;
                
                    ++tidx;
                    vis->SetValue(res[i], 1);
                }
                ++st;
            }
        }
        
    }
ee = clock();

    cout<<fv.vinfo;
    for(u64 i = 0;i < tidx;++i)
    {
        string res("");
        DisplayDistance(&Q->vec[i], res);
        cout<<res<<Q->vec[i].vtype<<" : "<<Q->vec[i].vinfo;
    }

/* 资源释放部分 */
    if(res != NULL)
        delete [] res;

    cerr<<type<<'\t'<<id<<" found "<<tidx<<" Vertex: "<<endl;
    cerr<<"---------First jump cost: "<<ee - bb<<" us"<<"---------"<<endl;
}


void Graph::NewSearch()
{
    string type;
    u64 id, jns, realid;
    cerr<<"Format : VertexType VertexId JumpNums"<<endl;
    cin>>type>>id>>jns;
clock_t bb, ee;
bb = clock();
    
    u64 nums, st, i;
    NorQue *Q = Que[0]->SchQ; /* 下标是用户的id */
    InArray *vis = Que[0]->vis;
    u64 &hidx = Q->hidx, &tidx = Q->tidx;
    hidx = 0;
    tidx = 0;
    u64* res = NULL;
    /* 得到真实顶点 */
    GetRealId(type, id, realid);
    /* 第一个顶点单独处理 */
    GNode fv;
    fv.level = 0;
    fv.prev = NULL;
    fv.vid = realid;
    fv.vtype = type;
    fv.multin = 1; /* 这个是预先知道的 */
    fv.multi = new MultiGNode[fv.multin];
    GetVInfo(fv.vinfo, realid);
    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    fv.oldid = strtoull(fv.vinfo.substr(0, fv.vinfo.find_first_of('|')).c_str(), NULL, 10);
        /* 第一跳入队 */
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]))
        {
            Q->vec[tidx].vid = res[i];
            Q->vec[tidx].eid = st;
            Q->vec[tidx].level = 1;
            Q->vec[tidx].prev = &fv;
            Q->vec[tidx].multin = 1; /* 这个是预先知道的 */
            Q->vec[tidx].multi = new MultiGNode[Q->vec[tidx].multin];
            ++tidx;
            vis->SetValue(res[i], 1);
        }
        ++st;
    }

    while(hidx < tidx)
    {
        GNode &t = Q->vec[hidx];   
        GetEInfo(t.einfo, t.eid);
        GetVInfo(t.vinfo, t.vid);
        t.oldid = strtoull(t.vinfo.substr(0, t.vinfo.find_first_of('|')).c_str(), NULL, 10);
        GetVKindsById(t.vtype, t.vid);
        ++hidx;
        if(t.level < jns)
        {
            Adj->GetNextVid(t.vid, res, nums, st);
            for(i = 0;i < nums;++i)
            {
                if(!vis->GetValue(res[i]))
                {
                    GNode& tnode = Q->vec[tidx];
                    tnode.vid = res[i];
                    tnode.eid = st;
                    tnode.prev = &t;
                    tnode.level = t.level + 1;
                    if(tnode.level < jns)
                    {
                        tnode.multin = 1;
                        tnode.multi = new MultiGNode[tnode.multin];
                    }
                    ++tidx;
                    vis->SetValue(res[i], 1);
                }
                ++st;
            }
        }
    }
bb = clock();
    /* 路径追溯代码 */
    /* 第一次遍历计算数量 */
    for(i64 j = tidx - 1;j >= 0;--j)
        ++Q->vec[j].prev->multi[0].n; /* 知道multi的下标 */

    /* 第一次遍历开辟空间，并链起来 */
    fv.multi[0].vec = new GNode*[fv.multi[0].n];
    for(i = 0;i < tidx;++i)
    {
        if(Q->vec[i].multin != 0)
        {
            for(u64 j = 0;j < Q->vec[i].multin;++j)
                Q->vec[i].multi[j].vec = new GNode*[Q->vec[i].multi[j].n];
        }
        Q->vec[i].prev->multi[0].vec[Q->vec[i].prev->multi[0].idx++] = &Q->vec[i];    /* 知道multi的下标 */
    }
    

ee = clock();
cerr<<"it costs : "<<ee - bb<<" us "<<endl;    
        if(fv.multin)
        {
            for(int j = 0;j < fv.multi[0].n;++j)
                cout<<fv.multi[0].vec[j]->einfo<<fv.multi[0].vec[j]->vinfo<<endl;
        }
        
        
}

void Graph::FindInOutNums()
{
    u64 id, realid;
    string type;
    cerr<<"Format : Type Id"<<endl;
    cin>>type>>id;
timeval btime, etime;
gettimeofday(&btime,NULL);
    GetRealId(type, id, realid);
    u64 innums, outnums, st;
    u64 *res = NULL;
    Adj->GetPrevVid(realid, res, innums, st);
    Adj->GetNextVid(realid, res, outnums, st);
    cout<<"InNums: "<<innums<<endl;
    cout<<"OutNums: "<<outnums<<endl;
gettimeofday(&etime,NULL);
{  
    i64 restime =(etime.tv_sec * 1000000 + etime.tv_usec) - (btime.tv_sec * 1000000 + btime.tv_usec);
    cerr<<"Total cost : "<<restime<<"us"<<endl;
}
}

void Graph::threads_work(NorQue* SchQ, PtrQue* ResQ, volatile u64& shidx, volatile u64& stidx, int threads, MutexLock* tasklock, MutexLock* reslock)
{
    u64 price = (stidx - shidx) / threads;
    u64 mod = (stidx - shidx) % threads;
    u64 curwork = 0, sumwork = threads + 1;
    for(u64 i = 0;i < threads;++i)
    {
        NorSch_1* tmp1 = new NorSch_1(SchQ, this, &curwork, tasklock, reslock, ResQ);
        tmp1->st = i * price + shidx;
        tmp1->n = price;
        pool->addwork(tmp1, NorSch_1_Fun);
    }
    NorSch_1* tmp2 = new NorSch_1(SchQ, this, &curwork, tasklock, reslock, ResQ);
    tmp2->st = threads * price + shidx;
    tmp2->n = mod;
    pool->addwork(tmp2, NorSch_1_Fun);
    while(1)
    {
        {
            MutexLockGuard tmut(*tasklock);
            if(sumwork == curwork)
                break;
        }
    }
}

void Graph::threads_work_BI_3(NorQue* SchQ, volatile u64& shidx, volatile u64& stidx, int threads, MutexLock* tasklock, MutexLock* reslock, InArray* vis, GlobalVec* gVec)
{
    u64 price = (stidx - shidx) / threads;
    u64 mod = (stidx - shidx) % threads;
    u64 curwork = 0, sumwork = threads + 1;
    for(u64 i = 0;i < threads;++i)
    {
        BISch_3* tmp1 = new BISch_3(SchQ, this, &curwork, tasklock, vis, gVec);
        tmp1->st = i * price + shidx;
        tmp1->n = price;
        tmp1->id = i;
        pool->addwork(tmp1, BISch_3_Fun);
    }
    BISch_3* tmp2 = new BISch_3(SchQ, this, &curwork, tasklock, vis, gVec);
    tmp2->st = threads * price + shidx;
    tmp2->n = mod;
    tmp2->id = threads;
    pool->addwork(tmp2, BISch_3_Fun);
    while(1)
    {
        {
            MutexLockGuard tmut(*tasklock);
            if(sumwork == curwork)
                break;
        }
    }
}

void Graph::threads_work_BI_10_1(NorQue* SchQ, PtrQue* ResQ, volatile u64& shidx, volatile u64& stidx, int threads, MutexLock* tasklock, MutexLock* reslock, InArray* vis)
{
    u64 price = (stidx - shidx) / threads;
    u64 mod = (stidx - shidx) % threads;
    u64 curwork = 0, sumwork = threads + 1;
    for(u64 i = 0;i < threads;++i)
    {
        BISch_10_1* tmp1 = new BISch_10_1(SchQ, ResQ, this, &curwork, tasklock, reslock, vis);
        tmp1->st = i * price + shidx;
        tmp1->n = price;
        pool->addwork(tmp1, BISch_10_Fun_1);
    }
    BISch_10_1* tmp2 = new BISch_10_1(SchQ, ResQ, this, &curwork, tasklock, reslock, vis);
    tmp2->st = threads * price + shidx;
    tmp2->n = mod;
    pool->addwork(tmp2, BISch_10_Fun_1);
    while(1)
    {
        {
            MutexLockGuard tmut(*tasklock);
            if(sumwork == curwork)
                break;
        }
    }
}

void Graph::threads_ext_tag(unordered_map<u64, string>& tagMap, int threads, MutexLock* tasklock)
{
    u64 total = vkinds["tag"].n;
    u64 start = vkinds["tag"].st;
    u64 price = total / threads;
    u64 mod = total % threads;
    u64 curwork = 0, sumwork = threads + 1; 
    for(u64 i = start;i < start + total;++i)
        tagMap[i] = "";
    for(u64 i = 0;i < threads;++i)
    {
        Ext_Tag_Arg* tmp1 = new Ext_Tag_Arg(&tagMap, this, &curwork, tasklock);
        tmp1->st = i * price + start;
        tmp1->n = price;
        pool->addwork(tmp1, Ext_Tag_Fun);
    }
    Ext_Tag_Arg* tmp2 = new Ext_Tag_Arg(&tagMap, this, &curwork, tasklock);
    tmp2->st = threads * price + start;
    tmp2->n = mod;
    pool->addwork(tmp2, Ext_Tag_Fun);
    while(1)
    {
        {
            MutexLockGuard tmut(*tasklock);
            if(sumwork == curwork)
                break;
        }
    }
}


void Graph::threads_work_BI_10_2(PtrQue* ResQ, int threads, MutexLock* tasklock, InArray* vis, vector<unordered_map<u64, u64> >& ptrPerson)
{
    u64 price = ResQ->tidx / threads;
    u64 mod = ResQ->tidx % threads;
    u64 curwork = 0, sumwork = threads + 1;
    for(u64 i = 0;i < threads;++i)
    {
        BISch_10_2* tmp1 = new BISch_10_2(ResQ, this, &curwork, tasklock, vis, &ptrPerson);
        tmp1->st = i * price;
        tmp1->n = price;
        pool->addwork(tmp1, BISch_10_Fun_2);
    }
    BISch_10_2* tmp2 = new BISch_10_2(ResQ, this, &curwork, tasklock, vis, &ptrPerson);
    tmp2->st = threads * price;
    tmp2->n = mod;
    pool->addwork(tmp2, BISch_10_Fun_2);
    while(1)
    {
        {
            MutexLockGuard tmut(*tasklock);
            if(sumwork == curwork)
                break;
        }
    }
}

void Graph::threads_work_BI_16(int threads, MutexLock* tasklock, InArray* vis, vector<BISch_16_Vec>& personVec, u64 personVecSize, u64 maxKnowsLimit)
{
    u64 price = personVecSize / threads;
    u64 mod = personVecSize % threads;
    u64 curwork = 0, sumwork = threads + 1;
    for(u64 i = 0;i < threads;++i)
    {
        BISch_16* tmp1 = new BISch_16(this, &curwork, tasklock, vis, &personVec);
        tmp1->st = i * price;
        tmp1->n = price;
        tmp1->maxKnowsLimit = maxKnowsLimit;
        pool->addwork(tmp1, BISch_16_Fun);
    }
    BISch_16* tmp2 = new BISch_16(this, &curwork, tasklock, vis, &personVec);
    tmp2->st = threads * price;
    tmp2->n = mod;
    tmp2->maxKnowsLimit = maxKnowsLimit;
    pool->addwork(tmp2, BISch_16_Fun);
    while(1)
    {
        {
            MutexLockGuard tmut(*tasklock);
            if(sumwork == curwork)
                break;
        }
    }
}

void Graph::threads_work_BI_16_2(int threads, MutexLock* tasklock, InArray* visA, InArray* visB, vector<BISch_16_Vec>& personVec, u64 personVecSize, u64 maxKnowsLimit)
{
    u64 price = personVecSize / threads;
    u64 mod = personVecSize % threads;
    u64 curwork = 0, sumwork = threads + 1;
    for(u64 i = 0;i < threads;++i)
    {
        BISch_16_2* tmp1 = new BISch_16_2(this, &curwork, tasklock, visA, visB, &personVec);
        tmp1->st = i * price;
        tmp1->n = price;
        tmp1->maxKnowsLimit = maxKnowsLimit;
        pool->addwork(tmp1, BISch_16_Fun_2);
    }
    BISch_16_2* tmp2 = new BISch_16_2(this, &curwork, tasklock, visA, visB, &personVec);
    tmp2->st = threads * price;
    tmp2->n = mod;
    tmp2->maxKnowsLimit = maxKnowsLimit;
    pool->addwork(tmp2, BISch_16_Fun_2);
    while(1)
    {
        {
            MutexLockGuard tmut(*tasklock);
            if(sumwork == curwork)
                break;
        }
    }
}

void Graph::threads_work_getRealId(const string& type, const string& value, const string& row, u64& realid)
{
    u64 threads = max_threads;
    u64 totalSize = vkinds[type].n;
    u64 start = vkinds[type].st;
    u64 price = totalSize / threads;
    u64 mod = totalSize % threads;
    u64 curwork = 0, sumwork = threads + 1; 
    realid = -1;
    MutexLock *lock = new MutexLock;
    for(u64 i = 0;i < threads;++i){
        Get_Id_Arg* tmp1 = new Get_Id_Arg(&realid, this, &curwork, lock);
        tmp1->st = i * price + start;
        tmp1->n = price;
        tmp1->row = row;
        tmp1->type = type;
        tmp1->value = value;
        pool->addwork(tmp1, Get_Id_Fun);
    }
    Get_Id_Arg* tmp2 = new Get_Id_Arg(&realid, this, &curwork, lock);
    tmp2->st = threads * price + start;
    tmp2->n = mod;
    tmp2->row = row;
    tmp2->type = type;
    tmp2->value = value;
    pool->addwork(tmp2, Get_Id_Fun);
    while(1)
    {
        {
            MutexLockGuard tmut(*lock);
            if(sumwork == curwork)
                break;
        }
    }
    delete lock;

}

void Graph::threads_work_FindFriends(NorQue* SchQ, volatile u64& shidx, volatile u64& stidx, int threads, MutexLock* tasklock, MutexLock* reslock, CondData* condArg, InArray* vis)
{
    /*
        使用不同大小的数组错开抢锁的时机
    */
    u64 price = (stidx - shidx) / threads;
    u64 mod = (stidx - shidx) % threads;
    u64 curwork = 0, sumwork = threads + 1;
    GNode **tmpVec = new GNode*[sumwork];

    u64 *randNums = new u64[sumwork]; 
    for(u64 i = 0;i < sumwork;++i){
        randNums[i] = ARRAY_REDUNDANT + rand() % ARRAY_REDUNDANT;
        tmpVec[i] = new GNode[randNums[i]];
    }
    u64 tmpVexIdx = 0;
    MutexLock vecLock;
    for(u64 i = 0;i < threads;++i){
        FriendFind* tmp1 = new FriendFind(SchQ, this, vis, &curwork, tasklock, &vecLock, tmpVec[i], condArg, randNums[i]);
        tmp1->st = i * price + shidx;
        tmp1->n = price;
        pool->addwork(tmp1, FriendFind_Fun);
    }
    FriendFind* tmp2 = new FriendFind(SchQ, this, vis, &curwork, tasklock, &vecLock, tmpVec[threads], condArg, randNums[threads]);
    tmp2->st = threads * price + shidx;
    tmp2->n = mod;
    pool->addwork(tmp2, FriendFind_Fun);
    while(1)
    {
        {
            MutexLockGuard tmut(*tasklock);
            if(sumwork == curwork)
                break;
        }
    }
    for(u64 i = 0;i < sumwork;++i){
        if(tmpVec[i] != NULL)
            delete tmpVec[i];
    }
}

void Graph::PathTracing(PtrQue *ResQ)
{
    /* 第一次遍历计算数量，只有在结果集中的才会被分配内存 */
    for(i64 j = ResQ->tidx - 1;j >= 0;--j)
    {
        if(ResQ->ptrv[j]->prev != NULL && ResQ->ptrv[j]->prev->multi != NULL)
            ++ResQ->ptrv[j]->prev->multi[ResQ->ptrv[j]->previdx].n; /* 知道multi的下标 */
    }

    /* 开辟空间 */
    for(i64 i = 0;i < ResQ->tidx;++i)
    {
        if(ResQ->ptrv[i]->multin != 0)
        {
            for(u64 j = 0;j < ResQ->ptrv[i]->multin;++j)
                ResQ->ptrv[i]->multi[j].vec = new GNode*[ResQ->ptrv[i]->multi[j].n];
        } 
    }
    /* 连接 */

    for(i64 i = 0;i < ResQ->tidx;++i)
    {
        if(ResQ->ptrv[i]->prev != NULL && ResQ->ptrv[i]->prev->multi != NULL)
            ResQ->ptrv[i]->prev->multi[ResQ->ptrv[i]->previdx].vec[ResQ->ptrv[i]->prev->multi[ResQ->ptrv[i]->previdx].idx++] = ResQ->ptrv[i];    /* 知道multi的下标 */

    }

}

void Graph::GetRowInfo(string& res, const string& head, const string& src, const string& type)
{
    Bigram t = vkinds[type].hmap[head];
    u64 st = 0, end, len = src.length(), cnt = 1;
    if(res == "")
        res.reserve(32);
    for(;st < len;++st)
    {
        if(src[st] == '|')
            ++cnt;
        else if(cnt == t.v1)
            res += src[st];
        else if(cnt > t.v1)
            break;
    }
}

string Graph::GetRowInfo(const string& head, const string& src, const string& type)
{
    Bigram t = vkinds[type].hmap[head];
    u64 st = 0, end, len = src.length(), cnt = 1;
    string res("");
    res.reserve(32);
    for(;st < len;++st)
    {
        if(src[st] == '|')
            ++cnt;
        else if(cnt == t.v1)
            res += src[st];
        else if(cnt > t.v1)
            break;
    }
    return res;
}

void Graph::MultiFree(PtrQue *ResQ)
{
    for(i64 i = 0;i < ResQ->tidx;++i)
    {
        if(ResQ->ptrv[i]->multin != 0)
        {
            for(u64 j = 0;j < ResQ->ptrv[i]->multin;++j)
                    delete [] ResQ->ptrv[i]->multi[j].vec;
            delete [] ResQ->ptrv[i]->multi;
        } 
    }
}

void Graph::GNodeClear(GNode* vec, u64 size)
{
    for(u64 i = 0;i < size;++i){
        vec[i].cond = NULL;
        vec[i].effe = 0;
        vec[i].eid = 0;
        vec[i].einfo.resize(0);
        vec[i].level = 0;
        vec[i].multi = NULL;
        vec[i].multin = 0;
        vec[i].oldid = 0;
        vec[i].prev = NULL;
        vec[i].previdx = 0;
        vec[i].row = 0;
        vec[i].valid = 0;
        vec[i].vid = 0;
        vec[i].vinfo.resize(0);
        vec[i].vtype.resize(0);
    }
}

void Graph::DisplayDistance(GNode* x, string& res)
{
    if(x->prev->prev != NULL)
        DisplayDistance(x->prev, res);
    res = res + to_string(x->prev->oldid) + '\t' + x->prev->vtype + '\t' + x->einfo; 
}



void Graph::PrintHelp()
{
    cerr<<"Pls Select Search Mod:"<<endl;

    cerr<<'\t'<<"1.IC 1"<<endl;
    cerr<<'\t'<<"2.IC 2"<<endl;
    cerr<<'\t'<<"3.IC 5"<<endl;
    cerr<<'\t'<<"4.IC 9"<<endl;
    cerr<<'\t'<<"5.IC 13"<<endl;

    cerr<<'\t'<<"11.Get Vertex Info"<<endl;
    cerr<<'\t'<<"12.BFS"<<endl;
    cerr<<'\t'<<"13.Get Vertex In/Out Count"<<endl;
}


Graph::~Graph()
{
    if(Vertex_csa != NULL)
    {
        for(u64 i = 0;i < Vcsa_cnt;++i)
            delete Vertex_csa[i];
    }
    if(Edge_csa != NULL)
    {
        for(u64 i = 0;i < Ecsa_cnt;++i)
            delete Edge_csa[i];
    }
    if(Adj != NULL)
        delete Adj;
    if(hasdes != NULL)
        delete hasdes;
    if(etype != NULL)
        delete etype;
    if(pool != NULL)
    {
        pool->destroy_pool();
        delete pool;
    }
}

template<typename F, typename ...ArgTs>
double MeasureTime(F &&f, ArgTs &&...args) {
    auto start = std::chrono::high_resolution_clock::now();
    std::invoke(std::forward<F>(f), std::forward<ArgTs>(args)...);
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start).count();
}

[[nodiscard]] inline std::unique_ptr<Benchmark> FindBenchmark(const std::string &query_name) {
    if (query_name == "bfs")
        return std::make_unique<ParallelBfsBenchmark>(par::get_max_threads() - 1 /*16*/);
    else if (query_name == "serial_bfs")
        return std::make_unique<SerialBfsBenchmark>();
    else if (query_name == "page_rank")
        return std::make_unique<ParallelPageRankBenchmark>(par::get_max_threads() - 1);
    else if (query_name == "serial_page_rank")
        return std::make_unique<SerialPageRankBenchmark>();
    else if (query_name == "bi2") 
        return std::make_unique<BI2Benchmark>();
    else if (query_name == "bi3")
        return std::make_unique<BI3Benchmark>();
    else if (query_name == "bi12")
        return std::make_unique<BI12Benchmark>();
    else if (query_name == "bi19")
        return std::make_unique<BI19Benchmark>();
    else if (query_name == "ic2")
        return std::make_unique<IC2Benchmark>();
    else if (query_name == "ic9")
        return std::make_unique<IC9Benchmark>();
    else if (query_name == "ic10")
        return std::make_unique<IC10Benchmark>();
    return nullptr;
}

void Graph::TestRun(const std::string &query_name, const string &input_filename, 
                    const string &output_filename, size_t max_num_cases, 
                    const std::string &tmp_dir, bool print) {
    //oneapi::tbb::global_control gc(
    //    oneapi::tbb::global_control::max_allowed_parallelism, 16
    //);

    double result_prepare_time = 0.0, result_total_time = 0.0, result_avg_time = 0.0;
    size_t num_cases = 0;

    if (auto benchmark = FindBenchmark(query_name); benchmark) {
        auto input_format = benchmark->get_input_format();

        auto [prepare_time, total_time, avg_time, num] = 
            run_benchmark(*this, *benchmark, input_filename, tmp_dir, input_format.has_header, input_format.sep, max_num_cases);
        result_prepare_time = prepare_time;
        result_total_time = total_time; 
        result_avg_time = avg_time; 
        num_cases = num;
        //std::cout << "num rows: " << benchmark->get_result_table().num_rows() << "\n";

        if (true || print) {
            benchmark->get_result_table().print();
        }

    } else { // fallback to old code
        std::unordered_map<std::string, int> query_map {
            {"khops", 3},
            {"ic1", 4},
            {"ic5", 6},
            {"ic13", 8},
            {"bi3", 11},
            {"bi10", 12},
            {"bi16", 13}  
        };

        int mod = query_map[query_name];
        max_threads = par::get_max_threads() - 1;
        //max_threads = 1;
        //max_threads = 32;
        pool = new threadpool(max_threads);
        pool->creat_thread();
        /* 接收连接命令 (待实现) */
        /* 每建立一个连接就创建一个线程去服务 (待实现) */
        u64 Q_size = MAX_QUEUE_SIZE;
        Que[0] = new UserData;

        Que[0]->SchQ = new NorQue;
        Que[0]->SchQ->vec = new GNode[Q_size];
        memset(Que[0]->SchQ->vec, 0 , Q_size * sizeof(GNode));

        Que[0]->ResQ = new PtrQue;
        Que[0]->ResQ->ptrv = new GNode*[Q_size];
        memset(Que[0]->ResQ->ptrv, 0 , Q_size * sizeof(GNode*));

        Que[0]->tasklock = new MutexLock;
        Que[0]->reslock = new MutexLock;

        Que[0]->vis = new InArray(total_v, 1);

        u64 globalVecSize = GLOBAL_VEC_SIZE, globalTVecSize = GLOBAL_TVEC_SIZE;
        Que[0]->gVec = new GlobalVec();
        Que[0]->gVec->vec = new u64[globalVecSize];
        memset(Que[0]->gVec->vec, 0, sizeof(u64) * globalVecSize);
        Que[0]->gVec->tVec = (u64**)malloc(sizeof(u64*) * (max_threads + 1));
        for(u64 i = 0;i < max_threads + 1;++i){
            Que[0]->gVec->tVec[i] = new u64[globalTVecSize];
            memset(Que[0]->gVec->tVec[i], 0, sizeof(u64) * globalTVecSize);
        }

        std::ifstream input(input_filename, std::ios::in);
        std::string line;
        while (std::getline(input, line)) {
            double time = MeasureTime([this](int mod, const std::string &line) {
                TestSearch(mod, line);
            }, query_map[query_name], line);
            result_total_time += time;

            Que[0]->vis->ReSet();
            if((mod >= 3 && mod <= 7) || (mod >= 11 && mod <= 13)) {
                GNodeClear(Que[0]->SchQ->vec, Que[0]->SchQ->tidx);
                if(mod == 11){
                    for(u64 i = 0; i < max_threads + 1; ++i) {
                        memset(Que[0]->gVec->tVec[i], 0, sizeof(u64) * globalTVecSize);
                    }
                }
            }
            else if(mod == 8) {
                memset(Que[0]->gVec->vec, 0, sizeof(u64) * globalVecSize);
                for(u64 i = 0; i < max_threads + 1; ++i){
                    memset(Que[0]->gVec->tVec[i], 0, sizeof(u64) * globalTVecSize);
                }
            }

            ++num_cases;
            if (num_cases >= max_num_cases) 
                break;
        }
        result_avg_time = result_total_time / static_cast<double>(num_cases);
    }

    std::stringstream sstream;
    sstream << "query [" << query_name << "] "
            << num_cases << " cases, " 
            << std::fixed << std::setprecision(3) 
            << "prepare time: " << result_prepare_time << "ms, "
            << "running time: " << result_total_time << "ms, "
            << "avg running time: " << result_avg_time << "ms, "
            << "total time: " << result_total_time + result_prepare_time << "ms, "
            << "avg time: " << (result_total_time + result_prepare_time) / num_cases << "ms\n";
    std::string msg = sstream.str();
    std::cerr << msg;

    std::ofstream output(output_filename, std::ios::out | std::ios::app);
    output << msg;
    output.close();
}

void Graph::TestSearch(int mod, string obj)
{
    switch (mod)
    {
        case 1 : Search_IC_0(obj);
            break;
        case 2 : KHops(obj);
            break;
        case 3 : KHopsFriends(obj);
            break;
        case 4 : Search_IC_1(obj);
            break;
        case 5 : Search_IC_2(obj);
            break;
        case 6 : Search_IC_5(obj);
            break;
        case 7 : Search_IC_9(obj);
            break;
        case 8 : Search_IC_13(obj);
            break;
        case 9 : NormalSearch_threads(obj);
            break;
        case 10 : NormalSearch_InOut_threads(obj);
            break;
        case 11 : Search_BI_3(obj);
            break;
        case 12 : Search_BI_10(obj);
            break;
        case 13 : Search_BI_16_2(obj);
            break;
        case 50 : TypeExtTest(obj);
            break;
        case 51 : TypeLocTest(obj);
            break;
    }
}

void Graph::Search_IC_0(string obj)
{
    u64 id, realid;
    string type;
    type = obj.substr(0, obj.find_first_of(' '));
    id = strtoull(obj.substr(obj.find_first_of(' ') + 1).c_str(), NULL, 10);
    GetRealId(type, id, realid);
    string res("");
    GetVInfo(res, realid);
    cout<<res<<endl;
}

void Graph::Search_IC_1(string obj)
{
    u64 id, realid, jns = 3;
    string firstname, type("person");

    u64 threads = max_threads;
    volatile u64 sumwork = threads + 1, curwork = 0;

    id = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    firstname = obj.substr(obj.find_first_of(' ') + 1);

    u64 nums, st, i;
    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    InArray *vis = Que[0]->vis;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64* res = NULL;
    /* 
    GetRealId(type, id, realid);

    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    u64 condst = vkinds[type].st, condend = vkinds[type].n + condst; 
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]) && res[i] >= condst && res[i] < condend)
        {
            SchQ->vec[stidx].vid = res[i];
            SchQ->vec[stidx].eid = st;
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
            SchQ->vec[stidx].valid = 1;
            SchQ->vec[stidx].previdx = 0;
            SchQ->vec[stidx].cond.vobj = firstname;
            SchQ->vec[stidx].cond.vop = 0;
            SchQ->vec[stidx].cond.vdk = vkinds[type].hmap["firstname"];
            SchQ->vec[stidx].cond.eneed = 0;
            SchQ->vec[stidx].cond.vneed = 1;
            ++stidx;
            vis->SetValue(res[i], 1);
        }
        ++st;
    }
    
    
    while(shidx < stidx)
    {
        GNode &t = SchQ->vec[shidx];     
        ++shidx;
        if(t.level >= jns)
            break;
        Adj->GetNextVid(t.vid, res, nums, st);
        for(i = 0;i < nums;++i)
        {
            if(!vis->GetValue(res[i]) && res[i] >= condst && res[i] < condend)
            {
                GNode& tnode = SchQ->vec[stidx];
                tnode.vid = res[i];
                tnode.eid = st;
                tnode.prev = NULL;
                tnode.level = t.level + 1;
                tnode.valid = 1; // 1是找到的熟人 
                tnode.previdx = 0;
                tnode.cond.vobj = firstname;
                tnode.cond.vop = 0;
                tnode.cond.vdk = vkinds[type].hmap["firstname"];
                tnode.cond.vneed = 1;
                tnode.cond.eneed = 0;

                ++stidx;
                vis->SetValue(res[i], 1);
            }
            ++st;
        }
    }   
    */
    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->vobj = firstname;
    condArg_1->vop = 0;
    condArg_1->vdk = vkinds[type].hmap["firstname"];
    condArg_1->eneed = 0;
    condArg_1->vneed = 1;
    condArg_1->edsp = 0;

    shared_ptr<CondData> condArg_2 = make_shared<CondData>();
    condArg_2->eneed = 0;
    condArg_2->vneed = 1;
    condArg_2->edsp = 0;
    shared_ptr<CondData> condArg_3 = make_shared<CondData>();
    condArg_3->etype = "studyat";
    condArg_3->vop = 0;
    condArg_3->vobj = "university";
    condArg_3->vdk = vkinds["organisation"].hmap["type"];
    condArg_3->eneed = 1;
    condArg_3->vneed = 1;
    condArg_3->edsp = 1;
    shared_ptr<CondData> condArg_4 = make_shared<CondData>();
    condArg_4->etype = "workat";
    condArg_4->vop = 0;
    condArg_4->vobj = "company";
    condArg_4->vdk = vkinds["organisation"].hmap["type"];  
    condArg_4->eneed = 1;
    condArg_4->vneed = 1;
    condArg_4->edsp = 1;

    u64 noUsed;
    FindFriendsFun(id, condArg_1.get(), 1, jns, noUsed);
    shidx = 0;

    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);



    sort(ResQ->ptrv, ResQ->ptrv + ResQ->tidx, [&](GNode *x, GNode* y)
                                                {
                                                    if(x->level != y->level) return x->level < y->level;
                                                    int flag = strcmp(GetRowInfo("lastname", x->vinfo, x->vtype).c_str(), GetRowInfo("lastname", y->vinfo, y->vtype).c_str());
                                                    if(flag != 0)
                                                        return flag < 0;
                                                    return strtoul(GetRowInfo("id", x->vinfo, x->vtype).c_str(), NULL, 10) < strtoul(GetRowInfo("id", y->vinfo, y->vtype).c_str(), NULL, 10);
                                                });
    ResQ->tidx = ResQ->tidx > 20 ? 20 : ResQ->tidx;

    u64 rows = ResQ->tidx - ResQ->hidx;

    shidx = stidx;
    curwork = 0;
    while(ResQ->hidx < ResQ->tidx)
    {
        GNode* t = ResQ->ptrv[ResQ->hidx];
        ++ResQ->hidx;
        t->multin = 2;
        t->multi = new MultiGNode[t->multin];

        Adj->GetNextVid(t->vid, res, nums, st);
        u64 st1 = vkinds["place"].st, end1 = st1 + vkinds["place"].n;
        u64 st2 = vkinds["organisation"].st, end2 = st2 + vkinds["organisation"].n;
        for(u64 i = 0;i < nums;++i)
        {
            if(res[i] >= st1 && res[i] < end1)
            {
                /* person islocatedin city */
                GNode& tt = SchQ->vec[stidx];

                tt.vid = res[i];
                tt.eid = st;
                tt.level = t->level + 1;
                tt.prev = t;
                tt.valid = 2;
                tt.previdx = 0;
                tt.cond = condArg_2.get();
                // tt.cond.eneed = 0;
                // tt.cond.vneed = 1;
                ++stidx;
            }
            else if(res[i] >= st2 && res[i] < end2)
            {
                /* person workat organisation */
                GNode& tt1 = SchQ->vec[stidx];
                tt1.vid = res[i];
                tt1.eid = st;
                tt1.level = t->level + 1;
                tt1.prev = t;
                tt1.valid = 3;
                tt1.previdx = 1;

                GNode& tt2 = SchQ->vec[stidx + 1];
                tt2.vid = res[i];
                tt2.eid = st;
                tt2.level = t->level + 1;
                tt2.prev = t;
                tt2.valid = 3;
                tt2.previdx = 1;

                /* person 和 organisation 有多个关系，但对place顶点上的信息没有要求 */
                tt1.cond = condArg_3.get();
                // tt1.cond.etype = "studyat";
                // tt1.cond.vop = 0;
                // tt1.cond.vobj = "university";
                // tt1.cond.vdk = vkinds["organisation"].hmap["type"];
                // tt1.cond.eneed = 1;
                // tt1.cond.vneed = 1;
                tt2.cond = condArg_4.get();
                // tt2.cond.etype = "workat";
                // tt2.cond.vop = 0;
                // tt2.cond.vobj = "company";
                // tt2.cond.vdk = vkinds["organisation"].hmap["type"];  
                // tt2.cond.eneed = 1;
                // tt2.cond.vneed = 1;

                stidx += 2;
            }
            ++st;
        }
    }

    // threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);
    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

    shidx = stidx;
    curwork = 0;
    while(ResQ->hidx < ResQ->tidx)
    {
        GNode* t = ResQ->ptrv[ResQ->hidx];
        ++ResQ->hidx;
        if(t->valid == 3)
        {
            t->multin = 1;
            t->multi = new MultiGNode[t->multin];

            Adj->GetNextVid(t->vid, res, nums, st);
            u64 st1 = vkinds["place"].st, end1 = st1 + vkinds["place"].n;
            for(i = 0;i < nums;++i)
            {
                if(res[i] >= st1 && res[i] < end1)
                {
                    /* organisation islocatedin city */
                    GNode& tt = SchQ->vec[stidx];

                    tt.vid = res[i];
                    tt.eid = st;
                    tt.level = t->level + 1;
                    tt.prev = t;
                    tt.valid = 4;
                    tt.previdx = 0;
                    tt.cond = condArg_2.get();
                    // tt.cond.eneed = 0;
                    // tt.cond.vneed = 1;

                    /* organisation 和 place 只有islocatedin，并且对place顶点上的信息也没有要求 */
                    ++stidx;
                }
                ++st;
            }
        }
    }

    // threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);
    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

    PathTracing(ResQ);

///////////////////////////////////////////////////////////////////////////8333 Rahul
//结点指明行的位置
//id, lastname, level, birthday, creationdate, gender, browserused, 
//locationIP, email, speak,
//city.name, unversisity.name|classyear|city.name, 
//company.name|workyear|country.name

//orderby level great; lastname great; id great;
//limit 20
    /* 打印表头 */
    cout<<"otherPerson.id|otherPerson.lastName|distanceFromPerson|otherPerson.birthday|\
otherPerson.creationDate|otherPerson.gender|otherPerson.browserUsed|\
otherPerson.locationIP|otherPerson.email|otherPerson.speaks|locationCity.name|\
universities|companies"<<endl;
    struct data
    {
        string id = "";
        string lastname = "";
        string distanceFromPerson = "";
        string birthday = "";
        string creationdate = "";
        string gender = "";
        string browserused = "";
        string locationip = "";
        string email = "";
        string speaks = "";
        string locationCityname = "";
        string universities = "";
        string companies = "";
    };
    vector<data> table(rows);
    for(u64 i = 0;i < rows;++i)
    {
        GNode *t = ResQ->ptrv[i];
        data *tbl = &table[i];
        GetRowInfo(tbl->id, "id", t->vinfo, t->vtype);
        GetRowInfo(tbl->lastname, "lastname", t->vinfo, t->vtype);
        tbl->distanceFromPerson = to_string(t->level);
        GetRowInfo(tbl->birthday, "birthday", t->vinfo, t->vtype);
        GetRowInfo(tbl->creationdate, "creationdate", t->vinfo, t->vtype);
        GetRowInfo(tbl->gender, "gender", t->vinfo, t->vtype);
        GetRowInfo(tbl->browserused, "browserused", t->vinfo, t->vtype);
        GetRowInfo(tbl->locationip, "locationip", t->vinfo, t->vtype);
        GetRowInfo(tbl->email, "email", t->vinfo, t->vtype);
        GetRowInfo(tbl->speaks, "language", t->vinfo, t->vtype);
        for(u64 j = 0;j < t->multin;++j)
        {
            for(u64 l = 0;l < t->multi[j].n;++l)
            {
                GNode *tt = t->multi[j].vec[l];
                if(j == 0)
                    GetRowInfo(tbl->locationCityname, "name", tt->vinfo, tt->vtype);
                if(j == 1) // org
                {
                    if(tt->einfo.substr(0, tt->einfo.find_first_of('|')) == "studyat")
                    {
                        if(tbl->universities != "")
                            tbl->universities += ';';
                        GetRowInfo(tbl->universities, "name", tt->vinfo, tt->vtype);
                        tbl->universities += ',';
                        tbl->universities += tt->einfo.substr(tt->einfo.find_first_of('|') + 1);
                        tbl->universities += ',';
                        GetRowInfo(tbl->universities, "name", tt->multi[0].vec[0]->vinfo, tt->multi[0].vec[0]->vtype);
                    }
                    else 
                    {
                        if(tbl->companies!= "")
                            tbl->companies += ';';
                        GetRowInfo(tbl->companies, "name", tt->vinfo, tt->vtype);
                        tbl->companies += ',';
                        tbl->companies += tt->einfo.substr(tt->einfo.find_first_of('|') + 1);
                        tbl->companies += ',';
                        GetRowInfo(tbl->companies, "name", tt->multi[0].vec[0]->vinfo, tt->multi[0].vec[0]->vtype);
                    }
                }
            }
        }
    }

    MultiFree(ResQ);

    /* 排序部分 */
    // sort(table.begin(), table.end(), [](data& x, data& y)
    //     {
    //         if(x.distanceFromPerson != y.distanceFromPerson)
    //             return x.distanceFromPerson < y.distanceFromPerson;
    //         else if(x.lastname != y.lastname)
    //             return x.lastname < y.lastname;
    //         else
    //             return strtoul(x.id.c_str(), NULL, 10) < strtoul(y.id.c_str(), NULL, 10);
    //     });
    
    
    
    // sort(table.begin(), table.end(), [](data& x, data& y){return x.lastname < y.lastname;});
    // sort(table.begin(), table.end(), [](data& x, data& y){return strtoul(x.id.c_str(), NULL, 10) < strtoul(y.id.c_str(), NULL, 10);});

    for(u64 i = 0;i < rows && i < 20;++i)
        cout<<table[i].id<<'|'<<table[i].lastname<<'|'<<table[i].distanceFromPerson<<'|'<<table[i].birthday
            <<'|'<<table[i].creationdate<<'|'<<table[i].gender<<'|'<<table[i].browserused<<'|'<<table[i].locationip
            <<'|'<<table[i].email<<'|'<<table[i].speaks<<'|'<<table[i].locationCityname<<'|'<<table[i].universities
            <<'|'<<table[i].companies<<endl;
    if(res) delete [] res;
}

void Graph::Search_IC_2(string obj)
{
    u64 id, realid, jns = 1;
    string datetime, type("person");
    
    u64 threads = max_threads;

    id = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    datetime = obj.substr(obj.find_first_of(' ') + 1);
   
    u64 nums, st, i;
    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    InArray *vis = Que[0]->vis;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64* res = NULL;
    /* 
    GetRealId(type, id, realid);
    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    u64 condst = vkinds[type].st, condend = vkinds[type].n + condst; 
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]) && res[i] >= condst && res[i] < condend)
        {
            SchQ->vec[stidx].vid = res[i];
            SchQ->vec[stidx].eid = st;
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
            SchQ->vec[stidx].valid = 1;
            SchQ->vec[stidx].previdx = 0;
    
            SchQ->vec[stidx].cond.eneed = 0;
            SchQ->vec[stidx].cond.vneed = 1;

            ++stidx;
            vis->SetValue(res[i], 1);
        }
        ++st;
    }
    */

    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->eneed = 0;
    condArg_1->vneed = 1;
    condArg_1->edsp = 0;

    u64 noUsed;
    FindFriendsFun(id, condArg_1.get(), 1, 1, noUsed);

    shidx = 0;

    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);
    shidx = stidx;

    shared_ptr<CondData> condArg_2 = make_shared<CondData>();
    condArg_2->eneed = 0;
    condArg_2->vneed = 1;
    condArg_2->vop = -1;
    condArg_2->vobj = datetime;
    condArg_2->edsp = 0;
    condArg_2->vdk = vkinds["comment"].hmap["creationdate"];

    shared_ptr<CondData> condArg_3 = make_shared<CondData>();
    condArg_3->eneed = 0;
    condArg_3->vneed = 1;
    condArg_3->vop = -1;
    condArg_3->edsp = 0;
    condArg_3->vobj = datetime;
    condArg_3->vdk = vkinds["post"].hmap["creationdate"];

    while(ResQ->hidx < ResQ->tidx)
    {
        GNode* t = ResQ->ptrv[ResQ->hidx];
        ++ResQ->hidx;
        t->multin = 1;
        t->multi = new MultiGNode[t->multin];
        /* 找post和comment */
        Adj->GetPrevVid(t->vid, res, nums, st);
        u64 st1 = vkinds["comment"].st, end1 = st1 + vkinds["comment"].n;
        u64 st2 = vkinds["post"].st, end2 = st2 + vkinds["post"].n;
        for(i = 0;i < nums;++i)
        {
            if(res[i] >= st1 && res[i] < end1) // comment
            {
                GNode& tt = SchQ->vec[stidx];
                tt.vid = res[i];
                tt.level = t->level + 1;
                tt.prev = t;
                tt.valid = 2;
                tt.previdx = 0;
                
                tt.cond = condArg_2.get();
                // tt.cond.eneed = 0;
                // tt.cond.vneed = 1;
                // tt.cond.vop = -1;
                // tt.cond.vobj = datetime;
                // tt.cond.vdk = vkinds["comment"].hmap["creationdate"];
                ++stidx;
            }
            else if(res[i] >= st2 && res[i] < end2) // post
            {
                GNode& tt = SchQ->vec[stidx];
                tt.vid = res[i];
                tt.level = t->level + 1;
                tt.prev = t;
                tt.valid = 2;
                tt.previdx = 0;
                
                tt.cond = condArg_3.get();
                // tt.cond.eneed = 0;
                // tt.cond.vop = -1;
                // tt.cond.vneed = 1;
                // tt.cond.vobj = datetime;
                // tt.cond.vdk = vkinds["post"].hmap["creationdate"];
                ++stidx;
            }
        }
    }

    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

    /* 路径追溯代码 */
    PathTracing(ResQ);
///////////////////////////////////////////////////////////////////////////
//id, firstname, lastname, message.id, message.content/imagefile, message.creationdate
//message.creationdate less, message.id great
//limit 20
    /* 打印表头 */
    cout<<"friend.id|friend.firstName|friend.lastName|message.id|message.content/imageFile|creationDate"<<endl;
    struct data
    {
        string id = "";
        string firstName = "";
        string lastName = "";
        string mid = "";
        string content = "";
        string date = "";
    };
    u64 rows = 0;
    for(u64 i = 0;i < ResQ->tidx;++i)
        if(ResQ->ptrv[i]->valid == 2)
            ++rows;
    vector<data> table(rows);
    rows = 0;

    for(u64 i = 0;i < ResQ->tidx;++i)
    {
        GNode *t = ResQ->ptrv[i];
        if(t->valid != 1)
            break;
        for(u64 j = 0;j < t->multin;++j)
        {
            data tbl;
            GetRowInfo(tbl.id, "id", t->vinfo, t->vtype);
            GetRowInfo(tbl.firstName, "firstname", t->vinfo, t->vtype);
            GetRowInfo(tbl.lastName, "lastname", t->vinfo, t->vtype);
            for(u64 l = 0;l < t->multi[j].n;++l)
            {
                GNode *tt = t->multi[j].vec[l];
                table[rows] = tbl;
                GetRowInfo(table[rows].mid, "id", tt->vinfo, tt->vtype);
                if(tt->vtype == "post")
                    GetRowInfo(table[rows].content, "imagefile", tt->vinfo, tt->vtype);
                if(tt->vtype == "comment")
                    GetRowInfo(table[rows].content, "content", tt->vinfo, tt->vtype);
                GetRowInfo(table[rows].date, "creationdate", tt->vinfo, tt->vtype);
                ++rows;
            }
        }
    }
    MultiFree(ResQ);
    sort(table.begin(), table.end(), [](data& x, data& y)
        {
            if(x.date != y.date)
                return x.date > y.date;
            else
                return x.mid < y.mid;
        });
    // sort(table.begin(), table.end(), [](data& x, data& y){return x.mid < y.mid;});

    for(u64 i = 0;i < rows && i < 20;++i)
       cout<<table[i].id<<'|'<<table[i].firstName<<'|'<<table[i].lastName<<'|'<<table[i].mid<<'|'<<table[i].content<<'|'<<table[i].date<<endl; 

}

void Graph::Search_IC_5(string obj)
{
    u64 id, realid;
    string datetime, type("person");
    
    u64 threads = max_threads;

    id = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    datetime = obj.substr(obj.find_first_of(' ') + 1);

    char buf[128];
    time_t xx = stoull(datetime)/1000;
    tm *yy = gmtime(&xx);
    strftime(buf, sizeof(buf), "%FT%T.000%z", yy);
    datetime = buf;
  
    u64 nums, st, i;
    u64 nums2, st2;
    u64 nums3, st3;
    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    InArray *vis = Que[0]->vis;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64 *res = NULL, *res2 = NULL, *res3 = NULL;
    /* 
    GetRealId(type, id, realid);
    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    */
    u64 start1 = vkinds[type].st, end1 = vkinds[type].n + start1; //person
    u64 start2 = vkinds["forum"].st, end2 = vkinds["forum"].n + start2; //forum
    u64 start3 = vkinds["post"].st, end3 = vkinds["post"].n + start3; // post

    shared_ptr<CondData> condArg_0 = make_shared<CondData>();
    condArg_0->vneed = 0;
    condArg_0->eneed = 0;
    condArg_0->edsp = 0;
    u64 noUsed;
    FindFriendsFun(id, condArg_0.get(), 1, 2, noUsed);
    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->vneed = 1;
    condArg_1->eneed = 1;
    condArg_1->eop = 1;
    condArg_1->edsp = 1;
    condArg_1->eobj = datetime;
    condArg_1->etype = "hasmember";
    condArg_1->edk = ekinds["forum->person"].detail["hasmember"].hmap["joindate"];

    shidx = 0;
    u64 tail = stidx, resIdx = 0;

    while(shidx < tail){
        u64 nowHead = stidx;
        u64 nowMax = tail > shidx + IC_5_PERSON_MAX ? IC_5_PERSON_MAX : tail - shidx;
        for(u64 p = 0;p < nowMax;++p){
            GNode& t = SchQ->vec[shidx];
            ++shidx;
            Adj->GetPrevVid(t.vid, res, nums, st);
            for(u64 j = 0;j < nums;++j){
                if(!vis->GetValue(res[j]) && res[j] >= start2 && res[j] < end2){
                    Adj->GetNextVid(res[j], res2, nums2, st2);
                    u64 k = 0;
                    while(res2[k] != t.vid){
                        ++k;
                        ++st2;
                    }
                    GNode &tt = SchQ->vec[stidx];
                    tt.vid = res[j];
                    tt.eid = st2;
                    tt.prev = NULL;
                    tt.valid = 1;

                    tt.cond = condArg_1.get();
                    ++stidx;
                }
            }
        }

        threads_work(SchQ, ResQ, nowHead, stidx, threads, Que[0]->tasklock, Que[0]->reslock);   
        for(;resIdx < ResQ->tidx;++resIdx)
            if(!vis->GetValue(ResQ->ptrv[resIdx]->vid))
                vis->SetValue(ResQ->ptrv[resIdx]->vid, 1);
    }

    u64 leftIdx = -1, rightIdx = 0;
    for(;rightIdx < ResQ->tidx;++rightIdx){
        if(leftIdx == -1 || vis->GetValue(ResQ->ptrv[rightIdx]->vid)){
            ResQ->ptrv[++leftIdx] = ResQ->ptrv[rightIdx];
            vis->SetBitZero(ResQ->ptrv[rightIdx]->vid);
        }
    }
    
    ResQ->tidx = leftIdx + 1;

    shidx = stidx;

    u64 price = ResQ->tidx / threads;
    u64 mod = ResQ->tidx % threads;
    u64 curwork = 0, sumwork = threads + 1;
    for(u64 i = 0;i < threads;++i)
    {
        IC_5_Arg *tmp = new IC_5_Arg;
        tmp->ResQ = ResQ;
        tmp->ptr = this;
        tmp->adj = Adj;
        tmp->vis = vis;
        tmp->st = i * price;
        tmp->n = price;
        tmp->start1 = start1;
        tmp->start3 = start3;
        tmp->end3 = end3;
        tmp->tasklock = Que[0]->tasklock;
        tmp->cnt = &curwork;
        pool->addwork(tmp, IC_5_Fun);
    }
    IC_5_Arg *tmp = new IC_5_Arg;
    tmp->ResQ = ResQ;
    tmp->ptr = this;
    tmp->adj = Adj;
    tmp->vis = vis;
    tmp->st = threads * price;
    tmp->n = mod;
    tmp->start1 = start1;
    tmp->start3 = start3;
    tmp->end3 = end3;
    tmp->tasklock = Que[0]->tasklock;
    tmp->cnt = &curwork;
    pool->addwork(tmp, IC_5_Fun);
    while(1)
    {
        {
            MutexLockGuard tmut(*Que[0]->tasklock);
            if(sumwork == curwork)
                break;
        }
    }
///////////////////////////////////////////////////////////////////////////
//forum.title, post.count
//message.creationdate less, message.id great
//limit 20

    cout<<"forum.title|postCount"<<endl;

    vector<BigramS> table(ResQ->tidx);
    for(i = 0;i < ResQ->tidx;++i)
    {
        string tmp("");
        GetRowInfo(table[i].s, "title", ResQ->ptrv[i]->vinfo, ResQ->ptrv[i]->vtype);
        GetRowInfo(tmp, "id", ResQ->ptrv[i]->vinfo, ResQ->ptrv[i]->vtype);
        table[i].v1 = strtoul(tmp.c_str(), NULL, 10);
        table[i].v2 = ResQ->ptrv[i]->row;
    }
    MultiFree(ResQ);
    sort(table.begin(), table.end(), [](BigramS& x, BigramS& y)
        {
            if(x.v2 != y.v2)
                return x.v2 > y.v2;
            else
                return x.v1 < y.v1;
        }
        );

    for(i = 0;i < ResQ->tidx && i < 20;++i)
        cout<<table[i].s<<'|'<<table[i].v2<<endl;
    if(res) delete [] res;
    if(res2) delete [] res2;
    if(res3) delete [] res3;
}

void Graph::Search_IC_9(string obj)
{
    u64 id, realid, jns = 1;
    string datetime, type("person");
    u64 threads = max_threads;

    id = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    datetime = obj.substr(obj.find_first_of(' ') + 1);

    u64 nums, st;
    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    InArray *vis = Que[0]->vis;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64* res = NULL;
    u64 nums_2, st_2;
    u64 *res_2 = NULL;

    /*
    GetRealId(type, id, realid);
    vis->SetValue(realid, 1);
    
    Adj->GetNextVid(realid, res, nums, st);
    u64 condst = vkinds[type].st, condend = vkinds[type].n + condst; 
    for(u64 i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]) && res[i] >= condst && res[i] < condend)
        {
            SchQ->vec[stidx].vid = res[i];
            SchQ->vec[stidx].eid = st;
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
            SchQ->vec[stidx].valid = 1;
            SchQ->vec[stidx].previdx = 0;
    
            SchQ->vec[stidx].cond.eneed = 0;
            SchQ->vec[stidx].cond.vneed = 1;

            ++stidx;
            vis->SetValue(res[i], 1);
            Adj->GetNextVid(res[i], res_2, nums_2, st_2);
            for(u64 j = 0;j < nums_2;++j)
            {
                if(!vis->GetValue(res_2[j]) && res_2[j] >= condst && res_2[j] < condend)
                {
                    SchQ->vec[stidx].vid = res_2[j];
                    SchQ->vec[stidx].eid = st_2;
                    SchQ->vec[stidx].level = 2;
                    SchQ->vec[stidx].prev = NULL;

                    SchQ->vec[stidx].valid = 1;
                    SchQ->vec[stidx].previdx = 0;
            
                    SchQ->vec[stidx].cond.eneed = 0;
                    SchQ->vec[stidx].cond.vneed = 1;

                    ++stidx;
                    vis->SetValue(res_2[j], 1);
                }
                ++st_2;
            }
        }
        ++st;
    }
    */
    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->eneed = 0;
    condArg_1->vneed = 1;
    condArg_1->edsp = 0;
    u64 noUsed;
    FindFriendsFun(id, condArg_1.get(), 1, 2, noUsed);
    shidx = 0;

    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);
    shidx = stidx;

    shared_ptr<CondData> condArg_2 = make_shared<CondData>();
    condArg_2->eneed = 0;
    condArg_2->edsp = 0;
    condArg_2->vneed = 1;
    condArg_2->vop = -1;
    condArg_2->vobj = datetime;
    condArg_2->vdk = vkinds["comment"].hmap["creationdate"];

    shared_ptr<CondData> condArg_3 = make_shared<CondData>();
    condArg_3->eneed = 0;
    condArg_3->edsp = 0;
    condArg_3->vneed = 1;
    condArg_3->vop = -1;
    condArg_3->vobj = datetime;
    condArg_3->vdk = vkinds["post"].hmap["creationdate"];

    while(ResQ->hidx < ResQ->tidx)
    {
        GNode* t = ResQ->ptrv[ResQ->hidx];
        ++ResQ->hidx;
        t->multin = 1;
        t->multi = new MultiGNode[t->multin];
        /* 找post和comment */
        Adj->GetPrevVid(t->vid, res, nums, st);
        u64 st1 = vkinds["comment"].st, end1 = st1 + vkinds["comment"].n;
        u64 st2 = vkinds["post"].st, end2 = st2 + vkinds["post"].n;
        for(u64 i = 0;i < nums;++i)
        {
            if(res[i] >= st1 && res[i] < end1) // comment
            {
                GNode& tt = SchQ->vec[stidx];
                tt.vid = res[i];
                tt.level = t->level + 1;
                tt.prev = t;
                tt.valid = 2;
                tt.previdx = 0;
                
                tt.cond = condArg_2.get();
                // tt.cond.eneed = 0;
                // tt.cond.vneed = 1;
                // tt.cond.vop = -1;
                // tt.cond.vobj = datetime;
                // tt.cond.vdk = vkinds["comment"].hmap["creationdate"];
                ++stidx;
            }
            else if(res[i] >= st2 && res[i] < end2) // post
            {
                GNode& tt = SchQ->vec[stidx];
                tt.vid = res[i];
                tt.level = t->level + 1;
                tt.prev = t;
                tt.valid = 2;
                tt.previdx = 0;
                
                tt.cond = condArg_3.get();
                // tt.cond.eneed = 0;
                // tt.cond.vop = -1;
                // tt.cond.vneed = 1;
                // tt.cond.vobj = datetime;
                // tt.cond.vdk = vkinds["post"].hmap["creationdate"];
                ++stidx;
            }
        }
    }
    
    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

    /* 路径追溯代码 */
    PathTracing(ResQ);
    /* 打印表头 */
    cout<<"friend.id|friend.firstName|friend.lastName|message.id|message.content/imageFile|creationDate"<<endl;
    struct data
    {
        string id = "";
        string firstName = "";
        string lastName = "";
        string mid = "";
        string content = "";
        string date = "";
    };
    u64 rows = 0;
    for(u64 i = 0;i < ResQ->tidx;++i)
        if(ResQ->ptrv[i]->valid == 2)
            ++rows;
    vector<data> table(rows);
    rows = 0;

    for(u64 i = 0;i < ResQ->tidx;++i)
    {
        GNode *t = ResQ->ptrv[i];
        if(t->valid != 1)
            continue;
        for(u64 j = 0;j < t->multin;++j)
        {
            data tbl;
            GetRowInfo(tbl.id, "id", t->vinfo, t->vtype);
            GetRowInfo(tbl.firstName, "firstname", t->vinfo, t->vtype);
            GetRowInfo(tbl.lastName, "lastname", t->vinfo, t->vtype);
            for(u64 l = 0;l < t->multi[j].n;++l)
            {
                GNode *tt = t->multi[j].vec[l];
                table[rows] = tbl;
                GetRowInfo(table[rows].mid, "id", tt->vinfo, tt->vtype);
                if(tt->vtype == "post")
                    GetRowInfo(table[rows].content, "imagefile", tt->vinfo, tt->vtype);
                if(tt->vtype == "comment")
                    GetRowInfo(table[rows].content, "content", tt->vinfo, tt->vtype);
                GetRowInfo(table[rows].date, "creationdate", tt->vinfo, tt->vtype);
                ++rows;
            }
        }
    }
    MultiFree(ResQ);
    sort(table.begin(), table.end(), [](data& x, data& y)
    {
        if(x.date != y.date)
            return x.date > y.date;
        else
            return x.mid < y.mid;
    });
    // sort(table.begin(), table.end(), [](data& x, data& y){return x.mid < y.mid;});

    for(u64 i = 0;i < rows && i < 20;++i)
       cout<<table[i].id<<'|'<<table[i].firstName<<'|'<<table[i].lastName<<'|'<<table[i].mid<<'|'<<table[i].content<<'|'<<table[i].date<<endl; 


}

void Graph::Search_IC_13(string obj)
{
    u64 id1, id2, realid1, realid2;
    string type = "person";
    u64 threads = max_threads;
    volatile u64 sumwork = threads + 1, curwork = 0;
    u64 *vec = Que[0]->gVec->vec;
    u64 **tvec = Que[0]->gVec->tVec;

    id1 = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    id2 = strtoull(obj.substr(obj.find_first_of(' ') + 1).c_str(), NULL, 10);

    if(id1 == id2)
    {
        cout<<0<<endl;
        return;
    }
    GetRealId(type, id1, realid1);
    GetRealId(type, id2, realid2);
    u64 personSize = vkinds["person"].n, personSt = vkinds["person"].st;
    uchar *visPerson = new uchar[personSize];
    memset(visPerson, 0, personSize * sizeof(uchar));
    u64 hidx = 0, tidx = 0;
    MutexLock veclock;
    u64 *res = NULL;
    u64 nums, st;
    Adj->GetNextVid(realid1, res, nums, st);
    u64 condst = vkinds[type].st, condend = vkinds[type].n + condst; 
    bool flag = 0;
    i64 level = -1;
    for(u64 i = 0;i < nums;++i)
    {
        if(res[i] >= condst && res[i] < condend && !visPerson[res[i] - personSt])
        {
            if(res[i] == realid2)
            {
                flag = 1;
                level = 1;
                break;
            }
            if(res[i] >= condend)
                break;
            vec[tidx] = res[i];
            ++tidx;
            visPerson[res[i] - personSt] = 1;
        }
    }
    i64 tlevel = 1;

    while(!flag && hidx < tidx)
    {

        u64 price = (tidx - hidx) / threads;
        u64 mod = (tidx - hidx) % threads;
        u64 start = hidx;
        hidx = tidx;
        ++tlevel;
        for(u64 i = 0;i < threads;++i)
        {
            IC_13_Arg* tmp1 = new IC_13_Arg(vec, tvec[i], Adj, visPerson, Que[0]->tasklock, personSt, &tidx, &curwork, i * price + start, price, condst, condend);

            pool->addwork(tmp1, IC_13_Fun);
        }

        IC_13_Arg* tmp2 = new IC_13_Arg(vec, tvec[threads], Adj, visPerson, Que[0]->tasklock, personSt, &tidx, &curwork, threads * price + start, mod, condst, condend);
        pool->addwork(tmp2, IC_13_Fun);
        
        while(1)
        {
            {
                MutexLockGuard tmut(*Que[0]->tasklock);
                if(sumwork == curwork)
                    break;
            } 
        }
        hidx = 0;
        tidx = 0;
        for(u64 i = 0;i < sumwork;++i)
        {
            for(u64 j = 0;tvec[i][j] != -1;++j)
            {
                if(tvec[i][j] == realid2)
                {
                    level = tlevel;
                    flag = 1;
                    break;
                }
                if(!visPerson[tvec[i][j] - personSt])
                {
                    vec[tidx++] = tvec[i][j];
                    visPerson[tvec[i][j] - personSt] = 1;
                }
            }
        }
            
        curwork = 0;
    }
    cout<<level<<endl;
    //933 2199023676291
    delete [] visPerson;
}

// void Graph::Search_IC_13(string obj)
// {
//     u64 id1, id2, realid1, realid2;
//     string type = "person";
//     u64 threads = max_threads;
//     volatile u64 sumwork = threads + 1, curwork = 0;
//     u64 maxsize = 3ull << 22;
//     u64 *vec = new u64[maxsize];
//     u64 **tvec = new u64*[sumwork];
//     u64 tVecSize = maxsize >> 3;
//     for(u64 i = 0;i < sumwork;++i)
//         tvec[i] = new u64[tVecSize];
//     memset(vec, 0, 8 * maxsize);

//     id1 = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
//     id2 = strtoull(obj.substr(obj.find_first_of(' ') + 1).c_str(), NULL, 10);

//     if(id1 == id2)
//     {
//         cout<<0<<endl;
//         return;
//     }
//     GetRealId(type, id1, realid1);
//     GetRealId(type, id2, realid2);
//     u64 personSize = vkinds["person"].n, personSt = vkinds["person"].st;
//     uchar *visPerson = new uchar[personSize];
//     memset(visPerson, 0, personSize * sizeof(uchar));
//     u64 hidx = 0, tidx = 0;
//     MutexLock veclock;
//     u64 *res = NULL;
//     u64 nums, st;
//     Adj->GetNextVid(realid1, res, nums, st);
//     u64 condst = vkinds[type].st, condend = vkinds[type].n + condst; 
//     bool flag = 0;
//     i64 level = -1;
//     for(u64 i = 0;i < nums;++i)
//     {
//         if(res[i] >= condst && res[i] < condend && !visPerson[res[i] - personSt])
//         {
//             if(res[i] == realid2)
//             {
//                 flag = 1;
//                 level = 1;
//                 break;
//             }
//             if(res[i] >= condend)
//                 break;
//             vec[tidx] = res[i];
//             ++tidx;
//             visPerson[res[i] - personSt] = 1;
//         }
//     }
//     i64 tlevel = 1;

//     while(!flag && hidx < tidx)
//     {
//         u64 price = (tidx - hidx) / threads;
//         u64 mod = (tidx - hidx) % threads;
//         u64 start = hidx;
//         hidx = tidx;
//         ++tlevel;
//         for(u64 i = 0;i < threads;++i)
//         {
//             IC_13_Arg* tmp1 = new IC_13_Arg(vec, tvec[i], Adj, visPerson, Que[0]->tasklock, personSt, &tidx, &curwork, i * price + start, price, condst, condend);

//             pool->addwork(tmp1, IC_13_Fun);
//         }

//         IC_13_Arg* tmp2 = new IC_13_Arg(vec, tvec[threads], Adj, visPerson, Que[0]->tasklock, personSt, &tidx, &curwork, threads * price + start, mod, condst, condend);
//         pool->addwork(tmp2, IC_13_Fun);
        
//         while(1)
//         {
//             {
//                 MutexLockGuard tmut(*Que[0]->tasklock);
//                 if(sumwork == curwork)
//                     break;
//             } 
//         }
//         hidx = 0;
//         tidx = 0;
//         for(u64 i = 0;i < sumwork;++i)
//         {
//             for(u64 j = 0;tvec[i][j] != -1;++j)
//             {
//                 if(tvec[i][j] == realid2)
//                 {
//                     level = tlevel;
//                     flag = 1;
//                     break;
//                 }
//                 if(!visPerson[tvec[i][j] - personSt])
//                 {
//                     vec[tidx++] = tvec[i][j];
//                     visPerson[tvec[i][j] - personSt] = 1;
//                 }
//             }
//         }
//         // for(u64 i = hidx;i < tidx;++i)
//         // {
//         //     if(vec[i] == realid2)
//         //     {
//         //         level = tlevel;
//         //         flag = 1;
//         //         break;
//         //     }
                
//         //     if(!vis->GetValue(vec[i]))
//         //         vis->SetValue(vec[i], 1);
//         // }
            
//         curwork = 0;
//     }
//     cout<<level<<endl;
//     //933 2199023676291
//     delete [] vec;
//     for(u64 i = 0;i < sumwork;++i)
//         delete [] tvec[i];
//     delete [] tvec;
//     delete [] visPerson;
// }

void Graph::Search_BI_3(string obj)
{

    u64 threads = max_threads;
    volatile u64 sumwork = threads + 1, curwork = 0;

    string tagClass = obj.substr(0, obj.find_first_of(' ')), country = obj.substr(obj.find_first_of(' ') + 1);
    string placeType = "place", placeRow = "name";
    u64 placeRealId, rows = 0;

    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    InArray *vis = Que[0]->vis;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;

    u64 *placeRes = NULL;
    u64 placeNums, placeSt;

    GetRealId(placeType, country, placeRow, placeRealId);

    Adj->GetPrevVid(placeRealId, placeRes, placeNums, placeSt);
    u64 placeCondSt = vkinds["place"].st, placeCondEnd = vkinds["place"].n + placeCondSt;
    for(u64 i = 0;i < placeNums;++i){
        if(placeRes[i] >= placeCondSt && placeRes[i] < placeCondEnd){
            SchQ->vec[stidx].vid = placeRes[i];
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
            SchQ->vec[stidx].previdx = 0;
            ++stidx;
            if(placeRes[i] >= placeCondEnd) 
                break;
        }
        ++placeSt;
    }
    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->eneed = 0;
    condArg_1->edsp = 0;
    condArg_1->vneed = 1;

    u64 prestidx = stidx;
    u64 personCondSt = vkinds["person"].st, personCondEnd = vkinds["person"].n + personCondSt;
    u64 *personRes = NULL;
    u64 personNums, personSt;
    while(shidx < prestidx){
        GNode &t = SchQ->vec[shidx];     
        ++shidx;
        Adj->GetPrevVid(t.vid, personRes, personNums, personSt);
        for(u64 i = 0;i < personNums;++i){
            if(personRes[i] >= personCondSt && personRes[i] < personCondEnd){
                SchQ->vec[stidx].vid = personRes[i];
                SchQ->vec[stidx].level = 2;
                SchQ->vec[stidx].prev = NULL; 
                SchQ->vec[stidx].previdx = 0;
                SchQ->vec[stidx].cond = condArg_1.get();
                ++stidx;
                if(personRes[i] >= personCondEnd) 
                    break;
            }
            ++personSt;
        }
    }

    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

    u64 personIdxEnd = ResQ->tidx;
    u64 resIdx = ResQ->tidx;

    shared_ptr<CondData> condArg_2 = make_shared<CondData>();
    condArg_2->eneed = 1;
    condArg_2->vneed = 1;
    condArg_2->etype = "hasmoderator";
    condArg_2->edsp = 0;

    prestidx = stidx;
    u64 forumCondSt = vkinds["forum"].st, forumCondEnd = vkinds["forum"].n + forumCondSt;
    u64 *forumRes = NULL, *tmpRes = NULL;
    u64 forumSt, forumNums, tmpNums, tmpSt;

    u64 tail = stidx;

    while(shidx < tail){
        u64 nowHead = stidx;
        u64 nowMax = tail > shidx + BI_3_PERSON_MAX ? BI_3_PERSON_MAX : tail - shidx;
        for(u64 p = 0;p < nowMax;++p){
            GNode& t = SchQ->vec[shidx];
            ++shidx;
            t.multin = 1;
            t.multi = new MultiGNode[t.multin];

            Adj->GetPrevVid(t.vid, forumRes, forumNums, forumSt);
            for(u64 i = 0;i < forumNums;++i){
                if(!vis->GetValue(forumRes[i]) && forumRes[i] >= forumCondSt && forumRes[i] < forumCondEnd){
                    Adj->GetNextVid(forumRes[i], tmpRes, tmpNums, tmpSt);
                    u64 j = 0;
                    while(j < tmpNums && tmpRes[j] != t.vid){
                        ++tmpSt;
                        ++j;
                    }

                    GNode& tNode = SchQ->vec[stidx];
                    tNode.vid = forumRes[i];
                    tNode.eid = tmpSt;
                    tNode.prev = &t;
                    tNode.level = t.level + 1;
                    tNode.previdx = 0;

                    tNode.cond = condArg_2.get();

                    ++stidx;
                }
                ++forumSt;
            }
        }

        threads_work(SchQ, ResQ, nowHead, stidx, threads, Que[0]->tasklock, Que[0]->reslock);
        for(;resIdx < ResQ->tidx;++resIdx)
            if(!vis->GetValue(ResQ->ptrv[resIdx]->vid))
                vis->SetValue(ResQ->ptrv[resIdx]->vid, 1);
    }

    string tagClassType = "tagclass", tagClassRow = "name";
    u64* tagRes = NULL;
    u64 tagClassRealId, tagSt, tagNums;
    GetRealId(tagClassType, tagClass, tagClassRow, tagClassRealId);
    vis->SetValue(tagClassRealId, 1);
    Adj->GetPrevVid(tagClassRealId, tagRes, tagNums, tagSt);
    u64 tagCondSt = vkinds["tag"].st, tagCondEnd = tagCondSt + vkinds["tag"].n;
    for(u64 i = 0;i < tagNums;++i){
        if(!vis->GetValue(tagRes[i]) && tagRes[i] >= tagCondSt && tagRes[i] < tagCondEnd){
            vis->SetValue(tagRes[i], 1);
        }
    }
    threads_work_BI_3(SchQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock, vis, Que[0]->gVec);

    PathTracing(ResQ);

    /* 打印表头 */
    cout<<"forum.id|forum.title|forum.creationDate|person.id|messageCount"<<endl;
    struct data{
        string forumId = "";
        string forumTitle = "";
        string forumCreationDate = "";
        string personId = "";
        u64 messageCount = 0;
    };

    for(u64 i = 0;i < personIdxEnd;++i){
        GNode *t = ResQ->ptrv[i];
        rows += t->multi[0].n;
    }

    vector<data> table(rows);
    u64 tableIdx = 0;
    for(u64 i = 0;i < personIdxEnd;++i){
        GNode *t = ResQ->ptrv[i];
        for(u64 j = 0;j < t->multi[0].n;++j){
            data *tbl = &table[tableIdx++];
            GetRowInfo(tbl->personId, "id", t->vinfo, t->vtype);
            GNode *tt = t->multi[0].vec[j];
            GetRowInfo(tbl->forumId, "id", tt->vinfo, tt->vtype);
            GetRowInfo(tbl->forumTitle, "title", tt->vinfo, tt->vtype);
            GetRowInfo(tbl->forumCreationDate, "creationdate", tt->vinfo, tt->vtype);
            tbl->messageCount = tt->valid;
        }
    }

    MultiFree(ResQ);

    /* 排序部分 */
    sort(table.begin(), table.end(), [](data& x, data& y){
            if(x.messageCount != y.messageCount)
                return x.messageCount > y.messageCount;
            // else
                return strtoul(x.forumId.c_str(), NULL, 10) < strtoul(y.forumId.c_str(), NULL, 10);
        });
    
    for(u64 i = 0;i < rows && i < 20;++i){
        cout<<table[i].forumId
            <<'|'<<table[i].forumTitle
            <<'|'<<table[i].forumCreationDate
            <<'|'<<table[i].personId
            <<'|'<<table[i].messageCount<<endl;
    }
    if(placeRes) delete [] placeRes;
    if(personRes) delete [] personRes;
    if(forumRes) delete [] forumRes;
    if(tmpRes) delete [] tmpRes;
    if(tagRes) delete [] tagRes;

}

void Graph::Search_BI_10(string obj)
{
    u64 threads = max_threads;
    volatile u64 sumwork = threads + 1, curwork = 0;
    vector<string> tmpPar = GetParams(obj);
    u64 personId = strtoull(tmpPar[0].c_str(), NULL, 10);
    string country = tmpPar[1];
    string tagClass = tmpPar[2];
    //u64 minPathDistance = strtoull(tmpPar[3].c_str(), NULL, 10);
    //u64 maxPathDistance = strtoull(tmpPar[4].c_str(), NULL, 10);

    /* add */
    u64 minPathDistance = 3;
    u64 maxPathDistance = 4;

    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    InArray *vis = Que[0]->vis;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    
    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->eneed = 0;
    condArg_1->edsp = 0;
    condArg_1->vneed = 1;

    u64 minPathDistanceSt = 0;
    FindFriendsFun(personId, condArg_1.get(), minPathDistance, maxPathDistance, minPathDistanceSt);

    /* 地点 */

    u64 countryId;
    string placeType = "place", placeRow = "name";
    GetRealId(placeType, country, placeRow, countryId);
    u64 *countryRes = NULL;
    u64 countryNums, countrySt;
    Adj->GetPrevVid(countryId, countryRes, countryNums, countrySt);
    u64 placeCondSt = vkinds["place"].st, placeCondEnd = vkinds["place"].n + placeCondSt;
    for(u64 i = 0;i < countryNums;++i){
        if(!vis->GetValue(countryRes[i]) && countryRes[i] >= placeCondSt && countryRes[i] < placeCondEnd){
            vis->SetValue(countryRes[i], 1);
        }
    }

    /* 提取出来 */
    shidx = minPathDistanceSt;

    threads_work_BI_10_1(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock, vis);

    string tagClassType = "tagclass", tagClassRow = "name";
    u64* tagRes = NULL;
    u64 tagClassRealId, tagSt, tagNums;
    GetRealId(tagClassType, tagClass, tagClassRow, tagClassRealId);

    vis->SetValue(tagClassRealId, 1);
    Adj->GetPrevVid(tagClassRealId, tagRes, tagNums, tagSt);
    u64 tagCondSt = vkinds["tag"].st, tagCondEnd = tagCondSt + vkinds["tag"].n;
    unordered_map<u64, string> tagMap;
    for(u64 i = 0;i < tagNums;++i){
        if(!vis->GetValue(tagRes[i]) && tagRes[i] >= tagCondSt && tagRes[i] < tagCondEnd){
            vis->SetValue(tagRes[i], 1);
            // string tmpVInfo;
            // GetVInfo(tmpVInfo, tagRes[i]);
            // GetRowInfo(tagMap[tagRes[i]], "name", tmpVInfo, "tag");
        }
    }

    threads_ext_tag(tagMap, threads, Que[0]->tasklock);

    vector<unordered_map<u64, u64> > ptrPerson(ResQ->tidx);
    // for(u64 i = 0;i < ResQ->tidx;++i){
    //     for(auto p = tagMap.begin();p != tagMap.end();++p){
    //         ptrPerson[i][p->first] = 0;
    //     }
    // }

    threads_work_BI_10_2(ResQ, threads, Que[0]->tasklock, vis, ptrPerson);

    /* 打印表头 */
    cout<<"expertCandidatePerson.id|tag.name|messageCount"<<endl;
    struct data{
        string personId = "";
        string tagName = "";
        u64 messagesCount = 0;
    };

    u64 totalSize = 0;
    for(u64 i = 0;i < ResQ->tidx;++i)
        totalSize += ptrPerson[i].size();

    vector<data> table(totalSize);
    u64 tableIdx = 0;
    for(u64 i = 0;i < ResQ->tidx;++i){
        GNode *t = ResQ->ptrv[i];
        string tmpPersonId;
        GetRowInfo(tmpPersonId, "id", t->vinfo, t->vtype);
        for(unordered_map<u64, u64>::iterator p = ptrPerson[i].begin();p != ptrPerson[i].end();++p){
            table[tableIdx].personId = tmpPersonId;
            table[tableIdx].tagName = tagMap[p->first];
            table[tableIdx].messagesCount = p->second;
            ++tableIdx;
        }
    }

    /* Sort */
    sort(table.begin(), table.end(), [](data& x , data& y){
            if(x.messagesCount != y.messagesCount)
                return x.messagesCount > y.messagesCount;
            else if(x.tagName != y.tagName)
                return x.tagName < y.tagName;
            else
                return strtoul(x.personId.c_str(), NULL, 10) < strtoul(y.personId.c_str(), NULL, 10);
        });

    for(u64 i = 0;i < table.size() && i < 100;++i){
        cout<<table[i].personId
            <<'|'<<table[i].tagName
            <<'|'<<table[i].messagesCount<<endl;
    }
    if(countryRes) delete [] countryRes;
    if(tagRes) delete [] tagRes;
}

void Graph::Search_BI_16(string obj)
{
    u64 threads = max_threads;

    vector<string> tmpPar = GetParams(obj);
    string tagA = tmpPar[0], dateA = tmpPar[1], tagB = tmpPar[2], dateB = tmpPar[3];
    u64 maxKnowsLimit = strtoull(tmpPar[4].c_str(), NULL, 10);
    string tagType = "tag", tagRow = "name";
    u64 tagRealId1, tagRealId2;

    char buf1[64], buf2[64];
    time_t xx = stoull(dateA)/1000;
    tm *yy = gmtime(&xx);
    strftime(buf1, sizeof(buf1), "%FT%T.000%z", yy);
    dateA = buf1;
    xx = stoull(dateB)/1000;
    yy = gmtime(&xx);
    strftime(buf2, sizeof(buf2), "%FT%T.000%z", yy);
    dateB = buf2;

    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    InArray *vis = Que[0]->vis;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;

    GetRealId(tagType, tagA, tagRow, tagRealId1);
    GetRealId(tagType, tagB, tagRow, tagRealId2);
cerr<<tagRealId1<<endl;
cerr<<tagRealId2<<endl;
    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->eneed = 0;
    condArg_1->edsp = 0;
    condArg_1->vneed = 1;
    condArg_1->vop = 0;
    condArg_1->vobj = dateA;
    condArg_1->vdk = vkinds["post"].hmap["creationdate"];

    shared_ptr<CondData> condArg_2 = make_shared<CondData>();
    condArg_2->eneed = 0;
    condArg_2->edsp = 0;
    condArg_2->vneed = 1;
    condArg_2->vop = 0;
    condArg_2->vobj = dateA;
    condArg_2->vdk = vkinds["comment"].hmap["creationdate"];

    u64 postCondSt = vkinds["post"].st, postCondEnd = vkinds["post"].n + postCondSt;
    u64 commentCondSt = vkinds["comment"].st, commentCondEnd = vkinds["comment"].n + commentCondSt;
    u64 *messageRes = NULL;
    u64 messageNums, messageSt;
    Adj->GetPrevVid(tagRealId1, messageRes, messageNums, messageSt);
    for(u64 i = 0;i <  messageNums;++i){
        if(messageRes[i] >= postCondSt && messageRes[i] < postCondEnd){
            SchQ->vec[stidx].vid = messageRes[i];
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
            SchQ->vec[stidx].valid = 0;
            SchQ->vec[stidx].previdx = 0;
            SchQ->vec[stidx].cond = condArg_1.get();
            ++stidx;
        }
        else if(messageRes[i] >= commentCondSt && messageRes[i] < commentCondEnd){
            SchQ->vec[stidx].vid = messageRes[i];
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
            SchQ->vec[stidx].valid = 0;
            SchQ->vec[stidx].previdx = 0;
            SchQ->vec[stidx].cond = condArg_2.get();
            ++stidx;
        }
        ++messageSt;
    }

    shared_ptr<CondData> condArg_3 = make_shared<CondData>();
    condArg_3->eneed = 0;
    condArg_3->edsp = 0;
    condArg_3->vneed = 1;
    condArg_3->vop = 0;
    condArg_3->vobj = dateB;
    condArg_3->vdk = vkinds["post"].hmap["creationdate"];

    shared_ptr<CondData> condArg_4 = make_shared<CondData>();
    condArg_4->eneed = 0;
    condArg_4->edsp = 0;
    condArg_4->vneed = 1;
    condArg_4->vop = 0;
    condArg_4->vobj = dateB;
    condArg_4->vdk = vkinds["comment"].hmap["creationdate"];

    if(tagA != tagB){
        Adj->GetPrevVid(tagRealId2, messageRes, messageNums, messageSt);
        for(u64 i = 0;i <  messageNums;++i){
            if(messageRes[i] >= postCondSt && messageRes[i] < postCondEnd){
                SchQ->vec[stidx].vid = messageRes[i];
                SchQ->vec[stidx].level = 2;
                SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
                SchQ->vec[stidx].valid = 0;
                SchQ->vec[stidx].previdx = 0;
                SchQ->vec[stidx].cond = condArg_3.get();
                ++stidx;
            }
            else if(messageRes[i] >= commentCondSt && messageRes[i] < commentCondEnd){
                SchQ->vec[stidx].vid = messageRes[i];
                SchQ->vec[stidx].level = 2;
                SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
                SchQ->vec[stidx].valid = 0;
                SchQ->vec[stidx].previdx = 0;
                SchQ->vec[stidx].cond = condArg_4.get();
                ++stidx;
            }
            ++messageSt;
        }
    }

    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

    vector<BISch_16_Vec> personVec(vkinds["person"].n, BISch_16_Vec());
    u64 personCondSt = vkinds["person"].st, personCondEnd = vkinds["person"].n + personCondSt;
    u64 *personRes = NULL;
    u64 personNums, st;

    while(ResQ->hidx < ResQ->tidx){
        GNode *t = ResQ->ptrv[ResQ->hidx];
        ++ResQ->hidx;
        Adj->GetNextVid(t->vid, personRes, personNums, st);
        for(u64 i = 0;i < personNums;++i){
            if(personRes[i] >= personCondSt && personRes[i] < personCondEnd){
                if(!vis->GetValue(personRes[i]))
                    vis->SetValue(personRes[i], 1);
                personVec[personRes[i] - personCondSt].realId = personRes[i];
                if(t->level == 1)
                    ++personVec[personRes[i] - personCondSt].mesCountA;
                else if(t->level == 2)
                    ++personVec[personRes[i] - personCondSt].mesCountB;  
            }
            else if(personRes[i] >= personCondEnd)
                break;
        }
    }

    u64 personVecSize = 0;
    for(u64 i = 0;i < personVec.size();++i){
        if(personVec[i].mesCountA + personVec[i].mesCountB){
            personVec[personVecSize++] = personVec[i];
        }
    }

    threads_work_BI_16(threads, Que[0]->tasklock, vis, personVec, personVecSize, maxKnowsLimit);

    /* 打印表头 */
    cout<<"person.id|messageCountA|messageCountB"<<endl;
    struct data{
        u64 personId;
        u64 mesCountA;
        u64 mesCountB;
    };
    u64 dataSum = 0;
    for(u64 i = 0;i < personVecSize;++i)
        if(personVec[i].mesCountA + personVec[i].mesCountB)
            ++dataSum;
    vector<data> table(dataSum);
    u64 tableIdx = 0;
    for(u64 i = 0;i < personVecSize;++i){
        if(personVec[i].mesCountA + personVec[i].mesCountB){
            table[tableIdx].personId = personVec[i].oldId;
            table[tableIdx].mesCountA = personVec[i].mesCountA;
            table[tableIdx].mesCountB = personVec[i].mesCountB;
            ++tableIdx;
        }
    }

    sort(table.begin(), table.end(), [](data &x, data &y){
            if(x.mesCountA + x.mesCountB != y.mesCountA + y.mesCountB)
                return x.mesCountA + x.mesCountB > y.mesCountA + y.mesCountB;
            else
                return x.personId < y.personId;
        });

    for(u64 i = 0;i < table.size() && i < 20;++i){
        if(table[i].personId){
            cout<<table[i].personId
                <<'|'<<table[i].mesCountA
                <<'|'<<table[i].mesCountB<<endl;
        }
    }
    if(messageRes) delete [] messageRes;
    if(personRes) delete [] personRes;
}

void Graph::Search_BI_16_2(string obj)
{
    u64 threads = max_threads;

    vector<string> tmpPar = GetParams(obj);
    string tagA = tmpPar[0], dateA = tmpPar[1], tagB = tmpPar[2], dateB = tmpPar[3];
    //u64 maxKnowsLimit = strtoull(tmpPar[4].c_str(), NULL, 10);
    u64 maxKnowsLimit = 6;
    string tagType = "tag", tagRow = "name";
    u64 tagRealId1, tagRealId2;

    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    InArray *vis = Que[0]->vis;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;

    GetRealId(tagType, tagA, tagRow, tagRealId1);
    GetRealId(tagType, tagB, tagRow, tagRealId2);

    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->eneed = 0;
    condArg_1->edsp = 0;
    condArg_1->vneed = 1;
    condArg_1->vop = 0;
    condArg_1->vobj = dateA;
    condArg_1->vdk = vkinds["post"].hmap["creationdate"];

    shared_ptr<CondData> condArg_2 = make_shared<CondData>();
    condArg_2->eneed = 0;
    condArg_2->edsp = 0;
    condArg_2->vneed = 1;
    condArg_2->vop = 0;
    condArg_2->vobj = dateA;
    condArg_2->vdk = vkinds["comment"].hmap["creationdate"];

    u64 postCondSt = vkinds["post"].st, postCondEnd = vkinds["post"].n + postCondSt;
    u64 commentCondSt = vkinds["comment"].st, commentCondEnd = vkinds["comment"].n + commentCondSt;
    u64 *messageRes = NULL;
    u64 messageNums, messageSt;
    Adj->GetPrevVid(tagRealId1, messageRes, messageNums, messageSt);
    for(u64 i = 0;i <  messageNums;++i){
        if(messageRes[i] >= postCondSt && messageRes[i] < postCondEnd){
            SchQ->vec[stidx].vid = messageRes[i];
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
            SchQ->vec[stidx].valid = 0;
            SchQ->vec[stidx].previdx = 0;
            SchQ->vec[stidx].cond = condArg_1.get();
            ++stidx;
        }
        else if(messageRes[i] >= commentCondSt && messageRes[i] < commentCondEnd){
            SchQ->vec[stidx].vid = messageRes[i];
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
            SchQ->vec[stidx].valid = 0;
            SchQ->vec[stidx].previdx = 0;
            SchQ->vec[stidx].cond = condArg_2.get();
            ++stidx;
        }
        ++messageSt;
    }

    shared_ptr<CondData> condArg_3 = make_shared<CondData>();
    condArg_3->eneed = 0;
    condArg_3->edsp = 0;
    condArg_3->vneed = 1;
    condArg_3->vop = 0;
    condArg_3->vobj = dateB;
    condArg_3->vdk = vkinds["post"].hmap["creationdate"];

    shared_ptr<CondData> condArg_4 = make_shared<CondData>();
    condArg_4->eneed = 0;
    condArg_4->edsp = 0;
    condArg_4->vneed = 1;
    condArg_4->vop = 0;
    condArg_4->vobj = dateB;
    condArg_4->vdk = vkinds["comment"].hmap["creationdate"];

    if(tagA != tagB){
        Adj->GetPrevVid(tagRealId2, messageRes, messageNums, messageSt);
        for(u64 i = 0;i <  messageNums;++i){
            if(messageRes[i] >= postCondSt && messageRes[i] < postCondEnd){
                SchQ->vec[stidx].vid = messageRes[i];
                SchQ->vec[stidx].level = 2;
                SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
                SchQ->vec[stidx].valid = 0;
                SchQ->vec[stidx].previdx = 0;
                SchQ->vec[stidx].cond = condArg_3.get();
                ++stidx;
            }
            else if(messageRes[i] >= commentCondSt && messageRes[i] < commentCondEnd){
                SchQ->vec[stidx].vid = messageRes[i];
                SchQ->vec[stidx].level = 2;
                SchQ->vec[stidx].prev = NULL; // 不需要打印第一个结点的信息
                SchQ->vec[stidx].valid = 0;
                SchQ->vec[stidx].previdx = 0;
                SchQ->vec[stidx].cond = condArg_4.get();
                ++stidx;
            }
            ++messageSt;
        }
    }

    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

    vector<BISch_16_Vec> personVec(vkinds["person"].n, BISch_16_Vec());
    u64 personCondSt = vkinds["person"].st, personCondEnd = vkinds["person"].n + personCondSt;
    u64 *personRes = NULL;
    u64 personNums, st;

    InArray *visA = new InArray(vkinds["person"].n, 1);
    InArray *visB = new InArray(vkinds["person"].n, 1);

    while(ResQ->hidx < ResQ->tidx){
        GNode *t = ResQ->ptrv[ResQ->hidx];
        ++ResQ->hidx;
        Adj->GetNextVid(t->vid, personRes, personNums, st);
        for(u64 i = 0;i < personNums;++i){
            if(personRes[i] >= personCondSt && personRes[i] < personCondEnd){
                personVec[personRes[i] - personCondSt].realId = personRes[i];
                if(t->level == 1){
                    ++personVec[personRes[i] - personCondSt].mesCountA;
                    if(!visA->GetValue(personRes[i] - personCondSt))
                        visA->SetValue(personRes[i] - personCondSt, 1);
                }
                else if(t->level == 2){
                    ++personVec[personRes[i] - personCondSt].mesCountB;  
                    if(!visB->GetValue(personRes[i] - personCondSt))
                        visB->SetValue(personRes[i] - personCondSt, 1);
                }
            }
            else if(personRes[i] >= personCondEnd)
                break;
        }
    }

    u64 personVecSize = 0;
    for(u64 i = 0;i < personVec.size();++i){
        if(personVec[i].mesCountA > 0 && personVec[i].mesCountB > 0){
            personVec[personVecSize++] = personVec[i];
        }
    }

    threads_work_BI_16_2(threads, Que[0]->tasklock, visA, visB, personVec, personVecSize, maxKnowsLimit);

    /* 打印表头 */
    cout<<"person.id|messageCountA|messageCountB"<<endl;
    struct data{
        u64 personId;
        u64 mesCountA;
        u64 mesCountB;
    };
    u64 dataSum = 0;
    for(u64 i = 0;i < personVecSize;++i)
        if(personVec[i].mesCountA > 0 && personVec[i].mesCountB > 0)
            ++dataSum;
    vector<data> table(dataSum);
    u64 tableIdx = 0;
    for(u64 i = 0;i < personVecSize;++i){
        if(personVec[i].mesCountA > 0 && personVec[i].mesCountB > 0){
            table[tableIdx].personId = personVec[i].oldId;
            table[tableIdx].mesCountA = personVec[i].mesCountA;
            table[tableIdx].mesCountB = personVec[i].mesCountB;
            ++tableIdx;
        }
    }

    sort(table.begin(), table.end(), [](data &x, data &y){
            if(x.mesCountA + x.mesCountB != y.mesCountA + y.mesCountB)
                return x.mesCountA + x.mesCountB > y.mesCountA + y.mesCountB;
            else
                return x.personId < y.personId;
        });

    for(u64 i = 0;i < table.size() && i < 20;++i){
        if(table[i].personId){
            cout<<table[i].personId
                <<'|'<<table[i].mesCountA
                <<'|'<<table[i].mesCountB<<endl;
        }
    }
    if(messageRes) delete [] messageRes;
    if(personRes) delete [] personRes;
    if(visA) delete visA;
    if(visB) delete visB;
}

void Graph::TestExtVertex(u64 times, u64 len, string fn)
{
    timeval btime, etime, totaltime;
    totaltime.tv_sec = 0;
    totaltime.tv_usec = 0;
    vector<pair<u64, u64> > randNums;
    default_random_engine e{random_device{}()};
    uniform_int_distribution<u64> dis(0, 1ull << 40);
    for(u64 i = 0;i < 1000;++i){
        u64 idx = dis(e) % Vcsa_cnt;
        idx = 0;
        randNums.push_back(pair<u64, u64>(idx, dis(e) % (Vertex_csa[idx]->getN() - len)));
    }

    gettimeofday(&btime,NULL);
    for(u64 i = 0;i < 1000;++i)
        Vertex_csa[randNums[i].first]->extracting(randNums[i].second, len);
    gettimeofday(&etime,NULL);

    totaltime.tv_sec += (etime.tv_sec - btime.tv_sec);
    totaltime.tv_usec += (etime.tv_usec - btime.tv_usec);

    ofstream fout;
    fout.open(fn.c_str(), ios_base::out | ios_base::app);
    fout<<"Ext Vertex "<<times<<" "<<len<<" Total Time : "<<totaltime.tv_sec * 1000000 + totaltime.tv_usec<<" us"<<endl;
    fout<<"Arg time : "<<(totaltime.tv_sec * 1000000 + totaltime.tv_usec) * 1.0 / times<<" us"<<endl;
}

void Graph::TestExtEdge(u64 times, u64 len, string fn)
{
    timeval btime, etime, totaltime;
    totaltime.tv_sec = 0;
    totaltime.tv_usec = 0;
    vector<pair<u64, u64> > randNums;
    default_random_engine e{random_device{}()};
    uniform_int_distribution<u64> dis(0, 1ull << 40);
    for(u64 i = 0;i < 1000;++i){
        u64 idx = dis(e) % Ecsa_cnt;
        idx = 0;
        randNums.push_back(pair<u64, u64>(idx, dis(e) % (Edge_csa[idx]->getN() - len)));
    }

    gettimeofday(&btime,NULL);
    for(u64 i = 0;i < 1000;++i)
        Edge_csa[randNums[i].first]->extracting(randNums[i].second, len);
    gettimeofday(&etime,NULL);

    totaltime.tv_sec += (etime.tv_sec - btime.tv_sec);
    totaltime.tv_usec += (etime.tv_usec - btime.tv_usec);

    ofstream fout;
    fout.open(fn.c_str(), ios_base::out | ios_base::app);
    fout<<"Ext Edge "<<times<<" "<<len<<" Total Time : "<<totaltime.tv_sec * 1000000 + totaltime.tv_usec<<" us"<<endl;
    fout<<"Arg time : "<<(totaltime.tv_sec * 1000000 + totaltime.tv_usec) * 1.0 / times<<" us"<<endl;
}

void Graph::KHops(string obj)
{
    string type;
    u64 id, jns, realid;
    u64 threads = max_threads;

    type = obj.substr(0, obj.find_first_of(' '));
    obj = obj.substr(obj.find_first_of(' ') + 1);
    id = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    jns = strtoull(obj.substr(obj.find_first_of(' ') + 1).c_str(), NULL, 10);

    InArray *vis = Que[0]->vis;
    u64 nums, st, i;
    bool shutdown = 0;
    NorQue *SchQ = Que[0]->SchQ; /* 下标是用户的id */
    PtrQue *ResQ = Que[0]->ResQ;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64 sumwork = 0, curwork = 0;
    u64* res = NULL;
    /* 得到真实顶点 */
    GetRealId(type, id, realid);
    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    /* 第一跳入队 */
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]))
        {
            SchQ->vec[stidx].vid = res[i];
            SchQ->vec[stidx].level = 1;
            ++stidx;
            vis->SetValue(res[i], 1);
        }
    }
    while(shidx < stidx)
    {
        GNode &t = SchQ->vec[shidx];     
        ++shidx;
        if(t.level >= jns)
            break;
        Adj->GetNextVid(t.vid, res, nums, st);
        for(i = 0;i < nums;++i)
        {
            if(!vis->GetValue(res[i]))
            {
                SchQ->vec[stidx].vid = res[i];
                SchQ->vec[stidx].level = t.level + 1;
                ++stidx;
                vis->SetValue(res[i], 1);
            }
        }
    }
    cout<<"KHops Finds : "<<stidx<<" Vertexs."<<endl;
/* 输出时间 */

}

void Graph::KHopsFriends(string obj)
{
    string type("person");
    u64 id, jns, realid;
    u64 threads = max_threads;

    id = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    //jns = strtoull(obj.substr(obj.find_first_of(' ') + 1).c_str(), NULL, 10);
    jns = 3;

    InArray *vis = Que[0]->vis;
    u64 nums, st, i;
    bool shutdown = 0;
    NorQue *SchQ = Que[0]->SchQ; /* 下标是用户的id */
    PtrQue *ResQ = Que[0]->ResQ;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64 sumwork = 0, curwork = 0;
    u64* res = NULL;
    /* 得到真实顶点 */
    GetRealId(type, id, realid);
    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    /* 第一跳入队 */
    // ekinds["person->person"].n = 1; 所以不需要提取边
    u64 condst = vkinds["person"].st, condend = condst + vkinds["person"].n;
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]) && res[i] >= condst && res[i] < condend)
        {
            SchQ->vec[stidx].vid = res[i];
            SchQ->vec[stidx].level = 1;
            ++stidx;
            vis->SetValue(res[i], 1);
        }
    }
    while(shidx < stidx)
    {
        GNode &t = SchQ->vec[shidx];     
        ++shidx;
        if(t.level >= jns)
            break;
        Adj->GetNextVid(t.vid, res, nums, st);
        for(i = 0;i < nums;++i)
        {
            if(!vis->GetValue(res[i]) && res[i] >= condst && res[i] < condend)
            {
                SchQ->vec[stidx].vid = res[i];
                SchQ->vec[stidx].level = t.level + 1;
                ++stidx;
                vis->SetValue(res[i], 1);
            }
        }
    }
    cout<<"KHopsFriends Finds : "<<stidx<<" Vertexs."<<endl;
/* 输出时间 */

}

void Graph::NormalSearch_threads(string obj)
{
    string type;
    u64 id, jns, realid;
    u64 threads = max_threads;
    
    type = obj.substr(0, obj.find_first_of(' '));
    obj = obj.substr(obj.find_first_of(' ') + 1);
    id = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    jns = strtoull(obj.substr(obj.find_first_of(' ') + 1).c_str(), NULL, 10);


    InArray *vis = Que[0]->vis;
    u64 nums, st, i;
    bool shutdown = 0;
    NorQue *SchQ = Que[0]->SchQ; /* 下标是用户的id */
    PtrQue *ResQ = Que[0]->ResQ;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64 sumwork = 0, curwork = 0;
    u64* res = NULL;

    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->eneed = 1;
    condArg_1->vneed = 1;
    
    /* 得到真实顶点 */
    GetRealId(type, id, realid);
    /* 第一个顶点单独处理 */
    GNode fv;
    fv.level = 0;
    fv.vid = realid;
    fv.vtype = type;
    GetVInfo(fv.vinfo, realid);
    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    fv.oldid = strtoull(fv.vinfo.substr(0, fv.vinfo.find_first_of('|')).c_str(), NULL, 10);
    /* 第一跳入队 */
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]))
        {
            SchQ->vec[stidx].vid = res[i];
            SchQ->vec[stidx].eid = st;
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = &fv;
            SchQ->vec[stidx].cond = condArg_1.get();
            // SchQ->vec[stidx].cond.vneed = 1;
            // SchQ->vec[stidx].cond.eneed = 1;
            ++stidx;
            vis->SetValue(res[i], 1);
        }
        ++st;
    }
    while(shidx < stidx)
    {
        GNode &t = SchQ->vec[shidx];     
        ++shidx;
        if(t.level >= jns)
            break;
        Adj->GetNextVid(t.vid, res, nums, st);
        for(i = 0;i < nums;++i)
        {
            if(!vis->GetValue(res[i]))
            {
                GNode& tnode = SchQ->vec[stidx];
                tnode.vid = res[i];
                tnode.eid = st;
                tnode.prev = &t;
                tnode.level = t.level + 1;
                tnode.cond = condArg_1.get();
                // tnode.cond.vneed = 1;
                // tnode.cond.eneed = 1;

                ++stidx;
                vis->SetValue(res[i], 1);
            }
            ++st;
        }
    }
    shidx = 0;

    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

/* 资源释放部分 */
    if(res != NULL)
        delete [] res;

    cout<<fv.vinfo;
    for(u64 i = 0;i < ResQ->tidx;++i)
    {
        string res("");
        DisplayDistance(ResQ->ptrv[i], res);
        cout<<res<<ResQ->ptrv[i]->vtype<<" : "<<ResQ->ptrv[i]->vinfo<<endl;
    }

/* 输出时间 */
}

void Graph::NormalSearch_InOut_threads(string obj)
{
    string type;
    u64 id, jns, realid;
    u64 threads = max_threads;

    type = obj.substr(0, obj.find_first_of(' '));
    obj = obj.substr(obj.find_first_of(' ') + 1);
    id = strtoull(obj.substr(0, obj.find_first_of(' ')).c_str(), NULL, 10);
    jns = strtoull(obj.substr(obj.find_first_of(' ') + 1).c_str(), NULL, 10);

    InArray *vis = Que[0]->vis;
    u64 nums, st, i;
    bool shutdown = 0;
    NorQue *SchQ = Que[0]->SchQ; /* 下标是用户的id */
    PtrQue *ResQ = Que[0]->ResQ;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64 sumwork = 0, curwork = 0;
    u64* res = NULL;
    u64 st1, nums1;
    u64* res1 = NULL;

    shared_ptr<CondData> condArg_1 = make_shared<CondData>();
    condArg_1->eneed = 1;
    condArg_1->vneed = 1;
    
    /* 得到真实顶点 */
    GetRealId(type, id, realid);
    /* 第一个顶点单独处理 */
    GNode fv;
    fv.level = 0;
    fv.vid = realid;
    fv.vtype = type;
    GetVInfo(fv.vinfo, realid);
    vis->SetValue(realid, 1);
    Adj->GetPrevVid(realid, res, nums, st);
    fv.oldid = strtoull(fv.vinfo.substr(0, fv.vinfo.find_first_of('|')).c_str(), NULL, 10);
    /* 第一跳入队 */
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]))
        {
            Adj->GetNextVid(res[i], res1, nums1, st1);
            u64 l = 0;
            while(res1[l] != realid)
            {
                ++st1;
                ++l;
            }
            SchQ->vec[stidx].vid = res[i];
            SchQ->vec[stidx].eid = st1;
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = &fv;
            SchQ->vec[stidx].cond = condArg_1.get();
            // SchQ->vec[stidx].cond.vneed = 1;
            // SchQ->vec[stidx].cond.eneed = 1;
            ++stidx;
            vis->SetValue(res[i], 1);
        }
        ++st;
    }
    while(shidx < stidx)
    {
        GNode &t = SchQ->vec[shidx];     
        ++shidx;
        if(t.level >= jns)
            break;
        Adj->GetPrevVid(t.vid, res, nums, st);
        for(i = 0;i < nums;++i)
        {
            if(!vis->GetValue(res[i]))
            {
                Adj->GetNextVid(res[i], res1, nums1, st1);
                u64 l = 0;
                while(res1[l] != t.vid)
                {
                    ++st1;
                    ++l;
                }
                GNode& tnode = SchQ->vec[stidx];
                tnode.vid = res[i];
                tnode.eid = st1;
                tnode.prev = &t;
                SchQ->vec[stidx].cond = condArg_1.get();
                // SchQ->vec[stidx].cond.vneed = 1;
                // SchQ->vec[stidx].cond.eneed = 1;

                ++stidx;
                vis->SetValue(res[i], 1);
            }
            ++st;
        }
    }

    u64 inidx = ResQ->tidx;
    vis->ReSet();
    shidx = stidx;
    u64 tmphidx = shidx;
    vis->SetValue(realid, 1);
    Adj->GetNextVid(realid, res, nums, st);
    for(i = 0;i < nums;++i)
    {
        if(!vis->GetValue(res[i]))
        {
            SchQ->vec[stidx].vid = res[i];
            SchQ->vec[stidx].eid = st;
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = &fv;
            SchQ->vec[stidx].cond = condArg_1.get();
            // SchQ->vec[stidx].cond.vneed = 1;
            // SchQ->vec[stidx].cond.eneed = 1;
            ++stidx;
            vis->SetValue(res[i], 1);
        }
        ++st;
    }
    while(shidx < stidx)
    {
        GNode &t = SchQ->vec[shidx];     
        ++shidx;
        if(t.level >= jns)
            break;
        Adj->GetNextVid(t.vid, res, nums, st);
        for(i = 0;i < nums;++i)
        {
            if(!vis->GetValue(res[i]))
            {
                GNode& tnode = SchQ->vec[stidx];
                tnode.vid = res[i];
                tnode.eid = st;
                tnode.prev = &t;
                tnode.level = t.level + 1;
                SchQ->vec[stidx].cond = condArg_1.get();
                // SchQ->vec[stidx].cond.vneed = 1;
                // SchQ->vec[stidx].cond.eneed = 1;

                ++stidx;
                vis->SetValue(res[i], 1);
            }
            ++st;
        }
    }

    shidx = 0;
    threads_work(SchQ, ResQ, shidx, stidx, threads, Que[0]->tasklock, Que[0]->reslock);

/* 资源释放部分 */
    if(res != NULL)
        delete [] res;

    cout<<fv.vinfo;
    cout<<"IN : "<<endl;
    for(u64 i = 0;i < inidx;++i)
    {
        string res("");
        DisplayDistance(ResQ->ptrv[i], res);
        cout<<res<<ResQ->ptrv[i]->vtype<<" : "<<ResQ->ptrv[i]->vinfo<<endl;
    }

    cout<<"OUT : "<<endl;
    for(u64 i = inidx;i < ResQ->tidx;++i)
    {
        string res("");
        DisplayDistance(ResQ->ptrv[i], res);
        cout<<res<<ResQ->ptrv[i]->vtype<<" : "<<ResQ->ptrv[i]->vinfo<<endl;
    }

/* 输出时间 */
}

void Graph::FindFriendsFun(u64 startPersonId, CondData* condArg, u64 minDistance, u64 maxDistance, u64& minDistanceSt)
{
    string type = "person";
    u64 realId;
    GetRealId(type, startPersonId, realId);
    InArray *vis = Que[0]->vis;
    NorQue *SchQ = Que[0]->SchQ;
    PtrQue *ResQ = Que[0]->ResQ;
    volatile u64 &shidx = SchQ->hidx, &stidx = SchQ->tidx;
    shidx = 0;
    stidx = 0;
    ResQ->hidx = 0;
    ResQ->tidx = 0;
    u64* res = NULL;
    u64 nums, st, i;
    vis->SetValue(realId, 1);
    Adj->GetNextVid(realId, res, nums, st);
    u64 personSt = vkinds[type].st, personEnd = personSt +  vkinds[type].n;
    for(i = 0;i < nums;++i){
        if(!vis->GetValue(res[i]) && res[i] >= personSt && res[i] < personEnd){
            SchQ->vec[stidx].vid = res[i];  
            SchQ->vec[stidx].eid = st;
            SchQ->vec[stidx].level = 1;
            SchQ->vec[stidx].prev = NULL;/* 不包括startPerson */
            SchQ->vec[stidx].cond = condArg;
            ++stidx; 
            vis->SetValue(res[i], 1);
        }
        else if(res[i] >= personEnd)
            break;
        ++st;
    }
    while(shidx < stidx){
        u64 prestidx = stidx;
        if(SchQ->vec[shidx].level == minDistance)
            minDistanceSt = shidx;
        if(SchQ->vec[shidx].level >= maxDistance)
            break;
        while(shidx < prestidx){
            GNode &t = SchQ->vec[shidx];   
            ++shidx;
            Adj->GetNextVid(t.vid, res, nums, st);
            for(i = 0;i < nums;++i){
                if(!vis->GetValue(res[i]) && res[i] >= personSt && res[i] < personEnd){
                    SchQ->vec[stidx].vid = res[i];
                    SchQ->vec[stidx].eid = st;
                    SchQ->vec[stidx].level = t.level + 1;
                    SchQ->vec[stidx].prev = &t;
                    SchQ->vec[stidx].cond = condArg;
                    ++stidx;
                    vis->SetValue(res[i], 1);
                }
                else if(res[i] >= personEnd)
                    break;
                ++st;
            }
        }
    }
}

void Graph::TypeExt(const string& type, const string& value, const string& col, u64& realId){
    string tmpValue;
    for(u64 i = vmap[type].st;i < vmap[type].st + vmap[type].bphf->nbKeys();++i){
        string tmpRes;
        GetVInfo(tmpValue, i);
        GetRowInfo(tmpRes, col, tmpValue, type);
        if(tmpRes == value){
            realId = i;
            break;
        }
    }
}

void Graph::TypeLoc(const string& type, const string& value, const string& col, u64& realId){
    realId = GetVLoc(type, col, value);
}

void Graph::TypeExtTest(string obj){
    string tagClass = obj.substr(0, obj.find_first_of(' ')), country = obj.substr(obj.find_first_of(' ') + 1);
    string placeType = "place", placeRow = "name";
    string tagClassType = "tagclass", tagClassRow = "name";
    u64 placeRealId = -1, tagClassRealId = -1;
    TypeExt(placeType, country, placeRow, placeRealId);
    TypeExt(tagClassType, tagClass, tagClassRow, tagClassRealId);
    // cout<<placeRealId<<' '<<tagClassRealId<<endl;
}

void Graph::TypeLocTest(string obj){
    string tagClass = obj.substr(0, obj.find_first_of(' ')), country = obj.substr(obj.find_first_of(' ') + 1);
    string placeType = "place", placeRow = "name";
    string tagClassType = "tagclass", tagClassRow = "name";
    u64 placeRealId = -1, tagClassRealId = -1;
    TypeLoc(placeType, country, placeRow, placeRealId);
    TypeLoc(tagClassType, tagClass, tagClassRow, tagClassRealId);
    // cout<<placeRealId<<' '<<tagClassRealId<<endl;
}



