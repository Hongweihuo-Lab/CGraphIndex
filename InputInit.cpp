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
#include"InputInit.h"
struct vhandle_arg
{
    vector<string *>* vec;
    vector<Keyvalue>* kv;
    boophf_t* bphf;
    MutexLock *lock;
    u64 st;
    u64 n;
    volatile u64 *finishworks;
};
void vhandle_fun(void* x)
{
    vhandle_arg* arg = (vhandle_arg*) x;
    vector<Keyvalue>* kv = arg->kv;
    vector<string *>* vec = arg->vec;
    for(u64 i = arg->st;i < arg->st + arg->n;++i)
        (*vec)[arg->bphf->lookup((*kv)[i].id)] = &(*kv)[i].s;
    {
        MutexLockGuard t(*arg->lock);
        ++(*arg->finishworks);
    }
    free(arg);
}

struct ehandle_arg
{
    vector<BigramS>* vec;
    vector<EdgeMap *>* v_v;
    boophf_t* lbphf;
    boophf_t* rbphf;
    MutexLock *lock;
    u64 st;
    u64 n;
    volatile u64 *finishworks;
    u64 etype;
    u64 rst;
    u64 isKnows;
};

void ehandle_fun(void* x)
{
    ehandle_arg* arg = (ehandle_arg*) x;
    vector<BigramS>* vec = arg->vec;
    vector<EdgeMap *>* v_v = arg->v_v;
    for(u64 i = arg->st;i < arg->st + arg->n;++i)
    {
        u64 idx = arg->lbphf->lookup((*vec)[i].v1);
        BigramS obj;
        obj.v1 = arg->rbphf->lookup((*vec)[i].v2) + arg->rst;
        obj.v2 = arg->etype;
        obj.s = (*vec)[i].s;
        {
            MutexLockGuard t((*v_v)[idx]->lock);
            (*v_v)[idx]->vec.push_back(obj);
        }
        if(arg->isKnows)
        {
            BigramS obj2;
            obj2.v1 = idx + arg->rst;
            obj2.v2 = arg->etype;
            obj2.s = (*vec)[i].s;
            {
                MutexLockGuard t((*v_v)[obj.v1 - arg->rst]->lock);
                (*v_v)[obj.v1 - arg->rst]->vec.push_back(obj2);
            }
        }
    }
    {
        MutexLockGuard t(*arg->lock);
        ++(*arg->finishworks);
    }
    free(arg);
}

struct sorthandle_arg
{
    vector<EdgeMap *>* v_v;
    MutexLock *lock;
    u64 st;
    u64 n;
    volatile u64 *finishworks;
};

void sorthandle_fun(void* x)
{
    sorthandle_arg* arg = (sorthandle_arg*) x;
    vector<EdgeMap *>* v_v = arg->v_v;
    u64 j = 0;
    for(u64 i = arg->st;i < arg->st + arg->n;++i)
        sort((*v_v)[i]->vec.begin(), (*v_v)[i]->vec.end(), [](BigramS& x, BigramS& y){return x.v1 <= y.v1;});
        // MergeSort((*v_v)[i]->vec, 0, (*v_v)[i]->vec.size() - 1);
    {
        MutexLockGuard t(*arg->lock);
        ++(*arg->finishworks);
    }
    free(arg);
}

struct sorthandle_arg_op
{
    vector<vector<BigramS> *>* v_v;
    MutexLock *lock;
    u64 st;
    u64 n;
    volatile u64 *finishworks;
};

void sorthandle_fun_op(void* x)
{
    sorthandle_arg_op* arg = (sorthandle_arg_op*) x;
    vector<vector<BigramS> *>* v_v = arg->v_v;
    u64 j = 0;
    for(u64 i = arg->st;i < arg->st + arg->n;++i)
        sort((*v_v)[i]->begin(), (*v_v)[i]->end(), [](BigramS& x, BigramS& y){return x.v1 <= y.v1;});
        // MergeSort((*v_v)[i]->vec, 0, (*v_v)[i]->vec.size() - 1);
    {
        MutexLockGuard t(*arg->lock);
        ++(*arg->finishworks);
    }
    free(arg);
}

PreProcess::PreProcess()
{}

PreProcess::PreProcess(string tmpdir)
{   
    cerr<<"------dir is : "<<tmpdir<<endl;
    string tmpsuf = ".csv";
    rootdir = tmpdir;
    objdir = tmpdir;
    suffix = tmpsuf;
    vector<string> res;
    vector<Fstring> tedgefiles;
    GetFiles(tmpdir, res);

    /* 文件大小 */
    u64 srcvsize = 0, srcesize = 0;
    struct stat64 statbuf;
    
    for(i64 i = 0;i < res.size();++i)
    {
        if(res[i].length() > 12 && res[i].find("updateStream") != string::npos)
        {
            Bstring tmp;
            tmp.s1 = res[i].substr(res[i].find_last_of('_') + 1, 
                                res[i].find_last_of('.') - res[i].find_last_of('_') - 1);
            tmp.s2 = res[i];
            ToSmall(tmp.s1);
            updatefiles.push_back(tmp);
            continue;
        }
        ifstream fin;
        fin.open(res[i].c_str(), ios::in);
        string head("");
        getline(fin, head);
        u64 pos = head.find_first_of('|');
        if(head.length() >= 2 && head.substr(0, 2) == "id")
        {
            Bstring tmp;
            string x = res[i].substr(res[i].find_last_of('/') + 1);
            tmp.s1 = x.substr(0, x.find_first_of('_'));
            tmp.s2 = res[i];
            ToSmall(tmp.s1);
            vertexfiles.push_back(tmp);
            if(stat64(res[i].c_str(), &statbuf) == 0)
                srcvsize += statbuf.st_size;
        }
            
        else if(pos != string::npos && pos >= 3 &&
            head.substr(pos - 3, 3) == ".id" && 
            head.find(".id", pos) != string::npos)
        {
            Fstring tmp;
            string x = head.substr(0, head.find_first_of('.'));
            tmp.s1 = x;
            x = head.substr(head.find_first_of('|') + 1);
            tmp.s2 = x.substr(0, x.find_first_of('.'));
            x = res[i].substr(res[i].find_last_of('/'));
            x = x.substr(x.find_first_of('_') + 1);
            tmp.s3 = x.substr(0, x.find_first_of('_'));
            tmp.s4 = res[i];
            ToSmall(tmp.s1);
            ToSmall(tmp.s2);
            ToSmall(tmp.s3);
            tedgefiles.push_back(tmp);
            if(stat64(res[i].c_str(), &statbuf) == 0)
                srcesize += statbuf.st_size;
        }
        else if(pos != string::npos && pos >= 3 &&
            head.substr(pos - 3, 3) == ".id")
        {
            Bstring tmp;
            string x = res[i].substr(res[i].find_last_of('/') + 1);
            u64 pos = x.find_first_of('_') + 1;
            tmp.s1 = x.substr(pos, x.find_first_of('_', pos));
            tmp.s2 = res[i];
            ToSmall(tmp.s1);
            rowfiles.push_back(tmp);
            if(stat64(res[i].c_str(), &statbuf) == 0)
                srcvsize += statbuf.st_size;
        }
        else
        {
            cerr<<"------"<<res[i]<<" Can't Identify"<<endl;
            exit(1);
        }
        fin.close();
    }

    /* 输出文件大小 */
    cout<<srcvsize<<endl;
    cout<<srcesize<<endl;

    sort(vertexfiles.begin(), vertexfiles.end(), [](Bstring x, Bstring y){return x.s1 <= y.s1;});
    sort(tedgefiles.begin(), tedgefiles.end(), [](Fstring x, Fstring y)
                                                {if(x.s1 != y.s1)
                                                    return x.s1 <= y.s1;
                                                else
                                                    return x.s2 <= y.s2;});
    sort(rowfiles.begin(), rowfiles.end(), [](Bstring x, Bstring y){return x.s1 <= y.s1;});
    sort(updatefiles.begin(), updatefiles.end(), [](Bstring x, Bstring y){return x.s1 <= y.s1;});
    
    CreateFiles(tedgefiles);
}
void PreProcess::GetFiles(string tmpdir, vector<string> &res)
{
    DIR *dir;
    struct dirent *ptr;

    if ((dir = opendir(tmpdir.c_str())) == NULL)
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
            if(pos != string::npos && tmp.substr(pos) == suffix)
                res.push_back(tmpdir + tmp);
        }
        else if (ptr->d_type == 10)
            continue;
        else if (ptr->d_type == 4)
            GetFiles(tmpdir + ptr->d_name + "/", res);
    }
    closedir(dir);
}


void PreProcess::InitRun(string tmpobj, u64 size_for_GB)
{
    if(tmpobj != "")
        objdir = tmpobj;
    cutsize = size_for_GB << 30;
    cerr<<"------objdir is : "<<objdir<<endl;
    cerr<<"------size of every file is about : "<<size_for_GB<<"GB"<<endl;
    /* LDBC的数据类型的预处理 */
    MapType();
    max_threads  = (u64)omp_get_max_threads();
    pool = new threadpool(max_threads / 2);
    pool->creat_thread();
    /* step1: vertex set*/
    cerr<<"------step1: vertex"<<endl;
    // HandleVertex();
timeval bb, ee;
gettimeofday(&bb, NULL);
    HandleVertex();
gettimeofday(&ee, NULL);
cout<<"Vertex Costs : "<<GetCostTime(bb, ee) * 1.0 / 1000000<<" sec"<<endl;
    cerr<<"------vertex is ok!"<<endl;
    /* step2: edge set*/
    cerr<<"------step2: edge"<<endl;
    // HandleEdge();
gettimeofday(&bb, NULL);
    HandleEdge();
gettimeofday(&ee, NULL);
cout<<"Edge Costs : "<<GetCostTime(bb, ee) * 1.0 / 1000000<<" sec"<<endl;
    cerr<<"------edge is ok!"<<endl;

    /* step3: uodate set*/
}


void PreProcess::HandleRow(vector<string*>& obj, boophf_t* bphf, string& head)
{
    for(u64 i = 0;i < rowfiles.size();++i)
    {
        cerr<<"------dealing with : "<<rowfiles[i].s2<<endl;
        vector<Keyvalue> kv;
        ifstream fin;
        fin.open(rowfiles[i].s2.c_str(), ios::in);
        u64 n = 0;
        string tmp("");
        getline(fin, tmp);
        head += tmp.substr(tmp.find_first_of('|'));
        while(getline(fin, tmp))
        {
            Keyvalue tkv;
            tkv.id = strtoull(tmp.substr(0, tmp.find_first_of('|')).c_str(), NULL, 10);
            tkv.s = tmp.substr(tmp.find_first_of('|') + 1);
            kv.push_back(tkv);
            ++n;
        }
        fin.close();
        u64 sum = obj.size();
        for(u64 i = 0;i < sum;++i)
            *obj[i] += '|';
        for(u64 i = 0;i < n;++i)
        {
            string& t = *obj[bphf->lookup(kv[i].id)];
            if(t.back() == '|')
                t += kv[i].s;
            else
            {
                t += ';';
                t += kv[i].s;
            }
        }
    }
}


void PreProcess::HandleVertex()
{
    u64 st = 0;
    vnums = 0;
    u64 vcurlen = 0;
    u64 vrows = 0;
    vindexrow.push_back(vrows);
    vlength = 0;
    u64 vfilenums = 1;
    string voutfn = objdir + "Vertex_" + to_string(vlength / cutsize);
    ofstream vfout;
    vfout.open(voutfn.c_str(), ios::out);
    u64 buffsize = cutsize + (1 << 20);
    string voput(buffsize, 0);   
    voput.resize(0);      
    for(u64 i = 0;i < vertexfiles.size();++i) 
    {
        cerr<<"------dealing with : "<<vertexfiles[i].s2<<endl;
        vector<Keyvalue> kv;
        ifstream fin;
        fin.open(vertexfiles[i].s2.c_str(), ios::in);
        u64 n = 0;
        string hrow("");
        pair<string, Vdetail> tvkinds;
        getline(fin, hrow);
        string tmp("");
        /* 读取文件部分 */
        while(getline(fin, tmp))
        {
            Keyvalue tkv;
            tkv.id = strtoull(tmp.substr(0, tmp.find_first_of('|')).c_str(), NULL, 10);
            tkv.s = tmp;
            kv.push_back(tkv);
            u64 len = tmp.length() + 1;
            ++n;
        }

        fin.close();
        tvkinds.second.n = n;
        tvkinds.first = vertexfiles[i].s1;
        tvkinds.second.st = st;
        pair<string, VertexMap> tvmap;
        tvmap.first = vertexfiles[i].s1;
        tvmap.second.st = st;
        st += n;
        vnums += n;
        u64 *tdata = new u64[n];
        for(u64 i = 0;i < n;++i)
            tdata[i] = kv[i].id;
        /* 顶点映射部分 */
        auto data_iterator = boomphf::range(tdata, tdata + n);

        // double gammaFactor = 2.0; // 倍率
        double gammaFactor = 1.0; // 倍率
        tvmap.second.bphf = new boomphf::mphf<u_int64_t, hasher_t>(n, data_iterator, max_threads, gammaFactor);
        vmap.insert(tvmap);
        delete [] tdata;
        /* 顶点映射完毕 */
        /* 写到文件 */
        vector<string *> tres(n);
        volatile u64 sendworks = 0, finishworks = 0;
        MutexLock vhandle_lock;
        u64 price = 500000; /* 默认每个线程50万 */
        u64 works = n / price; 
        u64 mod = n % price;
        for(u64 i = 0;i < works;++i)
        {
            vhandle_arg* arg = (vhandle_arg*)malloc(sizeof(vhandle_arg));
            arg->bphf = tvmap.second.bphf;
            arg->finishworks = &finishworks;
            arg->lock = &vhandle_lock;
            arg->kv = &kv;
            arg->n = price;
            arg->st = i * price;
            arg->vec = &tres;
            pool->addwork(arg, vhandle_fun);
            ++sendworks;
        }
        for(u64 i = works * price;i < n;++i)
            tres[tvmap.second.bphf->lookup(kv[i].id)] = &kv[i].s;
        while(1)
        {
            {
                MutexLockGuard t(vhandle_lock);
                if(finishworks == sendworks)
                    break;
            }
        }
        /* 判断是否是person顶点，因为需要附加列 */
        if(vertexfiles[i].s1 == "person")
            HandleRow(tres, tvmap.second.bphf, hrow);
        ToSmall(hrow);
        tvkinds.second.head = hrow; 
        GetHMap(tvkinds.second.hmap, tvkinds.second.head, '|');
        tvkinds.second.rows = tvkinds.second.hmap.size();
        vkinds.insert(tvkinds);
        /* 可以多线程 */
        for(u64 i = 0;i < n;++i)
        {
            u64 len = tres[i]->length();
            vcurlen += len + 1;
            voput += *tres[i];
            voput += '\n';
            ++vrows;
            if(vcurlen >= cutsize)
            {
                vlength += vcurlen;
                vfout<<voput;
                vfout.close();
                voutfn = objdir + "Vertex_" + to_string(vlength / cutsize);
                if(i < n - 1)
                {
                    vfout.open(voutfn.c_str(), ios::out);
                    ++vfilenums;
                }   
                vcurlen = 0;
                voput.resize(0);
                vindexrow.push_back(vrows);
            }
        }
    }
    if(vcurlen > 0)
    {
        vlength += vcurlen;
        vfout<<voput;
        vcurlen = 0;
    }
    vfout.close();
    vindexrow.push_back(vrows);
    /* 将辅助结构写到文件中 */
    SaveVrow();
    SaveVkinds();
    SaveVmap();

    cerr<<"------Vnums : "<<vnums<<endl;
    cerr<<"------Vlength : "<<vlength<<endl;
    cerr<<"------create "<<vfilenums<<" files, name as Vertex_X"<<endl;
}

void PreProcess::HandleVertexBanama()
{
    u64 st = 0;
    vnums = 0;
    u64 vcurlen = 0;
    u64 vrows = 0;
    vindexrow.push_back(vrows);
    vlength = 0;
    u64 vfilenums = 1;
    string voutfn = objdir + "Vertex_" + to_string(vlength / cutsize);
    ofstream vfout;
    vfout.open(voutfn.c_str(), ios::out);
    u64 buffsize = cutsize + (1 << 20);
    string voput(buffsize, 0);   
    voput.resize(0);      
    for(u64 i = 0;i < vertexfiles.size();++i){
        cerr<<"------dealing with : "<<vertexfiles[i].s2<<endl;
        vector<Keyvalue> kv;
        ifstream fin;
        fin.open(vertexfiles[i].s2.c_str(), ios::in);
        u64 n = 0;
        string hrow("");
        pair<string, Vdetail> tvkinds;
        getline(fin, hrow);
        string tmp("");
        /* 读取文件部分 */
        while(getline(fin, tmp)){
            Keyvalue tkv;
            tkv.id = strtoull(tmp.substr(0, tmp.find_first_of(',')).c_str(), NULL, 10);
            tkv.s = tmp;
            kv.push_back(tkv);
            u64 len = tmp.length() + 1;
            ++n;
        }

        fin.close();
        tvkinds.second.n = n;
        tvkinds.first = vertexfiles[i].s1;
        tvkinds.second.st = st;
        pair<string, VertexMap> tvmap;
        tvmap.first = vertexfiles[i].s1;
        tvmap.second.st = st;
        st += n;
        vnums += n;
        u64 *tdata = new u64[n];
        for(u64 i = 0;i < n;++i)
            tdata[i] = kv[i].id;
        /* 顶点映射部分 */
        auto data_iterator = boomphf::range(tdata, tdata + n);
        double gammaFactor = 1.0;
        tvmap.second.bphf = new boomphf::mphf<u_int64_t, hasher_t>(n, data_iterator, max_threads, gammaFactor);
        vmap.insert(tvmap);
        delete [] tdata;
        /* 顶点映射完毕 */
        /* 写到文件 */
        vector<string *> tres(n);
        volatile u64 sendworks = 0, finishworks = 0;
        MutexLock vhandle_lock;
        u64 price = 500000; /* 默认每个线程50万 */
        u64 works = n / price; 
        u64 mod = n % price;
        for(u64 i = 0;i < works;++i)
        {
            vhandle_arg* arg = (vhandle_arg*)malloc(sizeof(vhandle_arg));
            arg->bphf = tvmap.second.bphf;
            arg->finishworks = &finishworks;
            arg->lock = &vhandle_lock;
            arg->kv = &kv;
            arg->n = price;
            arg->st = i * price;
            arg->vec = &tres;
            pool->addwork(arg, vhandle_fun);
            ++sendworks;
        }
        for(u64 i = works * price;i < n;++i)
            tres[tvmap.second.bphf->lookup(kv[i].id)] = &kv[i].s;
        while(1)
        {
            {
                MutexLockGuard t(vhandle_lock);
                if(finishworks == sendworks)
                    break;
            }
        }
        ToSmall(hrow);
        tvkinds.second.head = hrow; 
        GetHMap(tvkinds.second.hmap, tvkinds.second.head, ',');
        tvkinds.second.rows = tvkinds.second.hmap.size();
        vkinds.insert(tvkinds);

        /* 可以多线程 */
        for(u64 i = 0;i < n;++i){
            u64 len = tres[i]->length();
            vcurlen += len + 1;
            voput += *tres[i];
            voput += '\n';
            ++vrows;
            if(vcurlen >= cutsize)
            {
                vlength += vcurlen;
                vfout<<voput;
                vfout.close();
                voutfn = objdir + "Vertex_" + to_string(vlength / cutsize);
                if(i < n - 1)
                {
                    vfout.open(voutfn.c_str(), ios::out);
                    ++vfilenums;
                }   
                vcurlen = 0;
                voput.resize(0);
                vindexrow.push_back(vrows);
            }
        }
    }

    if(vcurlen > 0)
    {
        vlength += vcurlen;
        vfout<<voput;
        vcurlen = 0;
    }

    vfout.close();
    vindexrow.push_back(vrows);
    /* 将辅助结构写到文件中 */
    SaveVrow();

    SaveVkinds();

    SaveVmap();

    cerr<<"------Vnums : "<<vnums<<endl;
    cerr<<"------Vlength : "<<vlength<<endl;
    cerr<<"------create "<<vfilenums<<" files, name as Vertex_X"<<endl;

}

void PreProcess::BanamaInit(string tmpdir, string tmpsuf, string tmpobj, u64 size_for_GB)
{
    cerr<<"------dir is : "<<tmpdir<<endl;
    cerr<<"------suffix is : "<<tmpsuf<<endl;
    rootdir = tmpdir;
    objdir = tmpdir;
    suffix = tmpsuf;
    DIR *dir;
    struct dirent *ptr;
    vector<string> res;

    if ((dir = opendir(tmpdir.c_str())) == NULL)
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
            res.push_back(tmpdir + tmp);   
        }
        else if (ptr->d_type == 10)
            continue;
    }
    closedir(dir);

    /* 文件大小 */
    u64 srcvsize = 0, srcesize = 0;
    struct stat64 statbuf;
    vector<Fstring> eFile;

    for(i64 i = 0;i < res.size();++i){
        string tmp = res[i].substr(res[i].find_last_of('/') + 1);
        if(tmp.substr(0, tmp.find_first_of('-')) == "nodes"){
            Bstring vTmp;
            string x = tmp.substr(tmp.find_first_of('-') + 1);
            vTmp.s1 = x.substr(0, x.find_first_of('.'));
            vTmp.s2 = res[i];
            ToSmall(vTmp.s1);
            vertexfiles.push_back(vTmp);
            if(stat64(res[i].c_str(), &statbuf) == 0)
                srcvsize += statbuf.st_size;
        }
        else if(tmp.substr(0, tmp.find_first_of('-')) == "edges"){
            Fstring eTmp;
            string x = tmp.substr(tmp.find_first_of('-') + 1);
            eTmp.s1 = x.substr(0, x.find_first_of('-'));
            x = x.substr(x.find_first_of('-') + 1);
            eTmp.s3 = x.substr(0, x.find_first_of('-'));
            x = x.substr(x.find_first_of('-') + 1);
            eTmp.s2 = x.substr(0, x.find_first_of('.'));
            eTmp.s4 = res[i];
            ToSmall(eTmp.s1);
            ToSmall(eTmp.s2);
            ToSmall(eTmp.s3);
            eFile.push_back(eTmp);
            if(stat64(res[i].c_str(), &statbuf) == 0)
                srcesize += statbuf.st_size;
        }  
    }

    /* 输出文件大小 */
    cout<<srcvsize<<endl;
    cout<<srcesize<<endl;

    sort(vertexfiles.begin(), vertexfiles.end(), [](Bstring& x, Bstring& y){
                                            return x.s1 < y.s1;
                                        });
    sort(eFile.begin(), eFile.end(), [](Fstring& x, Fstring& y){
                                            if(x.s1 != y.s1)
                                                return x.s1 < y.s1;
                                            else 
                                                return x.s2 < y.s2;
                                        });

// for(u64 i = 0;i < vFile.size();++i){
//     cerr<<vFile[i].s1<<'\t'<<vFile[i].s2<<endl;
// }
// for(u64 i = 0;i < eFile.size();++i){
//     cerr<<eFile[i].s1<<'\t'<<eFile[i].s2<<'\t'<<eFile[i].s3<<'\t'<<eFile[i].s4<<endl;
// }

//     exit(0);

    etypesum = eFile.size();
    pair<string, vector<Tstring> > tedge;
    Tstring ttstring;
    tedge.first = eFile[0].s1;
    ttstring.s1 = eFile[0].s2;
    ttstring.s2 = eFile[0].s3;
    ttstring.s3 = eFile[0].s4;
    tedge.second.push_back(ttstring);
    for(u64 i = 1;i < eFile.size();++i)
    {
        if(eFile[i].s1 == tedge.first)
        {
            ttstring.s1 = eFile[i].s2;
            ttstring.s2 = eFile[i].s3;
            ttstring.s3 = eFile[i].s4;
            tedge.second.push_back(ttstring);
        }
        else
        {
            edgefiles.insert(tedge);
            tedge.first = eFile[i].s1;
            vector<Tstring>().swap(tedge.second);
            ttstring.s1 = eFile[i].s2;
            ttstring.s2 = eFile[i].s3;
            ttstring.s3 = eFile[i].s4;
            tedge.second.push_back(ttstring);
        }
    }
    edgefiles.insert(tedge);

// for(auto p = edgefiles.begin();p != edgefiles.end();++p){
//     cerr<<p->first<<endl;
//     for(u64 i = 0;i < p->second.size();++i){
//         cerr<<p->second[i].s1<<'\t'<<p->second[i].s2<<endl;
//     }
//     cerr<<"------------------------"<<endl;
// }
// exit(0);

    objdir = tmpobj;
    cutsize = size_for_GB << 30;
    cerr<<"------objdir is : "<<objdir<<endl;
    cerr<<"------size of every file is about : "<<size_for_GB<<"GB"<<endl;
    /* LDBC的数据类型的预处理 */
    MapTypeBanama();
    max_threads  = (u64)omp_get_max_threads();
    pool = new threadpool(max_threads / 2);
    pool->creat_thread();
    /* step1: vertex set*/
    cerr<<"------step1: vertex"<<endl;
    timeval bb, ee;
gettimeofday(&bb, NULL);
    HandleVertexBanama();
gettimeofday(&ee, NULL);
cout<<"Vertex Costs : "<<GetCostTime(bb, ee) * 1.0 / 1000000<<" sec"<<endl;
    cerr<<"------vertex is ok!"<<endl;
    /* step2: edge set*/
    cerr<<"------step2: edge"<<endl;
    // HandleEdge();
gettimeofday(&bb, NULL);
    HandleEdgeBanama();
gettimeofday(&ee, NULL);
cout<<"Edge Costs : "<<GetCostTime(bb, ee) * 1.0 / 1000000<<" sec"<<endl;
    cerr<<"------edge is ok!"<<endl;

    /* step3: uodate set*/
}

void PreProcess::SaveVrow()
{
    string fn(objdir + "Vrows.myg");
    ofstream fout;
    fout.open(fn.c_str(), ios::out);
    for(u64 i = 0;i < vindexrow.size();++i)
        fout<<vindexrow[i]<<endl;
    fout.close();
    vector<u64>().swap(vindexrow);
}

void PreProcess::SaveVkinds()
{
    string fn(objdir + "Vkinds.myg");
    ofstream fout;
    fout.open(fn.c_str(), ios::out);
    for(map<string, Vdetail>::iterator i = vkinds.begin();i != vkinds.end();++i)
    {
        fout<<i->first<<endl;
        fout<<i->second.head<<endl;
        fout<<i->second.rows<<endl;
        for(auto p = i->second.hmap.begin();p != i->second.hmap.end();++p)
        {
            fout<<p->first<<endl;
            fout<<p->second.v1<<endl;
            fout<<p->second.v2<<endl;
        }
        fout<<i->second.n<<endl;
        fout<<i->second.st<<endl;
    }
    fout.close();
}

void PreProcess::SaveVmap()
{
    string fn(objdir + "Vmap.myg");
    ofstream fout;
    fout.open(fn.c_str(), ios::out);
    fout<<vmap.size()<<endl;
    for(auto i = vmap.begin();i != vmap.end();++i)
    {
        fout<<i->first<<endl;
        fout<<i->second.st<<endl;
        i->second.bphf->save(fout);
    }
    fout.close();
}


void PreProcess::HandleEdge()
{
    elength = 0;
    u64 efilenums = 1;
    string eoutfn = objdir + "Edge_" + to_string(elength / cutsize);
    ofstream efout;
    efout.open(eoutfn.c_str(), ios::out);
    enums = 0;
    u64 erows = 0;
    eindexrow.push_back(erows);
    u64 ecurlen = 0;

    string eoput("");
    string adjfn = objdir + "TmpAdjList.myg";
    FILE* adjfp;
    adjfp = fopen(adjfn.c_str(), "w");

    u64 typenum;
    string temap;
    u64 totale = edgefiles.size();
    InArray *thasdes[totale] = {NULL};
    InArray *tetype[totale] = {NULL};
    u64 etypeidx = 0;

    u64 tsize = 1ull << 32; /* 4G个u64的缓冲区 */
    u64 *tan = new u64[tsize];
    u64 tidx = 0;
    memset(tan, 0, 8 * tsize);
    for(map<string, vector<Tstring> >::iterator i = edgefiles.begin();i != edgefiles.end();++i)
    {
        u64 tfvs = vkinds[i->first].n;
        u64 voff = vkinds[i->first].st;
        vector<EdgeMap *> v_v(tfvs);
        for(u64 i = 0;i < tfvs;++i)
            v_v[i] = new EdgeMap;
        u64 sum_type = 0;

        u64 fileidx = 0; //临时文件的标号

        /* 同一类型顶点的文件 */
        for(u64 j = 0;j < i->second.size();++j)
        {
            cerr<<"------dealing with : "<<i->second[j].s3<<endl;
            vector<BigramS> bs;
            ifstream fin;
            fin.open(i->second[j].s3.c_str(), ios::in);
            u64 n = 0;
            string tmp("");
            pair<string, Especial> tdetail;
            string ekindshead = i->first + string("->") + i->second[j].s1;
            boophf_t* lbphf = vmap[i->first].bphf;
            boophf_t* rbphf = vmap[i->second[j].s1].bphf;
            u64 rst = vmap[i->second[j].s1].st;
            tdetail.first = i->second[j].s2;
            u64 isKnows = tdetail.first == "knows" ? 1 : 0;
            if(!emap.count(tdetail.first))
            {
                typenum = emap.size();
                emap[tdetail.first] = typenum;
            }
            else
                typenum = emap[tdetail.first];
            getline(fin, tmp);
            ToSmall(tmp);
            bool eflag = false;
            if(tmp.find_first_of('|') != tmp.find_last_of('|')) /* 边有说明*/
            {
                string x = tmp.substr(tmp.find_first_of('|') + 1);
                tdetail.second.head = x.substr(x.find_first_of('|') + 1);
                eflag = true;
            }
            else
                tdetail.second.head = "";
            tdetail.second.hasexp = eflag;
            if(eflag)
            {
                GetHMap(tdetail.second.hmap, tdetail.second.head, '|');
                tdetail.second.rows = tdetail.second.hmap.size();
            }
                
            while(getline(fin, tmp))
            {
                BigramS tbs;
                tbs.s = "";
                tbs.v1 = strtoull(tmp.substr(0, tmp.find_first_of('|')).c_str(), NULL, 10);
                if(eflag)
                {
                    string x = tmp.substr(tmp.find_first_of('|') + 1);
                    tbs.v2 = strtoull(x.substr(0, x.find_first_of('|')).c_str(), NULL, 10);
                    tbs.s = x.substr(x.find_first_of('|') + 1);
                }
                else
                    tbs.v2 = strtoull(tmp.substr(tmp.find_first_of('|') + 1).c_str(), NULL, 10);
                bs.push_back(tbs);
                ++n;
            }
            fin.close();

            enums += n;
            sum_type += n;
            tdetail.second.n = n;
            if(isKnows)
            {
                enums += n;
                sum_type += n;
                tdetail.second.n += n;
            }
            if(ekinds.count(ekindshead)) 
                ++ekinds[ekindshead].n;
            else
                ekinds[ekindshead].n = 1;
            ekinds[ekindshead].detail.insert(tdetail);
            /* 边一一映射部分 */
            volatile u64 sendworks = 0, finishworks = 0;
            MutexLock ehandle_lock;
            u64 price = 500000; /* 默认每个线程50万 */
            u64 works = n / price; 
            u64 mod = n % price;
            for(u64 i = 0;i < works;++i)
            {
                ehandle_arg* arg = (ehandle_arg*)malloc(sizeof(ehandle_arg));
                arg->lbphf = lbphf;
                arg->rbphf = rbphf;
                arg->finishworks = &finishworks;
                arg->lock = &ehandle_lock;
                arg->vec = &bs;
                arg->n = price;
                arg->st = i * price;
                arg->v_v = &v_v;
                arg->etype = typenum;
                arg->rst = rst;
                arg->isKnows = isKnows;
                pool->addwork(arg, ehandle_fun);
                ++sendworks;
            }
            for(u64 i = works * price;i < n;++i)
            // for(u64 i = 0;i < n;++i)
            {
                u64 idx = lbphf->lookup(bs[i].v1);
                BigramS obj;
                obj.v1 = rbphf->lookup(bs[i].v2) + rst;
                obj.v2 = typenum;
                obj.s = bs[i].s;
                {
                    MutexLockGuard t(v_v[idx]->lock);
                    v_v[idx]->vec.push_back(obj);
                }
                if(isKnows)
                {
                    BigramS obj2;
                    obj2.v1 = idx + rst;
                    obj2.v2 = typenum;
                    obj2.s = bs[i].s;
                    {
                        MutexLockGuard t(v_v[obj.v1 - rst]->lock);
                        v_v[obj.v1 - rst]->vec.push_back(obj2);
                    }
                }
            }
            while(1)
            {
                {
                    MutexLockGuard t(ehandle_lock);
                    if(finishworks == sendworks)
                        break;
                }
            }
            /* 写到文件 */
            string fn = objdir + i->first + "_" + to_string(fileidx);
            ofstream *fout = new ofstream(fn, ofstream::binary);
            for(u64 i = 0;i < tfvs;++i)
            {
                u64 len = v_v[i]->vec.size();
                fout->write((char*)&len, sizeof(u64));
                for(u64 j = 0;j < len;++j)
                {
                    fout->write((char*)&v_v[i]->vec[j].v1, sizeof(u64));
                    fout->write((char*)&v_v[i]->vec[j].v2, sizeof(u64));
                    u64 slen = v_v[i]->vec[j].s.length();
                    fout->write((char*)&slen, sizeof(u64));

                    if(slen > 0)
                        fout->write(v_v[i]->vec[j].s.data(), slen * sizeof(char));
                }
                v_v[i]->vec.resize(0);
            }
            ++fileidx;
            fout->close();
            delete fout;

        }

        for(u64 i = 0;i < tfvs;++i)
            delete v_v[i];
        /* 从文件读取 */
        vector<ifstream*> fin(fileidx, NULL);
        vector<vector<BigramS>* > e_e(tfvs);
        vector<u64> nums(fileidx, 0); 
        u64 price = 500000; /* 默认每个线程排序500000 */
        u64 workst = 0;
        volatile u64 sendworks = 0, finishworks = 0;
        MutexLock sorthandle_lock;
        for(u64 j = 0;j < fileidx;++j)
        {
            string fn = objdir + i->first + "_" + to_string(j);
            fin[j] = new ifstream(fn, ifstream::binary);
        }
        
        for(u64 i = 0;i < tfvs;++i)
        {
            u64 len = 0;
            for(u64 j = 0;j < fileidx;++j)
            {
                fin[j]->read((char*)&nums[j], sizeof(u64));
                len += nums[j];
            }
            e_e[i] = new vector<BigramS>(len);
            u64 idx = 0;
            for(u64 j = 0;j < fileidx;++j)
            {
                for(u64 k = 0;k < nums[j];++k)
                {
                    BigramS t;
                    fin[j]->read((char*)&t.v1, sizeof(u64));
                    fin[j]->read((char*)&t.v2, sizeof(u64));
                    u64 sf = 0;
                    fin[j]->read((char*)&sf, sizeof(u64));
                    if(sf > 0)
                    {
                        char *ts = new char[sf + 1];
                        fin[j]->read(ts, sf * sizeof(char));
                        ts[sf] = '\0';
                        t.s = ts;
                        delete [] ts;
                    }
                    (*e_e[i])[idx] = t;
                    ++idx;
                }
            }
            /* 排序部分 */
            if((i + 1) % price == 0)
            {
                sorthandle_arg_op* arg = (sorthandle_arg_op*)malloc(sizeof(sorthandle_arg_op));
                arg->finishworks = &finishworks;
                arg->lock = &sorthandle_lock;
                arg->n = price;
                arg->st = workst * price;
                arg->v_v = &e_e;
                pool->addwork(arg, sorthandle_fun_op);
                ++sendworks;
                ++workst;
            }
            else if(i + 1 == tfvs) 
            {
                for(u64 p = workst * price;p < tfvs;++p)
                    sort(e_e[p]->begin(), e_e[p]->end(), [](BigramS& x, BigramS& y){return x.v1 <= y.v1;});
            }
        }

        for(u64 j = 0;j < fileidx;++j)
        {
            string fn = objdir + i->first + "_" + to_string(j);
            fin[j]->close();
            delete fin[j];
            remove(fn.c_str());
        }

        while(1)
        {
            {
                MutexLockGuard t(sorthandle_lock);
                if(finishworks == sendworks)
                    break;
            }
        }

        /* 边类型和说明 */
        /* 写到文件 */
        thasdes[etypeidx] = new InArray(sum_type, 1);
        tetype[etypeidx] = new InArray(sum_type, blog(etypesum)); /* etypesum肯定大于等于typenum，最后按照typenum分配内存 */
        u64 curidx = 0;

        for(u64 i = 0;i < tfvs;++i)
        {
            tan[tidx] = voff + i;
            tan[tidx + 1] = e_e[i]->size();
            u64 lsize = tan[tidx + 1];
            tidx += 2;
            for(u64 j = 0;j < lsize;++j, ++tidx)
            {
                tan[tidx] = (*e_e[i])[j].v1;
                if((*e_e[i])[j].s != "")
                {
                    thasdes[etypeidx]->SetValue(curidx, 1);
                    eoput += (*e_e[i])[j].s;
                    eoput += "\n";
                    ecurlen += (*e_e[i])[j].s.length() + 1;
                    ++erows;
                }
                tetype[etypeidx]->SetValue(curidx, (*e_e[i])[j].v2);
                ++curidx;
                if(ecurlen >= cutsize)
                {
                    efout<<eoput;
                    efout.close();
                    elength += ecurlen;
                    eoutfn = objdir + "Edge_" + to_string(elength / cutsize);
                    efout.open(eoutfn.c_str(), ios::out);
                    ++efilenums;
                    ecurlen = 0;
                    eoput.resize(0);
                    eindexrow.push_back(erows);
                }
            }
            delete e_e[i];
            if(tidx >= (tsize >> 1))
            {
                fwrite(tan, 8, tidx, adjfp);
                tidx = 0;
            }
        }
        ++etypeidx;
    }
    if(tidx)
        fwrite(tan, 8, tidx, adjfp);
    delete [] tan;
    fwrite(&vnums, 8, 1, adjfp);
    fwrite(&enums, 8, 1, adjfp);
    fclose(adjfp);

    if(ecurlen == 0)
    {
        efout.close();
        remove(eoutfn.c_str());
    }
    else
    {
        efout<<eoput;
        elength += ecurlen;
        ecurlen = 0;
    }
    efout.close();
    eindexrow.push_back(erows);
    string().swap(eoput);

    /* 边类型和说明进行总结 */
    hasdes = new InArray(enums, 1);
    etype = new InArray(enums, blog(typenum));
    u64 eidx = 0;
    for(u64 i = 0;i < etypeidx;++i)
    {
        u64 len = thasdes[i]->GetNum();
        for(u64 j = 0;j < len;++j)
        {
            hasdes->SetValue(eidx, thasdes[i]->GetValue(j));
            etype->SetValue(eidx, tetype[i]->GetValue(j));
            ++eidx;
        }
    }
    hasdes->constructrank(256, 16);

    SaveErow();
    SaveEkinds();
    SaveEmap();
    SaveEtype();
    SaveEHasdes();
    cerr<<"------Enums : "<<enums<<endl;
    cerr<<"------Elength : "<<elength<<endl;
    cerr<<"------create "<<efilenums<<" files, name as Edge_X"<<endl;
}


void PreProcess::HandleEdgeBanama()
{
    elength = 0;
    u64 efilenums = 1;
    string eoutfn = objdir + "Edge_" + to_string(elength / cutsize);
    ofstream efout;
    efout.open(eoutfn.c_str(), ios::out);
    enums = 0;
    u64 erows = 0;
    eindexrow.push_back(erows);
    u64 ecurlen = 0;

    string eoput("");
    string adjfn = objdir + "TmpAdjList.myg";
    FILE* adjfp;
    adjfp = fopen(adjfn.c_str(), "w");

    u64 typenum;
    string temap;
    u64 totale = edgefiles.size();
    InArray *thasdes[totale] = {NULL};
    InArray *tetype[totale] = {NULL};
    u64 etypeidx = 0;

    u64 tsize = 1ull << 24; /* 4G个u64的缓冲区 */
    u64 *tan = new u64[tsize];
    u64 tidx = 0;
    memset(tan, 0, 8 * tsize);
    for(map<string, vector<Tstring> >::iterator i = edgefiles.begin();i != edgefiles.end();++i){
        u64 tfvs = vkinds[i->first].n;
        u64 voff = vkinds[i->first].st;
        vector<EdgeMap *> v_v(tfvs);
        for(u64 i = 0;i < tfvs;++i)
            v_v[i] = new EdgeMap;
        u64 sum_type = 0;

        u64 fileidx = 0; //临时文件的标号

        /* 同一类型顶点的文件 */
        for(u64 j = 0;j < i->second.size();++j){
            cerr<<"------dealing with : "<<i->second[j].s3<<endl;
            vector<BigramS> bs;
            ifstream fin;
            fin.open(i->second[j].s3.c_str(), ios::in);
            u64 n = 0;
            string tmp("");
            pair<string, Especial> tdetail;
            string ekindshead = i->first + string("->") + i->second[j].s1;
            boophf_t* lbphf = vmap[i->first].bphf;
            boophf_t* rbphf = vmap[i->second[j].s1].bphf;
            u64 rst = vmap[i->second[j].s1].st;
            tdetail.first = i->second[j].s2;
            if(!emap.count(tdetail.first)){
                typenum = emap.size();
                emap[tdetail.first] = typenum;
            }
            else
                typenum = emap[tdetail.first];
            getline(fin, tmp);
            ToSmall(tmp);
            bool eflag = false;
            if(tmp.find_first_of(',') != tmp.find_last_of(',')){ /* 边有说明*/
                string x = tmp.substr(tmp.find_first_of(',') + 1);
                tdetail.second.head = x.substr(x.find_first_of(',') + 1);
                eflag = true;
            }
            else
                tdetail.second.head = "";
            tdetail.second.hasexp = eflag;
            if(eflag){
                GetHMap(tdetail.second.hmap, tdetail.second.head, ',');
                tdetail.second.rows = tdetail.second.hmap.size();
            }

            while(getline(fin, tmp)){
                BigramS tbs;
                tbs.s = "";
                tbs.v1 = strtoull(tmp.substr(0, tmp.find_first_of(',')).c_str(), NULL, 10);
                if(eflag){
                    string x = tmp.substr(tmp.find_first_of(',') + 1);
                    tbs.v2 = strtoull(x.substr(0, x.find_first_of(',')).c_str(), NULL, 10);
                    tbs.s = x.substr(x.find_first_of(',') + 1);
                }
                else
                    tbs.v2 = strtoull(tmp.substr(tmp.find_first_of(',') + 1).c_str(), NULL, 10);
                bs.push_back(tbs);
                ++n;
            }
            fin.close();

            enums += n;
            sum_type += n;
            tdetail.second.n = n;
            if(ekinds.count(ekindshead)) 
                ++ekinds[ekindshead].n;
            else
                ekinds[ekindshead].n = 1;
            ekinds[ekindshead].detail.insert(tdetail);
            /* 边一一映射部分 */
            volatile u64 sendworks = 0, finishworks = 0;
            MutexLock ehandle_lock;
            u64 price = 500000; /* 默认每个线程50万 */
            u64 works = n / price; 
            u64 mod = n % price;
            for(u64 i = 0;i < works;++i){
                ehandle_arg* arg = (ehandle_arg*)malloc(sizeof(ehandle_arg));
                arg->lbphf = lbphf;
                arg->rbphf = rbphf;
                arg->finishworks = &finishworks;
                arg->lock = &ehandle_lock;
                arg->vec = &bs;
                arg->n = price;
                arg->st = i * price;
                arg->v_v = &v_v;
                arg->etype = typenum;
                arg->rst = rst;
                arg->isKnows = 0;
                pool->addwork(arg, ehandle_fun);
                ++sendworks;
            }
            for(u64 i = works * price;i < n;++i){
                u64 idx = lbphf->lookup(bs[i].v1);
                BigramS obj;
                obj.v1 = rbphf->lookup(bs[i].v2) + rst;
                obj.v2 = typenum;
                obj.s = bs[i].s;
                {
                    MutexLockGuard t(v_v[idx]->lock);
                    v_v[idx]->vec.push_back(obj);
                }
            }
            while(1){
                {
                    MutexLockGuard t(ehandle_lock);
                    if(finishworks == sendworks)
                        break;
                }
            }
            /* 写到文件 */
            string fn = objdir + i->first + "_" + to_string(fileidx);
            ofstream *fout = new ofstream(fn, ofstream::binary);
            for(u64 i = 0;i < tfvs;++i){
                u64 len = v_v[i]->vec.size();
                fout->write((char*)&len, sizeof(u64));
                for(u64 j = 0;j < len;++j){
                    fout->write((char*)&v_v[i]->vec[j].v1, sizeof(u64));
                    fout->write((char*)&v_v[i]->vec[j].v2, sizeof(u64));
                    u64 slen = v_v[i]->vec[j].s.length();
                    fout->write((char*)&slen, sizeof(u64));

                    if(slen > 0)
                        fout->write(v_v[i]->vec[j].s.data(), slen * sizeof(char));
                }
                v_v[i]->vec.resize(0);
            }
            ++fileidx;
            fout->close();
            delete fout;

        }

        for(u64 i = 0;i < tfvs;++i)
            delete v_v[i];
        /* 从文件读取 */
        vector<ifstream*> fin(fileidx, NULL);
        vector<vector<BigramS>* > e_e(tfvs);
        vector<u64> nums(fileidx, 0); 

        for(u64 j = 0;j < fileidx;++j){
            string fn = objdir + i->first + "_" + to_string(j);
            fin[j] = new ifstream(fn, ifstream::binary);
        }

        for(u64 i = 0;i < tfvs;++i){
            u64 len = 0;
            for(u64 j = 0;j < fileidx;++j){
                fin[j]->read((char*)&nums[j], sizeof(u64));
                len += nums[j];
            }
            e_e[i] = new vector<BigramS>(len);
            u64 idx = 0;
            for(u64 j = 0;j < fileidx;++j){
                for(u64 k = 0;k < nums[j];++k){
                    BigramS t;
                    fin[j]->read((char*)&t.v1, sizeof(u64));
                    fin[j]->read((char*)&t.v2, sizeof(u64));
                    u64 sf = 0;
                    fin[j]->read((char*)&sf, sizeof(u64));
                    if(sf > 0){
                        char *ts = new char[sf + 1];
                        fin[j]->read(ts, sf * sizeof(char));
                        ts[sf] = '\0';
                        t.s = ts;
                        delete [] ts;
                    }
                    (*e_e[i])[idx] = t;
                    ++idx;
                }
            }
        }      

        for(u64 p = 0;p < tfvs;++p)
            sort(e_e[p]->begin(), e_e[p]->end(), [](BigramS& x, BigramS& y){return x.v1 < y.v1;});

        for(u64 j = 0;j < fileidx;++j){
            string fn = objdir + i->first + "_" + to_string(j);
            fin[j]->close();
            delete fin[j];
            remove(fn.c_str());
        }

        /* 边类型和说明 */
        /* 写到文件 */
        thasdes[etypeidx] = new InArray(sum_type, 1);
        tetype[etypeidx] = new InArray(sum_type, blog(etypesum)); /* etypesum肯定大于等于typenum，最后按照typenum分配内存 */
        u64 curidx = 0;

        for(u64 i = 0;i < tfvs;++i){
            tan[tidx] = voff + i;
            tan[tidx + 1] = e_e[i]->size();
            u64 lsize = tan[tidx + 1];
            tidx += 2;
            for(u64 j = 0;j < lsize;++j, ++tidx){
                tan[tidx] = (*e_e[i])[j].v1;
                if((*e_e[i])[j].s != ""){
                    thasdes[etypeidx]->SetValue(curidx, 1);
                    eoput += (*e_e[i])[j].s;
                    eoput += "\n";
                    ecurlen += (*e_e[i])[j].s.length() + 1;
                    ++erows;
                }
                tetype[etypeidx]->SetValue(curidx, (*e_e[i])[j].v2);
                ++curidx;
                if(ecurlen >= cutsize){
                    efout<<eoput;
                    efout.close();
                    elength += ecurlen;
                    eoutfn = objdir + "Edge_" + to_string(elength / cutsize);
                    efout.open(eoutfn.c_str(), ios::out);
                    ++efilenums;
                    ecurlen = 0;
                    eoput.resize(0);
                    eindexrow.push_back(erows);
                }
            }
            delete e_e[i];
            if(tidx >= (tsize >> 1)){
                fwrite(tan, 8, tidx, adjfp);
                tidx = 0;
            }
        }
        ++etypeidx;
    }
    if(tidx)
        fwrite(tan, 8, tidx, adjfp);
    delete [] tan;
    fwrite(&vnums, 8, 1, adjfp);
    fwrite(&enums, 8, 1, adjfp);
    fclose(adjfp);

    if(ecurlen == 0){
        efout.close();
        remove(eoutfn.c_str());
    }
    else{
        efout<<eoput;
        elength += ecurlen;
        ecurlen = 0;
    }
    efout.close();
    eindexrow.push_back(erows);
    string().swap(eoput);

    /* 边类型和说明进行总结 */
    hasdes = new InArray(enums, 1);
    etype = new InArray(enums, blog(typenum));
    u64 eidx = 0;
    for(u64 i = 0;i < etypeidx;++i){
        u64 len = thasdes[i]->GetNum();
        for(u64 j = 0;j < len;++j){
            hasdes->SetValue(eidx, thasdes[i]->GetValue(j));
            etype->SetValue(eidx, tetype[i]->GetValue(j));
            ++eidx;
        }
    }

    hasdes->constructrank(256, 16);

    SaveErow();

    SaveEkinds();

    SaveEmap();

    SaveEtype();

    SaveEHasdes();
    cerr<<"------Enums : "<<enums<<endl;
    cerr<<"------Elength : "<<elength<<endl;
    cerr<<"------create "<<efilenums<<" files, name as Edge_X"<<endl;
}

void PreProcess::SaveEtype()
{
    string fn = objdir + "Etype.myg";
    savekit s(fn.c_str());
    etype->write(s);
}

void PreProcess::SaveEHasdes()
{
    string fn = objdir + "EHasdes.myg";
    savekit s(fn.c_str());
    hasdes->write(s);
}

void PreProcess::SaveErow()
{
    string fn(objdir + "Erows.myg");
    ofstream fout;
    fout.open(fn.c_str(), ios::out);
    for(u64 i = 0;i < eindexrow.size();++i)
        fout<<eindexrow[i]<<endl;
    fout.close();
}


void PreProcess::SaveEkinds()
{
    string fn(objdir + "Ekinds.myg");
    ofstream fout;
    fout.open(fn.c_str(), ios::out);
    for(auto i = ekinds.begin();i != ekinds.end();++i)
    {
        fout<<i->first<<endl;
        fout<<i->second.n<<endl;
        for(auto j = i->second.detail.begin();j != i->second.detail.end();++j)
        {
            fout<<j->first<<endl;
            fout<<j->second.head<<endl;
            fout<<j->second.hasexp<<endl;
            fout<<j->second.n<<endl;
            if(j->second.hasexp)
            {
                fout<<j->second.rows<<endl;
                for(auto l = j->second.hmap.begin();l != j->second.hmap.end();++l)
                {
                    fout<<l->first<<endl;
                    fout<<l->second.v1<<endl;
                    fout<<l->second.v2<<endl;
                }
            }
            else
                fout<<endl;
        }
    }
    fout.close();
}

void PreProcess::SaveEmap()
{
    string fn(objdir + "Emap.myg");
    ofstream fout;
    fout.open(fn.c_str(), ios::out);
    for(auto i = emap.begin();i != emap.end();++i)
        fout<<i->first<<endl<<i->second<<endl;
    fout.close();
}

void PreProcess::ToSmall(string& s)
{
    u64 len = s.length();
    for(u64 i = 0;i < len;++i)
    {
        if(s[i] >= 'A' && s[i] <= 'Z')
            s[i] = s[i] + 32;
    }
}

void PreProcess::GetHMap(unordered_map<string, Bigram>& hmap, string& s, char op)
{
    u64 len = s.length();
    vector<i64> idx; /* 列数 */
    idx.push_back(-1);
    for(u64 i = 0;i < len;++i)
        if(s[i] == op)
            idx.push_back(i);
    idx.push_back(len);
    u64 cnt = 1;
    for(u64 i = 0;i < idx.size() - 1;++i)
    {
        Bigram t;
        string ts = s.substr(idx[i] + 1, idx[i + 1] - idx[i] - 1);
        t.v1 = cnt;
        if(t_map.count(ts))
            t.v2 = t_map[ts];
        else if(ts.find(".id") != string::npos)
            t.v2 = TypeMap::int64type;
        else
        {
            cerr<<ts<<" is not found type! "<<endl;
            exit(1);
        }
        hmap.insert(pair<string, Bigram>{ts, t});
        ++cnt;
    }
}

void PreProcess::MapType()
{
    t_map.insert(pair<string, u64>{"id", TypeMap::int64type});
    t_map.insert(pair<string, u64>{"title", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"creationdate", TypeMap::datetimetype});
    t_map.insert(pair<string, u64>{"browserused", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"locationip", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"content", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"length", TypeMap::int32type});
    t_map.insert(pair<string, u64>{"name", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"url", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"firstname", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"lastname", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"gender", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"birthday", TypeMap::datetype});
    t_map.insert(pair<string, u64>{"email", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"speaks", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"language", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"imagefile", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"joindate", TypeMap::datetimetype});
    t_map.insert(pair<string, u64>{"classyear", TypeMap::int32type});
    t_map.insert(pair<string, u64>{"workfrom", TypeMap::int32type});
    t_map.insert(pair<string, u64>{"type", TypeMap::stringtype});
}

void PreProcess::MapTypeBanama()
{
    t_map.insert(pair<string, u64>{"node_id", TypeMap::int64type});
    t_map.insert(pair<string, u64>{"address", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"name", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"countries", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"country_codes", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"sourceid", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"valid_until", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"note", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"original_name", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"former_name", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"jurisdiction", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"jurisdiction_description", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"company_type", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"internal_id", TypeMap::int64type});
    t_map.insert(pair<string, u64>{"incorporation_date", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"inactivation_date", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"struck_off_date", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"dorm_date", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"status", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"service_provider", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"ibcruc", TypeMap::int64type});
    t_map.insert(pair<string, u64>{"node_id_start", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"node_id_end", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"link", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"start_date", TypeMap::stringtype});
    t_map.insert(pair<string, u64>{"end_date", TypeMap::stringtype});
}

void PreProcess::CreateFiles(vector<Fstring>& tedgefiles)
{
    etypesum = tedgefiles.size();
    pair<string, vector<Tstring> > tedge;
    Tstring ttstring;
    tedge.first = tedgefiles[0].s1;
    ttstring.s1 = tedgefiles[0].s2;
    ttstring.s2 = tedgefiles[0].s3;
    ttstring.s3 = tedgefiles[0].s4;
    tedge.second.push_back(ttstring);
    for(u64 i = 1;i < tedgefiles.size();++i)
    {
        if(tedgefiles[i].s1 == tedge.first)
        {
            ttstring.s1 = tedgefiles[i].s2;
            ttstring.s2 = tedgefiles[i].s3;
            ttstring.s3 = tedgefiles[i].s4;
            tedge.second.push_back(ttstring);
        }
        else
        {
            edgefiles.insert(tedge);
            tedge.first = tedgefiles[i].s1;
            vector<Tstring>().swap(tedge.second);
            ttstring.s1 = tedgefiles[i].s2;
            ttstring.s2 = tedgefiles[i].s3;
            ttstring.s3 = tedgefiles[i].s4;
            tedge.second.push_back(ttstring);
        }
    }
    edgefiles.insert(tedge);
}

u64 PreProcess::blog(u64 x)
{
    u64 res = 0;
    while(x > 0)
    {
        x = x >> 1;
        ++res;
    }
    return res;
}



void MergeSort(vector<Keyvalue> &kv, u64 left, u64 right)
{
    if (left >= right)    // 递归出口
        return;
    u64 mid = left + ((right - left) >> 1);
    MergeSort(kv, left, mid);
    MergeSort(kv, mid + 1, right);
    MergeHelp(kv, left, mid, right);
}

void MergeHelp(vector<Keyvalue> &kv, u64 left, u64 mid, u64 right)
{
    u64 len = right - left + 1;
    vector<Keyvalue> temp(len);
    u64 i = left, j = mid + 1, idx = 0;
    while(i <= mid && j <= right)
    {
        if(kv[i].id <= kv[j].id)
            temp[idx++] = kv[i++];
        else
            temp[idx++] = kv[j++];
    }      
    while(i <= mid)
        temp[idx++] = kv[i++];
    while(j <= right)
        temp[idx++] = kv[j++];
    for(u64 i = 0;i < len; ++i)
        kv[left + i] = temp[i]; 
}

void MergeSort(vector<BigramS> &bs, u64 left, u64 right)
{
    if (left >= right)    // 递归出口
        return;
    u64 mid = left + ((right - left) >> 1);
    MergeSort(bs, left, mid);
    MergeSort(bs, mid + 1, right);
    MergeHelp(bs, left, mid, right);
}

void MergeHelp(vector<BigramS> &bs, u64 left, u64 mid, u64 right)
{
    u64 len = right - left + 1;
    vector<BigramS> temp(len);
    u64 i = left, j = mid + 1, idx = 0;
    while(i <= mid && j <= right)
    {
        if(bs[i].v1 != bs[j].v1)
        {   
            if(bs[i].v1 < bs[j].v1)
                temp[idx++] = bs[i++];
            else
                temp[idx++] = bs[j++];
        }
        else
        {
            if(bs[i].v2 <= bs[j].v2)
                temp[idx++] = bs[i++];
            else
                temp[idx++] = bs[j++];
        }
    } 
    while(i <= mid)
        temp[idx++] = bs[i++];
    while(j <= right)
        temp[idx++] = bs[j++];
    for(u64 i = 0;i < len; ++i)
        bs[left + i] = temp[i]; 
}

void Merge(vector<Keyvalue> &kv1,const vector<Keyvalue> &kv2)
{
    u64 l1 = 0, l2 = 0;
    u64 r1 = kv1.size() - 1, r2 = kv2.size() - 1;
    u64 len1 = r1 - l1 + 1, len2 = r2 - l2 + 1;
    vector<Keyvalue> kvtmp;
    kvtmp.swap(kv1);
    kv1.resize(len1 + len2);
    u64 idx = 0;
    while(l1 <= r1 && l2 <= r2)
    {
        if(kvtmp[l1].id <= kv2[l2].id)
            kv1[idx++] = kvtmp[l1++];
        else
            kv1[idx++] = kv2[l2++];
    }      
    while(l1 <= r1)
        kv1[idx++] = kvtmp[l1++];
    while(l2 <= r2)
        kv1[idx++] = kv2[l2++];
}

