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
#include<stdlib.h>
#include<string.h>
#include"CSA.h"
#include<fstream>
#include<fstream>
#include<iostream>
#include <sys/time.h>

void PrintHelp()
{
    cerr<<"Format : "<<"create_index blockSize Samplesize Objdir Files ..."<<endl;
}
int main(int argc, char** argv)
{
    if(argc <= 4)
    {
        PrintHelp();
        return 0;
    }
    string firfn(argv[4]);
    u64 pos = firfn.find_last_of('/');
    if(pos != string::npos)
        firfn = firfn.substr(pos + 1);

    timeval bb, ee;
    gettimeofday(&bb, NULL);
    for(u64 i = 4;i < argc;++i)
    {
        string filepath(argv[i]);
        int L = atoi(argv[1]), D = atoi(argv[2]);
        CSA *test = new CSA(argv[i], L, D);
        string firfn(argv[i]);
        u64 pos = firfn.find_last_of('/');
        if(pos != string::npos)
        firfn = firfn.substr(pos + 1);
        string indexname = (string)argv[3] + firfn +".geindex";
        test->save(indexname.c_str());
        delete test;
        test = NULL;
    }
    gettimeofday(&ee, NULL);

    cout<<"Create Index Total Costs : "<<((ee.tv_sec * 1000000 + ee.tv_usec) - (bb.tv_sec * 1000000 + bb.tv_usec)) * 1.0 / 1000000<<" sec"<<endl;;
    
    return 0;
}