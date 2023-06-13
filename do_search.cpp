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
#include"Graph.h"
#include<ctime>
#include<fstream>
#include<iostream>
#include<map>
void PrintHelp()
{
    cerr<<"Format : "<<"do_search DataDir SerchMod InputFileName TimeFileName"<<endl;
    cerr<<"Format : "<<"do_search Datadir SerchMod ExtTimes ExtLen TimeFileName"<<endl;
}
using namespace std;
int main(int argc, char** argv)
{
    if(argc < 5 || argc > 6)
    {
        PrintHelp();
        return 0;
    }
    Graph s;
    s.LoadFiles(argv[1]);
    if(atoi(argv[2]) < 100)
        s.TestRun(atoi(argv[2]), argv[3], argv[4]);
    else if(atoi(argv[2]) == 100)
        s.TestExtVertex(atoi(argv[3]), atoi(argv[4]), argv[5]);
    else if(atoi(argv[2]) == 101)
        s.TestExtEdge(atoi(argv[3]), atoi(argv[4]), argv[5]);
}