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
void PrintHelp()
{
    cerr<<"Format : "<<"create_init Datadir Objdir Cutsize"<<endl;
}
int main(int argc, char** argv)
{
    if(argc <= 2)
    {
        PrintHelp();
        return 0;
    }
    timeval bb, ee;
    gettimeofday(&bb, NULL);

    PreProcess s(argv[1]);
    s.InitRun(argv[2], atoi(argv[3]));

    gettimeofday(&ee, NULL);
    cout<<"Init Costs : "<<GetCostTime(bb, ee) * 1.0 / 1000000<<" sec"<<endl;
    cerr<<"Init Costs : "<<GetCostTime(bb, ee) * 1.0 / 1000000<<" sec"<<endl;
    
    return 0;
}