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
void PrintHelp()
{
    cerr<<"Format : "<<"create_adjlist TmpAdjTabledir Objdir"<<endl;
}
int blog(u64 x)
{
    int res = 0;
    while(x > 0)
    {
        ++res;
        x = x >> 1; 
    }
    return res;
}
int main(int argc, char** argv)
{
    if(argc <= 2)
    {
        PrintHelp();
        return 0;
    }
    string adjfn = (string)argv[1] + "TmpAdjList.myg";

    clock_t bb ,ee;
    bb = clock();
    AdjList res(adjfn);
    res.ReadAdjTable();
    res.Construct();
    string savefn = (string)argv[2] + "AdjList.myg";

    savekit s(savefn.c_str());
    res.Write(s);
    ee = clock();
    
    u64 V = res.GetVnum();
    u64 E = res.GetEnum();
    cerr<<"Total V : "<<V<<endl;
    cerr<<"Total E : "<<E<<endl;
    cerr<<"Avg Out : "<<E * 1.0 / V<<endl;
    cerr<<"Compress Size : "<<res.GetMemorySizeByte()<<endl;
    u64 sourcesize = (V * blog(V) + (V + E) *blog(E)) / 8;
    cerr<<"Source Size : "<<sourcesize<<endl;
    cerr<<"Compress Radio : "<<res.GetMemorySizeByte() * 1.0 / sourcesize * 100<<'%'<<endl;
    cerr<<"AdjList Process costs "<<(ee - bb) * 1.0 / 1000000<<"s"<<endl;


    cout<<"Total V : "<<V<<endl;
    cout<<"Total E : "<<E<<endl;
    cout<<"Out Max : "<<res.GetOutMax()<<endl;
    cout<<"In Max : "<<res.GetInMax()<<endl;
    cout<<"Avg Out : "<<E * 1.0 / V<<endl;
    cout<<"Out Size : "<<res.GetOutSizeByte()<<" Bytes"<<endl;
    cout<<"Out Bits/Link : "<<res.GetOutSizeByte() * 8.0 / E<<" Bytes"<<endl;
    cout<<"In Size : "<<res.GetInSizeByte()<<" Bytes"<<endl;
    cout<<"In Bits/Link : "<<res.GetInSizeByte() * 8.0 / E<<" Bytes"<<endl;
    cout<<"S Radio : "<<res.GetSSizeRadio()<<'%'<<endl;
    cout<<"X Radio : "<<res.GetXSizeRadio()<<'%'<<endl;
    cout<<"Ngap Radio : "<<res.GetNgapSizeRadio()<<'%'<<endl;
    cout<<"Sam Radio : "<<res.GetSamSizeRadio()<<'%'<<endl;
    cout<<"Compress Size : "<<res.GetMemorySizeByte()<<" Bytes"<<endl;
    cout<<"Compress Radio : "<<res.GetMemorySizeRadio()<<'%'<<endl;
    cout<<"AdjList Process costs "<<(ee - bb) * 1.0 / 1000000<<" sec"<<endl;
    return 0;
}