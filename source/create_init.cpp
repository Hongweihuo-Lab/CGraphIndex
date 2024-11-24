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