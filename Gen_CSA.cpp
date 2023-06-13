/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
#include<string.h>
#include<ctime>
#include<fstream>
#include<iostream>
#include<cmath>
#include<vector>
#include "CSA.h"
using namespace std;
void fistIn();
void usage();
int FileExist(const char* fname);
void SplitString(const string& s, vector<string>& v, const string& c);
bool isLegal(int num);
void showpos( long long * pos,long long num);
int main(int argc, char* argv[])
{
    fistIn();
    usage();
    cout<<">>:";
    string command;
	vector<string> result;
	char filename[100] = {'\0'};
	char indexname[100] = {'\0'};
    string savename = "";
    string split_str = " ";
    int L = 256,D = 128;
	CSA *csa = NULL;
    while (1)
    {
        vector<string>().swap(result);
        command = "";
        getline(cin,command);
        SplitString(command,result,split_str);
        int result_len = result.size();
        if(result_len == 4){
            if(result[0] == "build"){
                if(csa != NULL)
                    delete csa;
                csa = NULL;
                memset(filename,0,100*sizeof(char));
                strcpy(filename,result[1].c_str());
                // cout<<filename<<endl;
                L = atoi(result[2].data());
                D = atoi(result[3].data());
                savename = "";
                savename += result[1] + to_string(L) + to_string(D) + ".geindex";
                if(FileExist(savename.data())){
                    csa = new CSA();
                    csa->load(savename.data());
                    cout<<"index load"<<endl;
                }else{
                    csa = new CSA(filename,L,D);
                    cout<<"index build"<<endl;
                }
            }else{
                usage();
            }
        }else if(result_len == 3){
            if(result[0] == "E")
            {
                if(csa!=NULL)
                {
                    int strat = atoi(result[1].data());
                    int len = atoi(result[2].data());
                    uchar *p;
                    p = csa->extracting(strat,len);
                    cout<<p<<endl;
                    delete [] p;
                }
                else
                    cout<<"build a csa first"<<endl;
            }else{
                usage();
            }

        }
        else if(result_len == 2){
            //index all
            if(result[0] == "build"){
                if(csa != NULL)
                    delete csa;
                csa = NULL;
                memset(filename,0,100*sizeof(char));
                strcpy(filename,result[1].c_str());
                // cout<<filename<<endl;
                savename = "";
                L = 256;
                D = 128;
                savename += result[1] + to_string(L) + to_string(D) + ".geindex";
                cout<<savename<<endl;
                if(FileExist(savename.data())){
                    csa = new CSA();
                    csa->load(savename.data());
                    cout<<"index load"<<endl;
                }else{
                    csa = new CSA(filename);
                    cout<<"index build"<<endl;
                }
            }
            if(csa != NULL){
                
                if(result[0] == "c"){
                    int count = atoi(result[1].data());
                    if(isLegal(count)){
                        csa->TestForCount(count);
                    }else{
                        cout<<"c times error"<<endl;
                    }
                }
                else if(result[0] == "l"){
                    int count = atoi(result[1].data());
                    if(isLegal(count)){
                        csa->TestForLocate(count);
                    }else{
                        cout<<"l times error"<<endl;
                    }
                }
                else if(result[0] == "e"){
                    int count = atoi(result[1].data());
                    if(isLegal(count)){
                        csa->TestForExtract(count);
                    }else{
                        cout<<"e times error"<<endl;
                    }
                }
                //index one
                else if(result[0]=="C")
                {
                    
                    if(csa != NULL)
                    {
                        long long  num = 0;
                        int p_len = result[1].size();
                        unsigned char* PP = new unsigned char[p_len + 1];
                        PP[p_len] = '\0';
                        for (int i = 0; i < p_len; i++)
                        {
                            PP[i] = result[1][i];
                        }
                        csa->counting(PP,num);
                        cout<<"count times: "<<num<<endl;
                        delete[] PP;
                    }
                    else
                    {
                        cout<<"build a csa first"<<endl;
                    }
                }
                else if(result[0] == "L")
                {
                    if(csa != NULL)
                    {
                        int p_len = result[1].size();
                        unsigned char* PP = new unsigned char[p_len + 1];
                        PP[p_len] = '\0';
                        for (int i = 0; i < p_len; i++)
                        {
                            PP[i] = result[1][i];
                        }
                        long long * pos;
                        long long num = 0;
                        pos=csa->locating(PP,num);
                        showpos(pos,num);
                        delete[] pos;
                        delete[] PP;
                    }
                    else
                    {
                        cout<<"build a csa first"<<endl;
                    }
                    
                }else{
                    usage();
                }
            }
            else{
                cout<<"build a csa first"<<endl;
            }
            
            
            
        }
        else if(result_len == 1){
            if(result[0] == "exit"){
                if(csa != NULL){
                    delete csa;
                }
                break;
            }
            if(result[0] == "save"){
                if(savename == ""){
                    cout<<"build index first"<<endl;
                    usage();
                }else{
                    cout<<savename<<endl;
                    csa->save(savename.data());
                    cout<<"index save"<<endl;
                }
            }
            if(csa != NULL){
                if(result[0] == "a"){
                    csa->TestForIndex();
                    cout << "ratio: " << csa->compressRatio() << endl;
                }
                else if(result[0] == "c"){
                    csa->TestForCount();
                }
                else if(result[0] == "l"){
                    csa->TestForLocate();
                }
                else if(result[0] == "e"){
                    csa->TestForExtract();
                }
                else if(result[0] == "s"){
                    cout << "index size: " << csa->sizeInByte()*1.0/(1024*1024)<<"MB"<< endl;
                }else if(result[0] == "r"){
                    cout << "ratio: " << csa->compressRatio() << endl;
                }else if(result[0] == "help"){
                    usage();
                }
                else{
                    usage();
                }
            }
            else
            {
                cout<<"build a csa first"<<endl;
            }
            
        }else{
            usage();
        }
        cout<<">>:";
    }
	
	return 0;
}

void showpos(long long * pos,long long num)
{
	cout<<"times:"<<num<<endl;
	for(long int i=0;i<num;i++)
	{
		cout<<pos[i]<<endl;
		if((i+1)%20==0)
		{
			char command;
			cout<<"-----------------more---------------------";
			system("stty raw");
			command=getchar();
     		cout<<endl<<'\r';
			system("stty cooked");
			if(command==27)
			{
				cout<<endl;
				return ;
			}
		}
	}
}


void splitcommand(string command,string result[])
{

	int i=0;
	int start=0;
	int len=command.length();
	result[0]=command;
	for(i=0;i<len;i++)
	{
		if(command.at(i)!=' ')
			continue;
		result[0]=command.substr(0,i);
		start=i+1;
		break;
	}
	result[1]=command.substr(start,len);
}

int FileExist(const char* fname)
{
    return (access(fname,0)==0);
}
void SplitString(const string& s, vector<string>& v, const string& c)
{
    string::size_type pos1, pos2;
    pos2 = s.find(c);
    pos1 = 0;
    while(string::npos != pos2)
    {
        v.push_back(s.substr(pos1, pos2-pos1));

        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if(pos1 != s.length())
        v.push_back(s.substr(pos1));
}
bool isLegal(int num){
    if(num > 0){
        if(num < INT32_MAX){
            return true;
        }
    }
    return false;
}

void fistIn(){
    cout<<"------------------------------------------------------"<<endl;
    cout<<"build index first,commands like : build filename [block_size] [sample_step]."<<endl;
}
void usage()
{
	cout<<"------------------------------------------------------"<<endl;
	cout<<"The flowing commands are supported "<<endl;
	cout<<"help           #show the details for the command XX"<<endl;
	cout<<"build filename [block_size] [sample_step]  #build or load the index of file"<<endl;
    cout<<"save           #save the index to file"<<endl;
    cout<<"c [num]        #test count [nums] times,defult 5000 times"<<endl;
    cout<<"l [num]        #test locate [nums] times,defult 5000 times"<<endl;
    cout<<"e [num]        #test extract [nums] times,defult 5000 times"<<endl;
    cout<<"a              #show all test"<<endl;
	cout<<"C PP           #count pattern PP in the index file"<<endl;
    cout<<"L PP           #locate pattern PP in the index file"<<endl;
    cout<<"E start len    #extract pattern PP in the index file"<<endl;
	cout<<"s              #the size of the csa"<<endl;
    cout<<"r              #the ratio of the csa"<<endl;
	cout<<"exit           #exit"<<endl;

}
