/*============================================		 
 # Source code: http://dx.doi.org/10.24433/CO.0710687.v1
 #
 # Paper: Practical high-order entropy-compressed text self-Indexing
 # Authors:  Peng Long, Zongtao He, Hongwei Huo and Jeffrey S. Vitter
 # In TKDE, 2023,  35(3): 2943–2960.
============================================*/
#include"loadkit.h"
void loadkit::close()
{
	if(r!=NULL)
		fclose(r);
	r=NULL;
}

loadkit::~loadkit()
{
	if(r!=NULL)
		fclose(r);
}

loadkit::loadkit(const char * file)
{
	this->r=fopen(file,"rb");
	if(r==NULL)
	{
		cout<<"Fopen error"<<endl;
		exit(0);
	}
}

integer loadkit::loadi64(i64 & value)
{
	integer num=fread(&value,sizeof(i64),1,r);
	return num;
}

integer loadkit::loadu64(u64 & value)
{
	integer num=fread(&value,sizeof(u64),1,r);
	return num;
}

integer loadkit::loadinteger(integer & value)
{
	integer num=fread(&value,sizeof(integer),1,r);
	return num;
}

integer loadkit::loadu32(u32 & value)
{
	integer num=fread(&value,sizeof(u32),1,r);
	return num;
}

integer loadkit::loadi32(i32 & value)
{
	integer num=fread(&value,sizeof(i32),1,r);
	return num;
}

integer loadkit::loadi16(i16 & value)
{
	integer num=fread(&value,sizeof(i16),1,r);
	return num;
}

integer loadkit::loadu16(u16 & value)
{
	integer num=fread(&value,sizeof(u16),1,r);
	return num;
}

integer loadkit::loadi64array(i64 * value,integer len)
{
	i64 num=fread(value,sizeof(i64),len,r);
	return num;
}
i64 loadkit::loadi64array(i64 * value,i64 len)
{
	i64 num=fread(value,sizeof(i64),len,r);
	return num;
}
integer loadkit::loadu64array(u64 * value,i64 len)
{
	integer num=fread(value,sizeof(u64),len,r);
	return num;
}

integer loadkit::loadintegerarray(integer * value,integer len)
{
	integer num=fread(value,sizeof(integer),len,r);
	return num;
}

integer loadkit::loadu32array(u32 * value,integer len)
{
	integer num=fread(value,sizeof(u32),len,r);
	return num;
}
i64 loadkit::loadu32array(u32 * value,i64 len)
{
	i64 num=fread(value,sizeof(u32),len,r);
	return num;
}
integer loadkit::loadi16array(i16 * value,integer len)
{
	integer num=fread(value,sizeof(i16),len,r);
	return num;
}

integer loadkit::loadu16array(u16 * value,integer len)
{
	integer num=fread(value,sizeof(u16),len,r);
	return num;
}
