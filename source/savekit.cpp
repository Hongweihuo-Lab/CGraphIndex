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

#include"savekit.h"
void savekit::close()
{
	if(w!=NULL)
		fclose(w);
	w=NULL;
}

savekit::~savekit()
{
	if(w!=NULL)
		fclose(w);
}

savekit::savekit(const char * file)
{
	this->w=fopen(file,"wb");
	if(w==NULL)
	{
		cout<<"Fopen error"<<endl;
		exit(0);
	}
}

integer savekit::writei64(i64 value)
{
	fwrite(&value,sizeof(i64),1,w);
	return 1;
}


integer savekit::writeu64(u64 value)
{
	fwrite(&value,sizeof(u64),1,w);
	return 1;
}

integer savekit::writeinteger( integer value)
{
	fwrite(&value,sizeof( integer),1,w);
	return 1;
}

integer savekit::writeu32(u32 value)
{
	fwrite(&value,sizeof(u32),1,w);
	return 1;
}

integer savekit::writei32(i32 value)
{
	fwrite(&value,sizeof(i32),1,w);
	return 1;
}

integer savekit::writei16(i16 value)
{
	fwrite(&value,sizeof(i16),1,w);
	return 1;
}

integer savekit::writeu16(u16 value)
{
	fwrite(&value,sizeof(u16),1,w);
	return 1;
}

 integer savekit::writei64array(i64 * value,integer len)
{
	fwrite(value,sizeof(i64),len,w);
	return 1;
}
 integer savekit::writei64array(i64 * value,i64 len)
{
	fwrite(value,sizeof(i64),len,w);
	return 1;
}

 integer savekit::writeu64array(u64 * value,i64 len)
{
	fwrite(value,sizeof(u64),len,w);
	return 1;
}

 integer savekit::writeintegerarray(integer * value,integer len)
{
	fwrite(value,sizeof(integer),len,w);
	return 1;
}
 integer savekit::writeu32array(u32* value,integer len)
{
	fwrite(value,sizeof(u32),len,w);
	return 1;
}
 integer savekit::writeu32array(u32* value,i64 len)
{
	fwrite(value,sizeof(u32),len,w);
	return 1;
}
 integer savekit::writei16array(i16 * value,integer len)
{
	fwrite(value,sizeof(i16),len,w);
	return 1;
}

 integer savekit::writeu16array(u16 * value,integer len)
{
	fwrite(value,sizeof(u16),len,w);
	return 1;
}
