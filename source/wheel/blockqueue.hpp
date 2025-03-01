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

#pragma once
#include"condition.hpp"
#include<string.h>
struct threadpool_task
{
    void (*fun)(void*);
    void* args;
};
class BlockQueue
{
public:
    BlockQueue(long long size):_cond(_mutex), buffsize(size), head(0), tail(0),
                buff(new threadpool_task[buffsize])
                {
                    memset(buff, 0, buffsize * sizeof(threadpool_task));
                }
    ~BlockQueue()
    {
        delete [] buff;
    }
    int CurSize()
    {
        MutexLockGuard mutex(_mutex);
        return (tail + buffsize - head) % buffsize;
    }
    void Push(threadpool_task& tmp)
    {
        MutexLockGuard mutex(_mutex);
        buff[tail] = tmp;
        ++tail;
        if(tail >= buffsize)
            tail = 0;
        _cond.notify();
    }
    threadpool_task Get_Pop()
    {
        MutexLockGuard mutex(_mutex);
        while(empty())
            _cond.wait();
        threadpool_task tmp = buff[head];
        ++head;
        if(head >= buffsize)
            head = 0;
        return tmp;
    }
    bool empty()
    {
        return head == tail;
    }
private:
    MutexLock _mutex;
    Condition _cond;
    long long buffsize;
    long long head, tail;
    threadpool_task *buff;
};