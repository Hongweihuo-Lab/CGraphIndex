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
#include<functional>
#include<memory>
#include<iostream>
#include"blockqueue.hpp"
#define THREAD_MAX 999
#define QUEUESIZE_MAX 10000000
extern void diyfun(void *arg);

class threadpool
{
public:
    threadpool(int t_c):threadcnt(t_c)
    {
        if(threadcnt <= 0 || threadcnt > THREAD_MAX)  
        exit(1);
        threads.reserve(threadcnt); 
    }
    void creat_thread()
    {
        taskqueue = new BlockQueue(QUEUESIZE_MAX);
        for(int i = 0; i < threadcnt; ++i) 
            if(pthread_create(&threads[i], NULL, work_fun, (void*)this))
                exit(1);
    }
    threadpool_task getwork()
    {
        threadpool_task tmp = taskqueue->Get_Pop();
        return tmp;
    }
    void addwork(void *args, void (*fun)(void*))
    {
        threadpool_task tmp;
        tmp.fun = fun;
        tmp.args = args;
        taskqueue->Push(tmp);
    }

    static void* work_fun(void *args)
    {
        threadpool* pool = (threadpool*)args;
        while(1)
        {
            threadpool_task tmp = pool->getwork();
            (tmp.fun)(tmp.args);
        }
    }
    static void exit_fun(void *args)
    {
        pthread_exit(NULL);
    }
    void destroy_pool()
    {
        for(int i = 0;i < threadcnt;++i)
            addwork(NULL, exit_fun);
        for(int i = 0;i < threadcnt;++i)
            pthread_join(threads[i], NULL);
        delete taskqueue;
    }
    ~threadpool()
    {
        
    }
private:
    int threadcnt;
    std::vector<pthread_t> threads;
    BlockQueue *taskqueue;
};

