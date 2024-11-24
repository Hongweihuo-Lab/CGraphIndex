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

