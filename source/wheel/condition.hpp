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
#include "noncopyable.hpp"
#include "mutexlock.hpp"
#include <pthread.h>

class Condition
{
public:
    explicit Condition(MutexLock &_mutex):
        mutex(_mutex)
    {
        pthread_cond_init(&cond, NULL);
    }
    ~Condition()
    {
        pthread_cond_destroy(&cond);
    }
    void wait()
    {
        pthread_cond_wait(&cond, mutex.get());
    }
    void notify()
    {
        pthread_cond_signal(&cond);
    }
    void notifyAll()
    {
        pthread_cond_broadcast(&cond);
    }
private:
    MutexLock &mutex;
    pthread_cond_t cond;
};