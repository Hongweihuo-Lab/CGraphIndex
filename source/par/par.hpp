// Copyright (C) 2024  Yongze Yu, Zongtao He, Hongwei Huo, and Jeffrey S. Vitter
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

#include <atomic>

#include "details/tbb_impl.hpp"

#define PARALLEL_BACKEND_SERIAL 0
#define PARALLEL_BACKEND_TBB    1
#define PARALLEL_BACKEND_OMP   -1 // OpenMP backend is not implemented, don't use it
#define DEFAULT_PARALLEL_BACKEND PARALLEL_BACKEND_TBB

#if DEFAULT_PARALLEL_BACKEND == PARALLEL_BACKEND_OMP
#   error "OpenMP backend is not implemented"
#endif

namespace par {

#if DEFAULT_PARALLEL_BACKEND == PARALLEL_BACKEND_TBB
using namespace tbb_backend;
#elif DEFAULT_PARALLEL_BACKEND == PARALLEL_BACKEND_OMP
using namespace omp_backend;
#endif

template<typename TaskGenF>
void launch_tasks(uint32_t num_threads, TaskGenF task_generator) {
    std::vector<std::thread> threads{num_threads};
    for (size_t i = 0; i < num_threads; ++i)
        threads[i] = std::thread{std::invoke(task_generator, i)};
    for (size_t i = 0; i < num_threads; ++i)
        threads[i].join();
}

namespace atomic {

template<typename T>
inline bool compare_and_exchange(T *ptr, T &expected, T desired, std::memory_order success, std::memory_order failure) {
    std::atomic_ref<T> ref{*ptr};
    return ref.compare_exchange_strong(expected, desired, success, failure);
}

template<typename T>
inline bool compare_and_exchange(T *ptr, T &expected, T desired, std::memory_order order = std::memory_order::seq_cst) {
    std::atomic_ref<T> ref{*ptr};
    return ref.compare_exchange_strong(expected, desired, order);
}

template<typename T>
inline T fetch_add(T *ptr, T x, std::memory_order order = std::memory_order::seq_cst) noexcept {
    std::atomic_ref<T> ref{*ptr};
    return ref.fetch_add(x, order);
}

template<typename T>
inline T fetch_sub(T *ptr, T x, std::memory_order order = std::memory_order::seq_cst) noexcept {
    std::atomic_ref<T> ref{*ptr};
    return ref.fetch_sub(x, order);
}

template<typename T>
inline T fetch_and(T *ptr, T x, std::memory_order order = std::memory_order::seq_cst) noexcept {
    std::atomic_ref<T> ref{*ptr};
    return ref.fetch_and(x, order);
}

template<typename T>
inline T fetch_or(T *ptr, T x, std::memory_order order = std::memory_order::seq_cst) noexcept {
    std::atomic_ref<T> ref{*ptr};
    return ref.fetch_or(x, order);
}

}

}