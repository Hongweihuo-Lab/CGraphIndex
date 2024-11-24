#pragma once

#include <cassert>
#include <cstdint>
#include <functional>

#include <oneapi/tbb/concurrent_vector.h>
#include <oneapi/tbb/concurrent_queue.h>
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/parallel_reduce.h>
#include <oneapi/tbb/parallel_scan.h>
#include <oneapi/tbb/parallel_sort.h>

namespace par::tbb_backend {

[[nodiscard]] inline uint32_t get_max_threads() {
    return oneapi::tbb::info::default_concurrency();
}

using ThreadArena = oneapi::tbb::task_arena;

[[nodiscard]] inline ThreadArena create_thread_arena(uint32_t num_threads = get_max_threads()) {
    return ThreadArena(num_threads);
}

template<typename RandomAccessIterT, typename SizeT, typename T>
void fill_n(RandomAccessIterT first, SizeT n, const T &x) {
    if constexpr (std::is_arithmetic_v<T>) {
        oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, [=](SizeT i) {
            first[i] = x;
        }, oneapi::tbb::static_partitioner{});
    } else {
        oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, [=](SizeT i) {
            first[i] = x;
        });
    }
}

template<typename RandomAccessIterT, typename T>
void fill(RandomAccessIterT first, RandomAccessIterT last, const T &x) {
    auto n = std::distance(first, last);
    assert(n >= 0);
    using SizeType = std::make_unsigned_t<decltype(n)>;
    fill_n(first, static_cast<SizeType>(n), x);
}

template<typename RandomAccessIterT, typename SizeT, typename T>
void fill_n(ThreadArena &arena, RandomAccessIterT first, SizeT n, const T &x) {
    arena.execute([=]() {
        oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, [=](SizeT i) {
        if constexpr (std::is_arithmetic_v<T>) {
            oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, [=](SizeT i) {
                first[i] = x;
            }, oneapi::tbb::static_partitioner{});
        } else {
            oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, [=](SizeT i) {
                first[i] = x;
            });
        }
        });
    });
}

template<typename RandomAccessIterT, typename T>
void fill(ThreadArena &arena, RandomAccessIterT first, RandomAccessIterT last, const T &x) {
    auto n = std::distance(first, last);
    assert(n >= 0);
    using SizeType = std::make_unsigned_t<decltype(n)>;
    fill_n(arena, first, static_cast<SizeType>(n), x);
}

template<typename RandomAccessIterT, typename SizeT, typename F>
void for_each_n(RandomAccessIterT first, SizeT n, const F &f) {
    oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, [=](SizeT i) {
        std::invoke(f, first[i]);
    });
}

template<typename RandomAccessIterT, typename F>
void for_each(RandomAccessIterT first, RandomAccessIterT last, const F &f) {
    auto n = std::distance(first, last);
    assert(n >= 0);
    using SizeType = std::make_unsigned_t<decltype(n)>;
    for_each_n(first, static_cast<SizeType>(n), f);
}

template<typename RandomAccessIterT, typename SizeT, typename F>
void for_each_n(ThreadArena &arena, RandomAccessIterT first, SizeT n, const F &f) {
    arena.execute([=]() {
        oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, [=](SizeT i) {
            std::invoke(f, first[i]);
        });
    });
}

template<typename RandomAccessIterT, typename F>
void for_each(ThreadArena &arena, RandomAccessIterT first, RandomAccessIterT last, const F &f) {
    auto n = std::distance(first, last);
    assert(n >= 0);
    using SizeType = std::make_unsigned_t<decltype(n)>;
    for_each_n(arena, first, static_cast<SizeType>(n), f);
}

template<typename RandomAccessIterT, typename T, typename F>
T reduce_sum(RandomAccessIterT first, RandomAccessIterT last, T init, const F &project) {
    return oneapi::tbb::parallel_reduce(tbb::blocked_range<RandomAccessIterT>{first, last},
        init,
        [&](const tbb::blocked_range<RandomAccessIterT> &r, T init) -> T {
            for (auto iter = r.begin(); iter != r.end(); ++iter)
                init += project(*iter);
            return init;
        },
        [&](T a, T b) -> T {
            return a + b;
        }
    );
}

template<typename SizeT, typename F>
void repeat(SizeT n, const F &f) {
    oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, f);
}

template<typename SizeT, typename F>
void repeat(ThreadArena &arena, SizeT n, const F &f) {
    arena.execute([=]() {
        oneapi::tbb::parallel_for(static_cast<SizeT>(0), n, f);
    });
}

template<typename RandomAccessIterT, typename CmpT>
void sort(RandomAccessIterT first, RandomAccessIterT last, CmpT cmp) {
    oneapi::tbb::parallel_sort(first, last, cmp);
}

template<typename RandomAccessIterT>
void sort(RandomAccessIterT first, RandomAccessIterT last) {
    oneapi::tbb::parallel_sort(first, last);
}

template<typename RandomAccessIterT, typename CmpT>
void sort(ThreadArena &arena, RandomAccessIterT first, RandomAccessIterT last, CmpT cmp) {
    arena.execute([=]() {
        oneapi::tbb::parallel_sort(first, last, cmp);
    });
}

template<typename RandomAccessIterT>
void sort(ThreadArena &arena, RandomAccessIterT first, RandomAccessIterT last) {
    arena.execute([=]() {
        oneapi::tbb::parallel_sort(first, last);
    });
}


}