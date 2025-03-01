#pragma once

#include <cassert>
#include <string_view>
#include <cstdint>
#include <queue>
#include <charconv>
#include <chrono>

#include "oneapi/tbb/concurrent_priority_queue.h"

template<typename T, typename Cmp>
class TopKPriorityQueue {
public:
    TopKPriorityQueue(size_t max_k)
        : max_k_(max_k) { }

    bool empty() const noexcept { return size() == 0; }
    size_t size() const noexcept { return pq_.size(); }

    const T &top() const { return pq_.top(); }
    void pop() { pq_.pop(); }

    void push(const T &val) {
        push(T{val});
    }

    void push(T &&val) {
        if (pq_.size() == max_k_) {
            if (cmp_(val, pq_.top())) {
                pq_.pop();
                pq_.push(std::move(val));
            }
        } else {
            pq_.push(std::move(val));
        }
    }

private:
    size_t                                      max_k_;
    Cmp                                         cmp_;
    std::priority_queue<T, std::vector<T>, Cmp> pq_;   
};

#if 0
template<typename T, typename Cmp>
class ConcurrentTopKPriorityQueue {
public:
    ConcurrentTopKPriorityQueue(size_t max_k)
        : max_k_(max_k) { }

    bool empty() const noexcept { return size() == 0; }
    size_t size() const noexcept { return pq_.size(); }

    const T &top() const { return pq_.top(); }
    void pop() { pq_.pop(); }

    void push(const T &val) {
        push(T{val});
    }

    void push(T &&val) {
        if (pq_.size() == max_k_) {
            if (cmp_(val, pq_.top())) {
                pq_.pop();
                pq_.push(std::move(val));
            }
        } else {
            pq_.push(std::move(val));
        }
    }

private:
    size_t                                         max_k_;
    Cmp                                            cmp_;
    oneapi::tbb::concurrent_priority_queue<T, Cmp> pq_;
};
#endif

inline uint64_t date_str_to_timestamp(std::string_view x) {
    //if (x.size() == 6) {
    //    auto s1 = std::string{x};
    //    puts(s1.data());
    //}

    auto year_str = x.substr(0, 4);
    auto month_str = x.substr(5, 2);
    auto day_str = x.substr(8, 2);

    int32_t year;
    uint32_t month, day;
    std::from_chars(year_str.data(), year_str.data() + year_str.size(), year); 
    std::from_chars(month_str.data(), month_str.data() + month_str.size(), month); 
    std::from_chars(day_str.data(), day_str.data() + day_str.size(), day); 

    auto ymd = std::chrono::year_month_day{std::chrono::year{year}, std::chrono::month{month}, std::chrono::day{day}};
    assert(ymd.ok());

    auto days = std::chrono::sys_days{ymd};
    auto ms = std::chrono::time_point_cast<std::chrono::milliseconds>(days);
    auto val = ms.time_since_epoch().count() - 28800000;
    return val;
}

inline std::vector<std::string> split_str(std::string_view s, std::string_view delim) {
    std::vector<std::string> res;
    size_t pos = 0, nxt = 0, idx = 0;
    while (true) {
        nxt = s.find(delim, pos);
        if (nxt == std::string_view::npos)
            break;
        std::string_view part = s.substr(pos, nxt - pos);
        res.push_back(std::string{part});
        pos = nxt + 1;
        ++idx;
    }
    std::string_view last_part = s.substr(pos);
    res.push_back(std::string{last_part});
    return res;
}
