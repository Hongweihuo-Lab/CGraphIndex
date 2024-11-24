#pragma once

#include <queue>

#include "AdjList.h"

#include "par/par.hpp"
#include "gollum/vertex_subset.hpp"

namespace gollum {

// This partitioner will not partition anything, it will contains only one bucket,
// and this bucket including all vertices. This partitioner is a fallback for serial computing.
class VertexSinglePartitioner {
public:
    class Partition {
        friend class VertexSinglePartitioner;
    public:
        [[nodiscard]] size_t num_vertices() const noexcept { return vertices_.size(); }
        [[nodiscard]] size_t num_buckets() const noexcept { return 1; }

        [[nodiscard]] std::span<const uint64_t> get_bucket(size_t bucket_index) const noexcept {
            return vertices_;
        }

    private:
        std::span<uint64_t> vertices_;

        Partition(std::span<uint64_t> vertices) noexcept
            : vertices_(vertices) { }
    };

public:
    [[nodiscard]] Partition build(AdjList &adjlist, VertexSubset &vertices) {
        return Partition{vertices.unsafe().span()};
    }
};

// This partitioner will partition vertices uniformly into buckets, For a given
// number of buckets `b`, each bucket will contain `ceil(n/b)` vertices.
class VertexUniformPartitioner {
public:
    class Partition {
        friend class VertexUniformPartitioner;
    public:
        [[nodiscard]] size_t num_vertices() const noexcept { return vertices_.size(); }
        [[nodiscard]] size_t num_buckets() const noexcept { return num_buckets_; }

        [[nodiscard]] std::span<const uint64_t> get_bucket(size_t bucket_index) const noexcept {
            assert(bucket_index < num_buckets());
            auto [q, r] = num_vertices_div_num_buckets_;
            size_t scaled_index = bucket_index * q;
            size_t first = scaled_index + std::min(bucket_index, r);
            size_t last = scaled_index + q + std::min(bucket_index + 1, r);
            return vertices_.subspan(first, last - first);
        }

    private:
        size_t                    num_buckets_;
        std::pair<size_t, size_t> num_vertices_div_num_buckets_;
        std::span<const uint64_t> vertices_;

        Partition(size_t num_buckets, std::span<const uint64_t> vertices) noexcept
            : num_buckets_(num_buckets), 
              num_vertices_div_num_buckets_(vertices.size() / num_buckets, vertices.size() % num_buckets),
              vertices_(vertices) { }
    };

public:
    VertexUniformPartitioner(size_t num_buckets) noexcept
        : num_buckets_(num_buckets) { }

    [[nodiscard]] Partition build(AdjList &adjlist, VertexSubset &vertices) {
        return Partition{num_buckets_, vertices.unsafe().span()};
    }

private:
    size_t num_buckets_;
};

// This partitioner use a greedy algorithm to partition the vertices by their in(out)degrees,
// it provides better load balancing, but requires more overhead to partition the vertices.
class VertexDegreePartitioner {
public:
    class Partition {
        friend class VertexDegreePartitioner;
    public:
        [[nodiscard]] size_t num_vertices() const noexcept { return shuffled_vertices_.size(); }
        [[nodiscard]] size_t num_buckets() const noexcept { return buckets_.size(); }

        [[nodiscard]] std::span<const uint64_t> get_bucket(size_t bucket_index) const noexcept {
            assert(bucket_index < num_buckets());
            return buckets_[bucket_index];
        }
    
    private:
        std::vector<uint64_t>              shuffled_vertices_;
        std::vector<std::span<uint64_t>>   buckets_;
        
        Partition(std::vector<uint64_t> &&shuffled_vertices, std::vector<std::span<uint64_t>> &&buckets) noexcept
            : shuffled_vertices_(std::move(shuffled_vertices)), buckets_(std::move(buckets)) { }
    };

    enum By { kIndegree, kOutdegree };

public:
    [[nodiscard]] Partition build(AdjList &adjlist, VertexSubset &vertices) {
        auto unsafe_vertices_view = vertices.unsafe();
        if (by_ == By::kIndegree) {
            par::sort(unsafe_vertices_view.begin(), unsafe_vertices_view.end(), [&adjlist](uint64_t a, uint64_t b) {
                return adjlist.GetInNums(a) < adjlist.GetInNums(b);
            });
        } else {
            par::sort(unsafe_vertices_view.begin(), unsafe_vertices_view.end(), [&adjlist](uint64_t a, uint64_t b) {
                return adjlist.GetOutNums(a) < adjlist.GetOutNums(b);
            });
        }

        auto origin_vertices = unsafe_vertices_view.span();
        const size_t num_vertices = origin_vertices.size();
        std::vector<std::span<uint64_t>> buckets;
        std::vector<uint64_t> shuffled_vertices;
        buckets.resize(num_buckets_);
        shuffled_vertices.resize(num_vertices);

        size_t writer_index = 0;
        for (size_t i = 0; i < num_buckets_; ++i) {
            size_t start = writer_index;
            for (size_t j = i; j < num_vertices; j += num_buckets_) {
                shuffled_vertices[writer_index++] = origin_vertices[j];
            }
            buckets[i] = std::span{shuffled_vertices.begin() + start, shuffled_vertices.begin() + writer_index};
        }

        return Partition{std::move(shuffled_vertices), std::move(buckets)};
    }

    [[nodiscard]] static VertexDegreePartitioner by_indegree(size_t num_buckets) noexcept {
        return VertexDegreePartitioner{By::kIndegree, num_buckets};
    }

    [[nodiscard]] static VertexDegreePartitioner by_outdegree(size_t num_buckets) {
        return VertexDegreePartitioner{By::kOutdegree, num_buckets};
    }

private:
    By     by_;
    size_t num_buckets_;

    VertexDegreePartitioner(By by, size_t num_buckets) noexcept
        : by_(by), num_buckets_(num_buckets) { }
};

class VertexDegreeGreedyPartitioner {
public:
    class Partition {
        friend class VertexDegreeGreedyPartitioner;
    public:
        [[nodiscard]] size_t num_vertices() const noexcept { return num_vertices_; }
        [[nodiscard]] size_t num_buckets() const noexcept { return buckets_.size(); }

        [[nodiscard]] std::span<const uint64_t> get_bucket(size_t bucket_index) const noexcept {
            assert(bucket_index < num_buckets());
            return buckets_[bucket_index];
        }
    
    private:
        size_t num_vertices_;
        std::vector<std::vector<uint64_t>> buckets_;
        
        Partition(size_t num_vertices, std::vector<std::vector<uint64_t>> &&buckets) noexcept
            : num_vertices_(num_vertices), buckets_(std::move(buckets)) { }
    };

    enum By { kIndegree, kOutdegree };

public:
    [[nodiscard]] Partition build(AdjList &adjlist, VertexSubset &vertices) {
        std::vector<std::vector<uint64_t>> buckets;
        buckets.resize(num_buckets_);

        struct OrderedBucketInfo {
            std::vector<uint64_t> *bucket;
            uint64_t               size;
        };
        struct OrderedBucketInfoCmp {
            bool operator()(const OrderedBucketInfo &a, const OrderedBucketInfo &b) const noexcept {
                return std::greater<uint64_t>{}(a.size, b.size);
            }
        };
        std::priority_queue<OrderedBucketInfo, std::vector<OrderedBucketInfo>, OrderedBucketInfoCmp> candidates;
        for (size_t i = 0; i < num_buckets_; ++i)
            candidates.emplace(std::addressof(buckets[i]), 0);
        
        auto vids = vertices.unsafe().span();
        for (size_t i = 0; i < vids.size(); ++i) {
            size_t degree = adjlist.GetOutNums(vids[i]);
            OrderedBucketInfo info = candidates.top(); candidates.pop();
            info.bucket->push_back(vids[i]);
            info.size += degree;
            candidates.push(info);
        }

        return Partition{vertices.unsafe().size(), std::move(buckets)};
    }

    [[nodiscard]] static VertexDegreeGreedyPartitioner by_indegree(size_t num_buckets) noexcept {
        return VertexDegreeGreedyPartitioner{By::kIndegree, num_buckets};
    }

    [[nodiscard]] static VertexDegreeGreedyPartitioner by_outdegree(size_t num_buckets) {
        return VertexDegreeGreedyPartitioner{By::kOutdegree, num_buckets};
    }

private:
    By     by_;
    size_t num_buckets_;

    VertexDegreeGreedyPartitioner(By by, size_t num_buckets) noexcept
        : by_(by), num_buckets_(num_buckets) { }
};

}