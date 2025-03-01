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

#include "page_rank.hpp"

#include <algorithm>
#include <cassert>
#include <span>
#include <queue>

#include "par/par.hpp"

struct PageRankDoubleBuffer {
    double *old_ranks;
    double *new_ranks;

    PageRankDoubleBuffer(double *old_ranks, double *new_ranks) noexcept
        : old_ranks(old_ranks), new_ranks(new_ranks) { }

    void Next() { std::swap(old_ranks, new_ranks); }
};

struct SharedInNeighborSpan {
    uint64_t  dst;
    u64      *data;
    size_t    size;
};

class WorkloadPartitioner {
public:
    WorkloadPartitioner(AdjList &adjlist, size_t num_bucket)
        : adjlist_(std::addressof(adjlist)),
          num_bucket_(num_bucket) {
        InitBuckets();
    }

    [[nodiscard]] const std::vector<uint64_t> &GetUniqueBucket(size_t idx) const noexcept {
        assert(idx < num_bucket_);
        return unique_buckets_[idx];
    }

    [[nodiscard]] size_t GetUniqueBucketMaxIndegree(size_t idx) const noexcept {
        assert(idx < num_bucket_);
        return unique_bucket_max_indegrees_[idx];
    }

    [[nodiscard]] const std::vector<SharedInNeighborSpan> &GetSharedBucket(size_t idx) const noexcept {
        assert(idx < num_bucket_);
        return shared_buckets_[idx];
    }

    [[nodiscard]] const std::vector<uint64_t> &GetExceptionVids() const noexcept {
        return exception_vids_;
    }

    [[nodiscard]] size_t NumBucket() const noexcept { return num_bucket_; }
    [[nodiscard]] size_t MaxBucketCapacity() const noexcept { return max_bucket_capacity_; }

private:
    AdjList                                        *adjlist_;
    size_t                                          num_bucket_;
    size_t                                          max_bucket_capacity_;
    std::vector<std::vector<uint64_t>>              unique_buckets_;
    std::vector<size_t>                             unique_bucket_max_indegrees_;
    std::vector<std::vector<SharedInNeighborSpan>>  shared_buckets_;
    std::vector<u64>                                uncompressed_in_neighbors_;
    std::vector<uint64_t>                           exception_vids_;

    void InitBuckets() {
        const uint64_t num_edge = adjlist_->GetEnum();
        const uint64_t num_vertex = adjlist_->GetVnum();

        max_bucket_capacity_ = num_edge / num_bucket_ + static_cast<size_t>(num_edge % num_bucket_ != 0);

        unique_buckets_.resize(num_bucket_);
        unique_bucket_max_indegrees_.resize(num_bucket_, 0);
        shared_buckets_.resize(num_bucket_);

        struct OrderedBucketInfo {
            std::vector<uint64_t> *bucket;
            size_t                *max_indegree;
            uint64_t               size;
        };
        struct OrderedBucketInfoCmp {
            bool operator()(const OrderedBucketInfo &a, const OrderedBucketInfo &b) const noexcept {
                return std::greater<uint64_t>{}(a.size, b.size);
            }
        };
        std::priority_queue<OrderedBucketInfo, std::vector<OrderedBucketInfo>, OrderedBucketInfoCmp> candidates;
        for (size_t i = 0; i < num_bucket_; ++i)
            candidates.emplace(std::addressof(unique_buckets_[i]), std::addressof(unique_bucket_max_indegrees_[i]), 0);
        
        struct ExceptionVertex {
            uint64_t  id;
            uint64_t  indegree;
            u64      *data;

            ExceptionVertex(uint64_t id, uint64_t indegree, u64 *data = nullptr)
                : id(id), indegree(indegree), data(data) { }
        };

        std::vector<ExceptionVertex> exceptions;
        for (size_t vid = 0; vid < num_vertex; ++vid) {
            uint64_t indegree = adjlist_->GetInNums(vid);
            OrderedBucketInfo bkt_info = candidates.top();
            if (bkt_info.size + indegree > max_bucket_capacity_) {
                exceptions.push_back(ExceptionVertex{vid, indegree});
            } else {
                candidates.pop();
                bkt_info.bucket->push_back(vid);
                *bkt_info.max_indegree = std::max(*bkt_info.max_indegree, indegree);
                bkt_info.size += indegree;
                candidates.push(bkt_info);
            }
        }
        exception_vids_.resize(exceptions.size());
        for (size_t i = 0; i < exceptions.size(); ++i)
            exception_vids_[i] = exceptions[i].id;

        if (!exceptions.empty()) {
            size_t total_uncompressed = 0;
            for (const auto &e : exceptions)
                total_uncompressed += e.indegree;
            uncompressed_in_neighbors_.resize(total_uncompressed);
            size_t writer_index = 0;
            for (auto &e : exceptions) {
                adjlist_->__get_input_neighbors(
                    e.id, uncompressed_in_neighbors_.data() + writer_index, 
                    uncompressed_in_neighbors_.size() - writer_index
                );
                e.data = uncompressed_in_neighbors_.data() + writer_index;
                writer_index += e.indegree;
            }

            for (size_t i = 0; i < exceptions.size(); ++i) {
                ExceptionVertex e = exceptions[i];
                size_t offset = 0;
                while (offset < e.indegree) {
                    size_t unconsumed = e.indegree - offset;
                    auto info = candidates.top(); candidates.pop();
                    size_t n = std::min(max_bucket_capacity_ - info.size, unconsumed);
                    SharedInNeighborSpan span;
                    span.dst = e.id;
                    span.data = e.data + offset;
                    span.size = n;
                    offset += n;
                    size_t bkt_id = info.bucket - unique_buckets_.data();
                    shared_buckets_[bkt_id].push_back(span);
                    if (info.size + n != max_bucket_capacity_) {
                        info.size += n;
                        candidates.push(info);
                    }
                }
            }
        }
    }
};

struct PageRankPrepareTask {
    AdjList &adjlist;
    size_t   range_first;
    size_t   range_last;
    double  *old_ranks;
    double  *local_sink_sum;

    void operator()() const {
        for (size_t i = range_first; i < range_last; ++i) {
            uint64_t outdegree = adjlist.GetOutNums(i);
            if (outdegree != 0) {
                old_ranks[i] /= static_cast<double>(outdegree);
            } else {
                *local_sink_sum += old_ranks[i];
            }
        }
    }
};

struct PageRankComputeTask {
    const double                             inv_num_vertex;
    AdjList                                 &adjlist;
    const std::vector<uint64_t>             &unique_bucket;
    const std::vector<SharedInNeighborSpan> &shared_bucket;
    double                                   dampling_factor;
    double                                   sink_sum;
    double                                  *old_ranks;
    double                                  *new_ranks;
    std::span<u64>                           local_vid_buffer;
    std::vector<double>                     *local_rank_sum_buffer;

    void operator()() const {
        for (auto dst_vid : unique_bucket) {
            //std::vector<u64> src_vids = adjlist.GetPrevVid(dst_vid);
            size_t indegree = adjlist.__get_input_neighbors(dst_vid, local_vid_buffer.data(), local_vid_buffer.size());
            std::span<u64> src_vids{local_vid_buffer.data(), indegree};
            double rank_sum = 0.0;
            for (u64 src_vid : src_vids)
                rank_sum += old_ranks[src_vid];
            new_ranks[dst_vid] = rank_sum * dampling_factor + 
                (1.0 - dampling_factor + dampling_factor * sink_sum) * inv_num_vertex;
        }

        for (size_t i = 0; i < shared_bucket.size(); ++i) {
            const auto &span = shared_bucket[i];
            uint64_t dst_vid = span.dst;
            double rank_sum = 0.0;
            for (size_t j = 0; j < span.size; ++j) {
                uint64_t src_vid = span.data[j];
                rank_sum += old_ranks[src_vid];
            }
            (*local_rank_sum_buffer)[i] = rank_sum;
        }
    }
};

SerialPageRankQuery::SerialPageRankQuery()
    : Query(1) { }

const char *SerialPageRankQuery::name() const noexcept { return "serial_page_rank"; }

bool SerialPageRankQuery::is_parallel() const noexcept { return false; }

size_t SerialPageRankQuery::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept {
    return graph.GetAdjList().GetVnum() * sizeof(double);
}

// args = (iterations:u64, damping_factor:f64)
void SerialPageRankQuery::execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                                  const QueryArgs &args, ResultTable *result_out) {
    AdjList &adjlist = graph.GetAdjList();
    const uint64_t num_vertices = adjlist.GetVnum();
    const uint64_t num_edges = adjlist.GetEnum();
    const double inv_num_vertex = 1.0 / static_cast<double>(num_vertices);

    const uint64_t iterations = args.get_u64(0);
    const double damping_factor = args.get_f64(1);

    double *aux_buf = reinterpret_cast<double *>(temp_storage);
    ResultTable &result = *result_out;
    if (result.num_rows() < num_vertices)
        result.resize(num_vertices);
    double *out_buf = result.get_f64(0).data();

    PageRankDoubleBuffer buf{nullptr, nullptr};
    if (iterations % 2 == 0) {
        buf.old_ranks = out_buf;
        buf.new_ranks = aux_buf;
    } else {
        buf.old_ranks = aux_buf;
        buf.new_ranks = out_buf;
    }

    std::fill_n(buf.old_ranks, num_vertices, inv_num_vertex);

    for (uint32_t iter_index = 0; iter_index < iterations; ++iter_index) {
        double sink_sum = 0.0;
        for (uint64_t i = 0; i < num_vertices; ++i) {
            uint64_t outdegree = adjlist.GetOutNums(i);
            if (outdegree != 0) {
                buf.old_ranks[i] /= static_cast<double>(outdegree);
            } else {
                sink_sum += buf.old_ranks[i];
            }
        }

        for (uint64_t i = 0; i < num_vertices; ++i) {
            buf.new_ranks[i] = (1.0 - damping_factor + damping_factor * sink_sum) * inv_num_vertex;
            auto vec = adjlist.GetPrevVid(i);
            double rank_sum = 0.0;
            for (size_t j = 0; j < vec.size(); ++j)
                rank_sum += vec[j];
            buf.new_ranks[i] += damping_factor * rank_sum;
        }
        buf.Next();
    }
}

ParallelPageRankQuery::ParallelPageRankQuery(uint32_t max_parallelism)
    : Query(max_parallelism) { }

const char *ParallelPageRankQuery::name() const noexcept { return "parallel_page_rank"; }

bool ParallelPageRankQuery::is_parallel() const noexcept { return true; }

size_t ParallelPageRankQuery::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept {
    return graph.GetAdjList().GetVnum() * sizeof(double);
}

// args = (iterations:u64, damping_factor:f64)
void ParallelPageRankQuery::execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                                    const QueryArgs &args, ResultTable *result_out) {
    AdjList &adjlist = graph.GetAdjList();
    const uint64_t num_vertices = adjlist.GetVnum();
    const uint64_t num_edges = adjlist.GetEnum();
    const double inv_num_vertex = 1.0 / static_cast<double>(num_vertices);

    const uint32_t num_threads = max_parallelism();
    const uint64_t iterations = args.get_u64(0);
    const double damping_factor = args.get_f64(1);

    double *aux_buf = reinterpret_cast<double *>(temp_storage);
    ResultTable &result = *result_out;
    if (result.num_rows() < num_vertices)
        result.resize(num_vertices);
    double *out_buf = result.get_f64(0).data();

    PageRankDoubleBuffer buf{nullptr, nullptr};
    if (iterations % 2 == 0) {
        buf.old_ranks = out_buf;
        buf.new_ranks = aux_buf;
    } else {
        buf.old_ranks = aux_buf;
        buf.new_ranks = out_buf;
    }

    std::vector<double> thread_local_sink_sum_buffers(num_threads, 0.0);
    std::vector<size_t> sink_task_bounds(num_threads + 1, 0);
    {
        size_t q = num_vertices / num_threads, r = num_vertices % num_threads;
        std::fill_n(sink_task_bounds.begin() + 1, r, q + 1);
        std::fill(sink_task_bounds.begin() + 1 + r, sink_task_bounds.end(), q);

        for (size_t i = 1; i <= num_threads; ++i) {
            sink_task_bounds[i] += sink_task_bounds[i - 1];
        }
    }

    WorkloadPartitioner partitioner{adjlist, num_threads};

    std::vector<std::vector<double>> thread_local_rank_buffers{num_threads};
    for (size_t i = 0; i < thread_local_rank_buffers.size(); ++i)
        thread_local_rank_buffers[i].resize(partitioner.GetSharedBucket(i).size());
    
    std::unique_ptr<u64 []> thread_local_vid_memory;
    std::vector<std::span<u64>> thread_local_vid_buffers(num_threads);
    
    {
        size_t thread_local_vid_buffer_memory_size = 0;
        for (size_t i = 0; i < partitioner.NumBucket(); ++i)
            thread_local_vid_buffer_memory_size += partitioner.GetUniqueBucketMaxIndegree(i);
        thread_local_vid_memory = std::make_unique<u64 []>(thread_local_vid_buffer_memory_size);

        for (size_t i = 0, offset = 0; i < partitioner.NumBucket(); ++i) {
            size_t num = partitioner.GetUniqueBucketMaxIndegree(i);
            thread_local_vid_buffers[i] = std::span<u64>{thread_local_vid_memory.get() + offset, num};
            offset += num;
        }
    }

    par::fill_n(buf.old_ranks, num_vertices, inv_num_vertex);

    for (uint32_t iter_index = 0; iter_index < iterations; ++iter_index) {
        // Prepare stage.
        par::launch_tasks(num_threads, [&](uint32_t i) {
            return PageRankPrepareTask{
                adjlist,
                sink_task_bounds[i],
                sink_task_bounds[i + 1],
                buf.old_ranks,
                std::addressof(thread_local_sink_sum_buffers[i])
            };
        });
        double sink_sum = 0.0;
        for (size_t i = 0; i < num_threads; ++i)
            sink_sum += thread_local_sink_sum_buffers[i];

        // Compute stage.
        par::launch_tasks(num_threads, [&](uint32_t i) {
            return PageRankComputeTask{
                inv_num_vertex,
                adjlist, 
                partitioner.GetUniqueBucket(i),
                partitioner.GetSharedBucket(i),
                damping_factor,
                sink_sum,
                buf.old_ranks,
                buf.new_ranks,
                thread_local_vid_buffers[i],
                std::addressof(thread_local_rank_buffers[i])
            };
        });

        for (size_t i = 0; i < partitioner.NumBucket(); ++i) {
            const auto &bkt = partitioner.GetSharedBucket(i);
            for (size_t j = 0; j < bkt.size(); ++j) {
                const auto &span = bkt[j];
                buf.new_ranks[span.dst] += thread_local_rank_buffers[i][j];
            }
        }
        for (auto vid : partitioner.GetExceptionVids()) {
            buf.new_ranks[vid] = buf.new_ranks[vid] * damping_factor + 
                (1.0 - damping_factor + damping_factor * sink_sum) * inv_num_vertex;
        }
        buf.Next();
    }
}