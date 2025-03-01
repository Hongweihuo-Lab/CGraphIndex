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

#include "gollum/execution.hpp"
#include "gollum/vertex_partitioner.hpp"
#include "gollum/vertex_subset.hpp"

#include "graph_re.hpp"

namespace gollum {

enum class PathExpandDir { kForward, kBackward };

template<typename SerF, typename ParF>
class EdgeFilter {
    SerF ser_filter_;
    ParF par_filter_;
public:
    EdgeFilter(SerF ser, ParF par)
        : ser_filter_(ser), par_filter_(par) { }

    [[nodiscard]] bool operator()(SerExecutionTag, uint32_t task_index, uint64_t from, uint64_t to) noexcept {
        return ser_filter_(task_index, from, to);
    }
    [[nodiscard]] bool operator()(ParExecutionTag, uint32_t task_index, uint64_t from, uint64_t to) noexcept {
        return par_filter_(task_index, from, to);
    }
};

template<typename SerF, typename ParF>
class EdgeFunctor {
    SerF ser_functor_;
    ParF par_functor_;
public:
    EdgeFunctor(SerF ser, ParF par)
        : ser_functor_(ser), par_functor_(par) { }

    void operator()(SerExecutionTag, uint32_t task_index, uint64_t from, uint64_t to, VertexSubset &nxt) noexcept {
        ser_functor_(task_index, from, to, nxt);
    }
    void operator()(ParExecutionTag, uint32_t task_index, uint64_t from, uint64_t to, VertexSubset &nxt) noexcept {
        par_functor_(task_index, from, to, nxt);
    }
};

template<typename SerF, typename ParF>
[[nodiscard]] inline EdgeFilter<std::decay_t<SerF>, std::decay_t<ParF>> 
make_edge_filter(SerF &&ser, ParF &&par) {
    return EdgeFilter<std::decay_t<SerF>, std::decay_t<ParF>>{std::forward<SerF>(ser), std::forward<ParF>(par)};
}

template<typename SerF, typename ParF>
[[nodiscard]] inline EdgeFunctor<std::decay_t<SerF>, std::decay_t<ParF>> 
make_edge_functor(SerF &&ser, ParF &&par) {
    return EdgeFunctor<std::decay_t<SerF>, std::decay_t<ParF>>{std::forward<SerF>(ser), std::forward<ParF>(par)};
}

namespace details {

template<PathExpandDir Dir>
[[nodiscard]] auto GetAllNeighbors(Graph &g, uint64_t vid) {
    if constexpr (Dir == PathExpandDir::kForward)
        return g.GetAdjList().GetNextVid(vid);
    else
        return g.GetAdjList().GetPrevVid(vid);
}

template<PathExpandDir Dir>
[[nodiscard]] auto GetAllNeighbors(Graph &g, uint64_t vid, const std::string &neighbor_vtype) {
    GraphRe graph{g};

    if constexpr (Dir == PathExpandDir::kForward)
        return graph.get_outgoing_edges(VID{vid}, neighbor_vtype);
    else
        return graph.get_incoming_edges(VID{vid}, neighbor_vtype);
}

template<PathExpandDir Dir, typename FilterT, typename FunctorT>
void PathExpandSerial(Graph &g, VertexSubset &cur, VertexSubset &nxt, FilterT filter, FunctorT func) {
    VertexSinglePartitioner partitioner;
    auto partition = partitioner.build(g.GetAdjList(), cur);
    auto vertices = partition.get_bucket(0);

    for (uint64_t from_vid : vertices) {
        auto to_vids = GetAllNeighbors<Dir>(g, from_vid);
        for (uint64_t to_vid : to_vids) {
            if (filter(ser_execution, 0, from_vid, to_vid))
                func(ser_execution, 0, from_vid, to_vid, nxt);
        }
    }
}

template<PathExpandDir Dir, typename FilterT, typename FunctorT>
void PathExpandSerial(Graph &g, const std::string &neighbor_vtype, VertexSubset &cur, VertexSubset &nxt, FilterT filter, FunctorT func) {
    VertexSinglePartitioner partitioner;
    auto partition = partitioner.build(g.GetAdjList(), cur);
    auto vertices = partition.get_bucket(0);

    for (uint64_t from_vid : vertices) {
        auto to_vids = GetAllNeighbors<Dir>(g, from_vid, neighbor_vtype);
        for (auto to_vid : to_vids) {
            if (filter(ser_execution, 0, from_vid, to_vid.value()))
                func(ser_execution, 0, from_vid, to_vid.value(), nxt);
        }
    }
}

template<PathExpandDir Dir, typename PartitionerT, typename FilterT, typename FunctorT>
void PathExpandParallelWithVertexPartitioner(PartitionerT partitioner, Graph &g,
                                             VertexSubset &cur, VertexSubset &nxt, 
                                             FilterT filter, FunctorT func) {
    auto partition = partitioner.build(g.GetAdjList(), cur);
    std::vector<std::thread> threads{partition.num_buckets()};

    struct Task {
        Graph                      *graph;
        uint32_t                    task_index;
        FilterT                     filter;
        FunctorT                    functor;
        std::span<const uint64_t>   vertices;
        VertexSubset               *nxt;

        Task(Graph &g, uint32_t task_index, FilterT filter, FunctorT functor, 
             std::span<const uint64_t> vertices, VertexSubset &nxt)
            : graph(std::addressof(g)), task_index(task_index), 
              filter(filter), functor(functor),
              vertices(vertices), nxt(std::addressof(nxt)) { }

        void operator()() {
            for (uint64_t from_vid : vertices) {
                auto to_vids = GetAllNeighbors<Dir>(*graph, from_vid);
                for (uint64_t to_vid : to_vids) {
                    if (filter(par_execution, task_index, from_vid, to_vid))
                        functor(par_execution, task_index, from_vid, to_vid, *nxt);
                }
            }
        }
    };

    for (size_t i = 0; i < threads.size(); ++i) {
        auto vertices = partition.get_bucket(i);
        if (vertices.empty())
            continue;
        threads[i] = std::thread{Task{g, static_cast<uint32_t>(i), filter, func, vertices, nxt}};
    }

    for (size_t i = 0; i < threads.size(); ++i) {
        if (threads[i].joinable())
            threads[i].join();
    }
}

template<PathExpandDir Dir, typename PartitionerT, typename FilterT, typename FunctorT>
void PathExpandParallelWithVertexPartitioner(PartitionerT partitioner, Graph &g,
                                             const std::string &neighbor_vtype,
                                             VertexSubset &cur, VertexSubset &nxt, 
                                             FilterT filter, FunctorT func) {
    auto partition = partitioner.build(g.GetAdjList(), cur);
    std::vector<std::thread> threads{partition.num_buckets()};

    struct Task {
        Graph                      *graph;
        std::string                 neighbor_vtype;                
        uint32_t                    task_index;
        FilterT                     filter;
        FunctorT                    functor;
        std::span<const uint64_t>   vertices;
        VertexSubset               *nxt;

        Task(Graph &g, const std::string &ntype, uint32_t task_index, FilterT filter, FunctorT functor, 
             std::span<const uint64_t> vertices, VertexSubset &nxt)
            : graph(std::addressof(g)), neighbor_vtype(ntype), task_index(task_index), 
              filter(filter), functor(functor),
              vertices(vertices), nxt(std::addressof(nxt)) { }

        void operator()() {
            for (uint64_t from_vid : vertices) {
                auto to_vids = GetAllNeighbors<Dir>(*graph, from_vid, neighbor_vtype);
                for (auto to_vid : to_vids) {
                    if (filter(par_execution, task_index, from_vid, to_vid.value()))
                        functor(par_execution, task_index, from_vid, to_vid.value(), *nxt);
                }
            }
        }
    };

    for (size_t i = 0; i < threads.size(); ++i) {
        auto vertices = partition.get_bucket(i);
        if (vertices.empty())
            continue;
        threads[i] = std::thread{Task{g, neighbor_vtype, static_cast<uint32_t>(i), filter, func, vertices, nxt}};
    }

    for (size_t i = 0; i < threads.size(); ++i) {
        if (threads[i].joinable())
            threads[i].join();
    }
}

enum class PathExpandHeuristicStrategy {
    kSerial, kDegreeVertexPartition, kUniformVertexPartition
};

[[nodiscard]] inline PathExpandHeuristicStrategy 
SelectPathExpandStrategy(uint32_t max_parallelism, size_t total_degree) {
    constexpr size_t kSerialThreshold = 1 << 16;
    constexpr size_t kDegreePerTaskThreshold = 1 << 26;

    if (total_degree <= kSerialThreshold)
        return PathExpandHeuristicStrategy::kSerial;
    else if (total_degree >= max_parallelism * kDegreePerTaskThreshold)
        return PathExpandHeuristicStrategy::kDegreeVertexPartition;
    return PathExpandHeuristicStrategy::kUniformVertexPartition;
}

}

enum class PathExpandHint {
    kDefault, kSerial, kDegreeVertexPartition, kUniformVertexPartition
};

template<typename EdgeFilterT, typename EdgeFunctorT>
void path_expand_forward(uint32_t max_parallelism, Graph &g, VertexSubset &cur, VertexSubset &nxt, 
                         EdgeFilterT filter, EdgeFunctorT f) {
    using enum details::PathExpandHeuristicStrategy;
    auto strategy = details::SelectPathExpandStrategy(max_parallelism, cur.unsafe().total_outdegree());
    if (strategy == kSerial) {
        details::PathExpandSerial<PathExpandDir::kForward>(g, cur, nxt, filter, f);
    } else if (strategy == kDegreeVertexPartition) {
        details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kForward>(
            VertexDegreePartitioner::by_outdegree(max_parallelism),
            g, cur, nxt, filter, f);
    } else if (strategy == kUniformVertexPartition) {
        details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kForward>(
            VertexUniformPartitioner{max_parallelism},
            g, cur, nxt, filter, f);
    }
}

template<typename EdgeFilterT, typename EdgeFunctorT>
void path_expand_forward(uint32_t max_parallelism, Graph &g, const std::string &ntype, VertexSubset &cur, VertexSubset &nxt, 
                         EdgeFilterT filter, EdgeFunctorT f, PathExpandHint hint = PathExpandHint::kDefault) {
    using enum details::PathExpandHeuristicStrategy;
    if (hint == PathExpandHint::kDefault) {
        auto strategy = details::SelectPathExpandStrategy(max_parallelism, cur.unsafe().total_outdegree());
        if (strategy == kSerial) {
            details::PathExpandSerial<PathExpandDir::kForward>(g, ntype, cur, nxt, filter, f);
        } else if (strategy == kDegreeVertexPartition) {
            details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kForward>(
                VertexDegreePartitioner::by_outdegree(max_parallelism),
                g, ntype, cur, nxt, filter, f);
        } else if (strategy == kUniformVertexPartition) {
            details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kForward>(
                VertexUniformPartitioner{max_parallelism},
                g, ntype, cur, nxt, filter, f);
        }
    } else if (hint == PathExpandHint::kSerial) {
        details::PathExpandSerial<PathExpandDir::kForward>(g, ntype, cur, nxt, filter, f);
    } else if (hint == PathExpandHint::kUniformVertexPartition) {
        details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kForward>(
            VertexUniformPartitioner{max_parallelism},
            g, ntype, cur, nxt, filter, f);
    } else if (hint == PathExpandHint::kDegreeVertexPartition) {
        details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kForward>(
            VertexDegreePartitioner::by_outdegree(max_parallelism),
            g, ntype, cur, nxt, filter, f);
    }
}

template<typename EdgeFilterT, typename EdgeFunctorT>
void path_expand_backward(uint32_t max_parallelism, Graph &g, VertexSubset &cur, VertexSubset &nxt, 
                          EdgeFilterT filter, EdgeFunctorT f) {
    using enum details::PathExpandHeuristicStrategy;
    auto strategy = details::SelectPathExpandStrategy(max_parallelism, cur.unsafe().total_indegree());
    if (strategy == kSerial) {
        details::PathExpandSerial<PathExpandDir::kBackward>(g, cur, nxt, filter, f);
    } else if (strategy == kDegreeVertexPartition) {
        details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kBackward>(
            VertexDegreePartitioner::by_indegree(max_parallelism),
            g, cur, nxt, filter, f);
    } else if (strategy == kUniformVertexPartition) {
        details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kBackward>(
            VertexUniformPartitioner{max_parallelism},
            g, cur, nxt, filter, f);
    }
}

template<typename EdgeFilterT, typename EdgeFunctorT>
void path_expand_backward(uint32_t max_parallelism, Graph &g, const std::string &ntype, VertexSubset &cur, VertexSubset &nxt, 
                          EdgeFilterT filter, EdgeFunctorT f, PathExpandHint hint = PathExpandHint::kDefault) {
    using enum details::PathExpandHeuristicStrategy;

    if (hint == PathExpandHint::kDefault) {
        auto strategy = details::SelectPathExpandStrategy(max_parallelism, cur.unsafe().total_indegree());
        if (strategy == kSerial) {
            details::PathExpandSerial<PathExpandDir::kBackward>(g, ntype, cur, nxt, filter, f);
        } else if (strategy == kDegreeVertexPartition) {
            details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kBackward>(
                VertexDegreePartitioner::by_indegree(max_parallelism),
                g, ntype, cur, nxt, filter, f);
        } else if (strategy == kUniformVertexPartition) {
            details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kBackward>(
                VertexUniformPartitioner{max_parallelism},
                g, ntype, cur, nxt, filter, f);
        }
    } else if (hint == PathExpandHint::kSerial) {
        details::PathExpandSerial<PathExpandDir::kBackward>(g, ntype, cur, nxt, filter, f);
    } else if (hint == PathExpandHint::kUniformVertexPartition) {
        details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kBackward>(
            VertexUniformPartitioner{max_parallelism},
            g, ntype, cur, nxt, filter, f);
    } else if (hint == PathExpandHint::kDegreeVertexPartition) {
        details::PathExpandParallelWithVertexPartitioner<PathExpandDir::kBackward>(
            VertexDegreePartitioner::by_indegree(max_parallelism),
            g, ntype, cur, nxt, filter, f);
    }
}

}