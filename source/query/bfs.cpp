#include "bfs.hpp"

#include <iomanip>
#include <ranges>

#include "gollum/path_expand.hpp"

SerialBfsQuery::SerialBfsQuery()
    : Query(1) { }

const char *SerialBfsQuery::name() const noexcept { return "serial_bfs"; }
bool SerialBfsQuery::is_parallel() const noexcept { return false; }

size_t SerialBfsQuery::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept { return 0; }

// args = (src:u64)
void SerialBfsQuery::execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                             const QueryArgs &args, ResultTable *result_out) { 
    AdjList &adjlist = graph.GetAdjList();
    uint64_t src_vid = graph.GetRealId("person", args.get_u64(0));

    if (result_out->num_rows() < adjlist.GetVnum())
        result_out->resize(adjlist.GetVnum());
    ResultTable &result = *result_out;
    std::span<uint64_t> depths = result.get_u64(0);

    std::fill_n(depths.begin(), depths.size(), std::numeric_limits<uint64_t>::max());

    std::queue<uint64_t> queue;
    queue.push(src_vid);
    depths[src_vid] = 0;

    while (!queue.empty()) {
        uint64_t u = queue.front(); queue.pop();
        uint64_t new_depth = depths[u] + 1;

        auto out_neighbors = adjlist.GetNextVid(u);
        for (auto v : out_neighbors) {
            if (depths[v] == std::numeric_limits<uint64_t>::max()) {
                depths[v] = new_depth;
                queue.push(v);
            }
        }
    }
}

ParallelBfsQuery::ParallelBfsQuery(uint32_t max_parallelism)
    : Query(max_parallelism) { }

const char *ParallelBfsQuery::name() const noexcept { return "parallel_bfs"; }
bool ParallelBfsQuery::is_parallel() const noexcept { return true; }

size_t ParallelBfsQuery::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept { 
    size_t num_vertices = graph.GetAdjList().GetVnum();
    return (sizeof(bool) * num_vertices + sizeof(uint64_t) * num_vertices) * 2;
}

// args = (src:u64)
void ParallelBfsQuery::execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                               const QueryArgs &args, ResultTable *result_out) { 
    AdjList &adjlist = graph.GetAdjList();
    uint64_t src_vid = graph.GetRealId("person", args.get_u64(0));

    if (result_out->num_rows() < adjlist.GetVnum())
        result_out->resize(adjlist.GetVnum());
    ResultTable &result = *result_out;
    std::span<uint64_t> depths = result.get_u64(0);

    auto reinterpret_uint64_buffer = [temp_storage](size_t byte_offset, size_t n) {
        return std::span<uint64_t>{
            reinterpret_cast<uint64_t *>(temp_storage + byte_offset), n
        };
    };
    auto reinterpret_bool_buffer = [temp_storage](size_t byte_offset, size_t n) {
        return std::span<bool>{
            reinterpret_cast<bool *>(temp_storage + byte_offset), n
        };
    };

    const size_t num_vertices = adjlist.GetVnum();
    auto frontier1_shared_buffer_mem = reinterpret_uint64_buffer(0, num_vertices);
    auto frontier2_shared_buffer_mem = reinterpret_uint64_buffer(num_vertices * sizeof(uint64_t), num_vertices);
    auto frontier1_flags_mem = reinterpret_bool_buffer(num_vertices * sizeof(uint64_t) * 2, num_vertices);
    auto frontier2_flags_mem = reinterpret_bool_buffer(num_vertices * sizeof(uint64_t) * 2 + num_vertices * sizeof(bool), num_vertices);

    gollum::VertexSubset frontier1{graph.GetAdjList(), max_parallelism(), frontier1_shared_buffer_mem, frontier1_flags_mem};
    gollum::VertexSubset frontier2{graph.GetAdjList(), max_parallelism(), frontier2_shared_buffer_mem, frontier2_flags_mem};
    gollum::VertexSubset *cur_frontier = std::addressof(frontier1);
    gollum::VertexSubset *nxt_frontier = std::addressof(frontier2);

    cur_frontier->unsafe().add_vertex(src_vid);
    depths[src_vid] = 0;
    size_t cur_depth = 1;

    while (!cur_frontier->unsafe().empty()) {
        gollum::path_expand_forward(max_parallelism(), adjlist, *cur_frontier, *nxt_frontier,
            gollum::make_edge_filter(
                [&](uint32_t task_index, uint64_t src, uint64_t dst) {
                    if (depths[dst] == std::numeric_limits<uint64_t>::max()) {
                        depths[dst] = cur_depth;
                        return true;
                    }
                    return false;
                },
                [&](uint32_t task_index, uint64_t src, uint64_t dst) {
                    std::atomic_ref<uint64_t> ref{depths[dst]};
                    uint64_t expected = std::numeric_limits<uint64_t>::max();
                    return ref.compare_exchange_strong(expected, cur_depth, std::memory_order_relaxed);
                }),
            gollum::make_edge_functor(
                [&](uint32_t task_index, uint64_t src, uint64_t dst, gollum::VertexSubset &nxt) {
                    nxt.unsafe().add_vertex(dst);
                },
                [&](uint32_t task_index, uint64_t src, uint64_t dst, gollum::VertexSubset &nxt) {
                    nxt.add_vertex(task_index, dst);
                })
        );
        nxt_frontier->unsafe().flush_buffers();
        std::swap(cur_frontier, nxt_frontier);
        nxt_frontier->unsafe().reset();
        ++cur_depth;
    }
}