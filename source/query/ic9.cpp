#include "query/ic9.hpp"

#include "graph_re.hpp"
#include "utils.hpp"

#include "gollum/path_expand.hpp"
#include <oneapi/tbb/global_control.h>

IC9Query::IC9Query()
    : Query(1) {}

const char *IC9Query::name() const noexcept {
    return "interactive-complex-9";
}

bool IC9Query::is_parallel() const noexcept {
    return false;
}

size_t IC9Query::compute_temp_storage_size(Graph &g, const QueryArgs *args) const noexcept {
    return 0;
}

struct ResultComparator {
    using ValueType = std::tuple<uint64_t, VID, VID>; // creationDate, message.id, person.id
    
    bool operator()(const ValueType &lhs, const ValueType &rhs) const {
        if (std::get<0>(lhs) == std::get<0>(rhs)) {
            return std::get<1>(lhs) < std::get<1>(rhs);
        }
        return std::get<0>(lhs) > std::get<0>(rhs);
    }
};

using ResultQueue = TopKPriorityQueue<ResultComparator::ValueType, ResultComparator>;

static auto FilterMessageByCreationDate(GraphRe &g, VID person_vid, uint64_t max_date, ResultQueue &queue) {
    auto post_vids = g.get_incoming_edges(person_vid, "post");
    auto comment_vids = g.get_incoming_edges(person_vid, "comment");
    
    for (auto vid : post_vids) {
        auto creation_date_raw_str = g.get_vertex_prop(vid, "creationdate");
        uint64_t timestamp = date_str_to_timestamp(creation_date_raw_str);
        if (timestamp < max_date) {
            queue.push(ResultComparator::ValueType{timestamp, vid, person_vid});
        }
    }
    for (auto vid : comment_vids) {
        auto creation_date_raw_str = g.get_vertex_prop(vid, "creationdate");
        uint64_t timestamp = date_str_to_timestamp(creation_date_raw_str);
        if (timestamp < max_date) {
            queue.push(ResultComparator::ValueType{timestamp, vid, person_vid});
        }
    }
}

void IC9Query::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                       const QueryArgs &args, ResultTable *result_out) {
    constexpr size_t RESULT_LIMIT = 20;

    GraphRe graph{g};

    uint64_t person_id = args.get_u64(0);
    uint64_t max_date = args.get_u64(1);

    VID person_vid = graph.get_vid("person", person_id);
    auto vid_range = graph.get_vtype_vid_range("person");
    VID vid_start = vid_range.first, vid_end = vid_range.second;
    size_t num_persons = vid_end.value() - vid_start.value();

    auto to_zero_based_index = [&](const VID &v) {
        return v.value() - vid_start.value();
    };
    
    auto flags = std::make_unique_for_overwrite<bool []>(num_persons);
    std::fill_n(flags.get(), num_persons, false);
    flags[to_zero_based_index(person_vid)] = true;

    std::vector<VID> friend_vids;
    ResultQueue result_queue(RESULT_LIMIT);

    auto expand_friends = [&]<bool EmitVertex>(VID vid, std::bool_constant<EmitVertex>) {
        // generate 1-hop friends (out-neighbors)
        {
            auto out_friends = graph.get_outgoing_edges(vid, "person");
            for (auto friend_vid : out_friends) {
                size_t idx = to_zero_based_index(friend_vid);
                if (!flags[idx]) {
                    flags[idx] = true;
                    if constexpr (EmitVertex) {
                        friend_vids.push_back(friend_vid);
                    }
                    FilterMessageByCreationDate(graph, friend_vid, max_date, result_queue);
                }
            }
        }

        // generate 1-hop friends (in-neighbors)
        {
            auto in_friends = graph.get_incoming_edges(vid, "person");
            for (auto friend_vid : in_friends) {
                size_t idx = to_zero_based_index(friend_vid);
                if (!flags[idx]) {
                    flags[idx] = true;
                    if constexpr (EmitVertex) {
                        friend_vids.push_back(friend_vid);
                    }
                    FilterMessageByCreationDate(graph, friend_vid, max_date, result_queue);
                }
            }
        }
    };

    expand_friends(person_vid, std::true_type{});
    
    for (auto friend_vid : friend_vids) {
        expand_friends(friend_vid, std::false_type{});
    }

    size_t result_size = std::min(RESULT_LIMIT, result_queue.size());

    result_out->resize(result_size);
    auto out_other_person_id = result_out->get_u64(0);
    auto out_other_person_first_name = result_out->get_str(1);
    auto out_other_person_last_name = result_out->get_str(2);
    auto out_message_id = result_out->get_u64(3);
    auto out_message_content_or_image_file = result_out->get_str(4);
    auto out_message_creation_date = result_out->get_u64(5);

    for (size_t j = result_size; j > 0; --j) {
        size_t i = j - 1;
        auto [creation_date, message_vid, person_vid] = result_queue.top();
        result_queue.pop();
        
        out_other_person_id[i] = std::stoull(graph.get_vertex_prop(person_vid, "id"));
        out_other_person_first_name[i] = graph.get_vertex_prop(person_vid, "firstname");
        out_other_person_last_name[i] = graph.get_vertex_prop(person_vid, "lastname");

        out_message_id[i] = std::stoull(graph.get_vertex_prop(message_vid, "id"));

        {
            auto raw = graph.get_vertex_prop(message_vid, "content");
            if (raw.empty()) {
                raw = graph.get_vertex_prop(message_vid, "imagefile");
            }
            out_message_content_or_image_file[i] = raw;
        }
        out_message_creation_date[i] = creation_date;
    }
}


















IC9ParallelQuery::IC9ParallelQuery()
    : Query() {}

const char *IC9ParallelQuery::name() const noexcept { return "interactive-complex-9-parallel"; }
bool IC9ParallelQuery::is_parallel() const noexcept { return true; }

size_t IC9ParallelQuery::compute_temp_storage_size(Graph &g, const QueryArgs *args) const noexcept {
    GraphRe graph{g};
    size_t num_vertices = graph.count_vtype("person");
    return (sizeof(bool) * num_vertices + sizeof(uint64_t) * num_vertices) * 2;
}

struct ConcurrentResultComparator {
    using ValueType = std::tuple<uint64_t, VID, VID>; // creationDate, message.id, person.id
    
    bool operator()(const ValueType &lhs, const ValueType &rhs) const {
        if (std::get<0>(lhs) == std::get<0>(rhs)) {
            return std::get<1>(lhs) > std::get<1>(rhs);
        }
        return std::get<0>(lhs) < std::get<0>(rhs);
    }
};

using ConcurrentResultQueue = oneapi::tbb::concurrent_priority_queue<ResultComparator::ValueType, ConcurrentResultComparator>;

#if 0
static auto ConcurrentFilterMessageByCreationDate(GraphRe &g, VID person_vid, uint64_t max_date, ConcurrentResultQueue &queue) {
    auto post_vids = g.get_incoming_edges(person_vid, "post");
    auto comment_vids = g.get_incoming_edges(person_vid, "comment");
    
    for (auto vid : post_vids) {
        auto creation_date_raw_str = g.get_vertex_prop(vid, "creationdate");
        uint64_t timestamp = date_str_to_timestamp(creation_date_raw_str);
        if (timestamp < max_date) {
            queue.push(ResultComparator::ValueType{timestamp, vid, person_vid});
        }
    }
    for (auto vid : comment_vids) {
        auto creation_date_raw_str = g.get_vertex_prop(vid, "creationdate");
        uint64_t timestamp = date_str_to_timestamp(creation_date_raw_str);
        if (timestamp < max_date) {
            queue.push(ResultComparator::ValueType{timestamp, vid, person_vid});
        }
    }
}
#endif

static auto ConcurrentFilterMessageByCreationDate(GraphRe &g, VID person_vid, uint64_t max_date, ConcurrentResultQueue &queue) {
    auto post_vids = g.get_incoming_edges(person_vid, "post");
    auto comment_vids = g.get_incoming_edges(person_vid, "comment");
    
    oneapi::tbb::parallel_for(oneapi::tbb::blocked_range<size_t>(0, post_vids.size()),
        [&](const oneapi::tbb::blocked_range<size_t> range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                VID vid = post_vids[i];
                auto creation_date_raw_str = g.get_vertex_prop(vid, "creationdate");
                uint64_t timestamp = date_str_to_timestamp(creation_date_raw_str);
                if (timestamp < max_date) {
                    queue.push(ResultComparator::ValueType{timestamp, vid, person_vid});
                }
            }
    });

    oneapi::tbb::parallel_for(oneapi::tbb::blocked_range<size_t>(0, comment_vids.size()),
        [&](const oneapi::tbb::blocked_range<size_t> range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                VID vid = comment_vids[i];
                auto creation_date_raw_str = g.get_vertex_prop(vid, "creationdate");
                uint64_t timestamp = date_str_to_timestamp(creation_date_raw_str);
                if (timestamp < max_date) {
                    queue.push(ResultComparator::ValueType{timestamp, vid, person_vid});
                }
            }
    });
}

void IC9ParallelQuery::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                               const QueryArgs &args, ResultTable *result_out) {
    execute_v2(parent_benchmark, g, temp_storage, temp_storage_size, args, result_out);
}

void IC9ParallelQuery::execute_v2(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                               const QueryArgs &args, ResultTable *result_out) {
    //oneapi::tbb::global_control global_limit(oneapi::tbb::global_control::max_allowed_parallelism, 4);

    constexpr size_t RESULT_LIMIT = 20;

    GraphRe graph{g};
    ConcurrentResultQueue result_queue;

    uint64_t person_id = args.get_u64(0);
    uint64_t max_date = args.get_u64(1);

    VID person_vid = graph.get_vid("person", person_id);
    auto vid_range = graph.get_vtype_vid_range("person");
    VID vid_start = vid_range.first, vid_end = vid_range.second;
    size_t num_persons = vid_end.value() - vid_start.value();

    auto to_zero_based_index = [&](const VID &v) {
        return v.value() - vid_start.value();
    };
    
    std::cout << std::format("num_persons: {}", num_persons) << "\n";

    auto flags = std::make_unique_for_overwrite<bool []>(num_persons);
    std::fill_n(flags.get(), num_persons, false);
    flags[to_zero_based_index(person_vid)] = true;

    std::vector<VID> friend_vids;
    
    auto collect_friends = [&](VID vid) {
        auto out_friends = graph.get_outgoing_edges(vid, "person");
        for (auto friend_vid : out_friends) {
            size_t idx = to_zero_based_index(friend_vid);
            if (!flags[idx]) {
                flags[idx] = true;
                friend_vids.push_back(friend_vid);
            }
        }

        auto in_friends = graph.get_incoming_edges(vid, "person");
        for (auto friend_vid : in_friends) {
            size_t idx = to_zero_based_index(friend_vid);
            if (!flags[idx]) {
                flags[idx] = true;
                friend_vids.push_back(friend_vid);
            }
        }
    };

    collect_friends(person_vid);
    std::cout << std::format("collect 1-hop friends: {}\n", friend_vids.size());

    size_t num_hop1_friends = friend_vids.size();
    for (size_t i = 0; i < num_hop1_friends; ++i) {
        collect_friends(friend_vids[i]);
    }

    std::cout << std::format("collect 2-hops friends: {}\n", friend_vids.size());

    oneapi::tbb::parallel_for(oneapi::tbb::blocked_range<size_t>{0, friend_vids.size()},
        [&](const oneapi::tbb::blocked_range<size_t> &range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                ConcurrentFilterMessageByCreationDate(graph, friend_vids[i], max_date, result_queue);
            }
        });
    
    std::cout << "finished\n";

    size_t result_size = std::min(RESULT_LIMIT, result_queue.size());

    result_out->resize(result_size);
    auto out_other_person_id = result_out->get_u64(0);
    auto out_other_person_first_name = result_out->get_str(1);
    auto out_other_person_last_name = result_out->get_str(2);
    auto out_message_id = result_out->get_u64(3);
    auto out_message_content_or_image_file = result_out->get_str(4);
    auto out_message_creation_date = result_out->get_u64(5);

    for (size_t j = result_size; j > 0; --j) {
        size_t i = j - 1;

        ResultComparator::ValueType tup;
        if (!result_queue.try_pop(tup)) {
            std::cout << "failed to pop\n";
        }
        auto creation_date = std::get<0>(tup);
        auto message_vid = std::get<1>(tup);
        auto person_vid = std::get<2>(tup);
        
        out_other_person_id[i] = std::stoull(graph.get_vertex_prop(person_vid, "id"));
        out_other_person_first_name[i] = graph.get_vertex_prop(person_vid, "firstname");
        out_other_person_last_name[i] = graph.get_vertex_prop(person_vid, "lastname");
        out_message_id[i] = std::stoull(graph.get_vertex_prop(message_vid, "id"));

        {
            auto raw = graph.get_vertex_prop(message_vid, "content");
            if (raw.empty()) {
                raw = graph.get_vertex_prop(message_vid, "imagefile");
            }
            out_message_content_or_image_file[i] = raw;
        }
        out_message_creation_date[i] = creation_date;
    }
}

void IC9ParallelQuery::execute_v1(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                               const QueryArgs &args, ResultTable *result_out) {
    constexpr size_t RESULT_LIMIT = 20;

    GraphRe graph{g};

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
    
    const size_t num_vertices = graph.count_vtype("person");
    auto frontier1_shared_buffer_mem = reinterpret_uint64_buffer(0, num_vertices);
    auto frontier2_shared_buffer_mem = reinterpret_uint64_buffer(num_vertices * sizeof(uint64_t), num_vertices);
    auto frontier1_flags_mem = reinterpret_bool_buffer(num_vertices * sizeof(uint64_t) * 2, num_vertices);
    auto frontier2_flags_mem = reinterpret_bool_buffer(num_vertices * sizeof(uint64_t) * 2 + num_vertices * sizeof(bool), num_vertices);

    gollum::VertexSubset frontier1{*graph.get_adjlist(), max_parallelism(), frontier1_shared_buffer_mem, frontier1_flags_mem};
    gollum::VertexSubset frontier2{*graph.get_adjlist(), max_parallelism(), frontier2_shared_buffer_mem, frontier2_flags_mem};
    gollum::VertexSubset *cur_frontier = std::addressof(frontier1);
    gollum::VertexSubset *nxt_frontier = std::addressof(frontier2);

    uint64_t person_id = args.get_u64(0);
    uint64_t max_date = args.get_u64(1);

    VID person_vid = graph.get_vid("person", person_id);
    auto vid_range = graph.get_vtype_vid_range("person");
    VID vid_start = vid_range.first, vid_end = vid_range.second;
    size_t num_persons = vid_end.value() - vid_start.value();

    auto to_zero_based_index = [&](const VID &v) {
        return v.value() - vid_start.value();
    };
    
    std::cout << std::format("num_persons: {}", num_persons) << "\n";

    auto flags = std::make_unique_for_overwrite<bool []>(num_persons);
    std::fill_n(flags.get(), num_persons, false);
    flags[to_zero_based_index(person_vid)] = true;

    std::vector<VID> friend_vids;
    ConcurrentResultQueue result_queue;

    cur_frontier->unsafe().add_vertex(person_vid.value());
    std::cout << "add first\n";

    auto expand_friends = [&]<bool EmitVertex>(VID vid, std::bool_constant<EmitVertex>, gollum::PathExpandHint hint) {
        gollum::path_expand_forward(max_parallelism(), g, "person", *cur_frontier, *nxt_frontier,
            gollum::make_edge_filter(
                [&](uint32_t task_index, uint64_t src, uint64_t dst) {
                    if (dst == person_vid.value()) { return false; }

                    if (!flags[to_zero_based_index(VID{dst})]) {
                        flags[to_zero_based_index(VID{dst})] = true;
                        return true;
                    }
                    return false;
                },
                [&](uint32_t task_index, uint64_t src, uint64_t dst) {
                    if (dst == person_vid.value()) { return false; }

                    std::atomic_ref<bool> ref{flags[to_zero_based_index(VID{dst})]};
                    bool expected = false;
                    return ref.compare_exchange_strong(expected, true, std::memory_order_relaxed);
                }
            ),
            gollum::make_edge_functor(
                [&](uint32_t task_index, uint64_t src, uint64_t dst, gollum::VertexSubset &nxt) {
                    ConcurrentFilterMessageByCreationDate(graph, VID{dst}, max_date, result_queue);
                    if constexpr (EmitVertex) {
                        nxt.unsafe().add_vertex(dst);
                    }
                },
                [&](uint32_t task_index, uint64_t src, uint64_t dst, gollum::VertexSubset &nxt) {
                    ConcurrentFilterMessageByCreationDate(graph, VID{dst}, max_date, result_queue);
                    if constexpr (EmitVertex) {
                        nxt.add_vertex(task_index, dst);
                    }
                }
            ), hint);

        gollum::path_expand_backward(max_parallelism(), g, "person", *cur_frontier, *nxt_frontier,
            gollum::make_edge_filter(
                [&](uint32_t task_index, uint64_t src, uint64_t dst) {
                    if (dst == person_vid.value()) { return false; }

                    if (!flags[to_zero_based_index(VID{dst})]) {
                        flags[to_zero_based_index(VID{dst})] = true;
                        return true;
                    }
                    return false;
                },
                [&](uint32_t task_index, uint64_t src, uint64_t dst) {
                    if (dst == person_vid.value()) { return false; }

                    std::atomic_ref<bool> ref{flags[to_zero_based_index(VID{dst})]};
                    bool expected = false;
                    return ref.compare_exchange_strong(expected, true, std::memory_order_relaxed);
                }
            ),
            gollum::make_edge_functor(
                [&](uint32_t task_index, uint64_t src, uint64_t dst, gollum::VertexSubset &nxt) {
                    ConcurrentFilterMessageByCreationDate(graph, VID{dst}, max_date, result_queue);
                    if constexpr (EmitVertex) {
                        nxt.unsafe().add_vertex(dst);
                    }
                },
                [&](uint32_t task_index, uint64_t src, uint64_t dst, gollum::VertexSubset &nxt) {
                    ConcurrentFilterMessageByCreationDate(graph, VID{dst}, max_date, result_queue);
                    if constexpr (EmitVertex) {
                        nxt.add_vertex(task_index, dst);
                    }
                }
            ), hint);
    };
    
    std::cout << "expand first\n";

    expand_friends(person_vid, std::true_type{}, gollum::PathExpandHint::kDefault);
    
    std::cout << "flush first\n";

    nxt_frontier->unsafe().flush_buffers();
    std::cout << std::format("vertex_set size: {}\n", nxt_frontier->unsafe().size());

    std::swap(cur_frontier, nxt_frontier);
    nxt_frontier->unsafe().reset();
    
    std::cout << "expand second\n";
    expand_friends(person_vid, std::true_type{}, gollum::PathExpandHint::kDefault);

    size_t result_size = std::min(RESULT_LIMIT, result_queue.size());

    result_out->resize(result_size);
    auto out_other_person_id = result_out->get_u64(0);
    auto out_other_person_first_name = result_out->get_str(1);
    auto out_other_person_last_name = result_out->get_str(2);
    auto out_message_id = result_out->get_u64(3);
    auto out_message_content_or_image_file = result_out->get_str(4);
    auto out_message_creation_date = result_out->get_u64(5);

    for (size_t j = result_size; j > 0; --j) {
        size_t i = j - 1;

        ResultComparator::ValueType tup;
        if (!result_queue.try_pop(tup)) {
            std::cout << "failed to pop\n";
        }
        auto creation_date = std::get<0>(tup);
        auto message_vid = std::get<1>(tup);
        auto person_vid = std::get<2>(tup);
        
        out_other_person_id[i] = std::stoull(graph.get_vertex_prop(person_vid, "id"));
        out_other_person_first_name[i] = graph.get_vertex_prop(person_vid, "firstname");
        out_other_person_last_name[i] = graph.get_vertex_prop(person_vid, "lastname");
        out_message_id[i] = std::stoull(graph.get_vertex_prop(message_vid, "id"));

        {
            auto raw = graph.get_vertex_prop(message_vid, "content");
            if (raw.empty()) {
                raw = graph.get_vertex_prop(message_vid, "imagefile");
            }
            out_message_content_or_image_file[i] = raw;
        }
        out_message_creation_date[i] = creation_date;
    }
}