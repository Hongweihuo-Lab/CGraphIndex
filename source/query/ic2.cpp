#include "query/ic2.hpp"

#include <format>

#include "graph_re.hpp"
#include "utils.hpp"

#include "oneapi/tbb/parallel_for.h"
#include "oneapi/tbb/concurrent_priority_queue.h"

IC2Query::IC2Query()
    : Query(1) {}

const char *IC2Query::name() const noexcept {
    return "interactive-complex-2";
}

bool IC2Query::is_parallel() const noexcept {
    return false;
}

size_t IC2Query::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept {
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

struct ConcurrentResultComparator {
    using ValueType = std::tuple<uint64_t, VID, VID>; // creationDate, message.id, person.id
    
    bool operator()(const ValueType &lhs, const ValueType &rhs) const {
        if (std::get<0>(lhs) == std::get<0>(rhs)) {
            return std::get<1>(lhs) > std::get<1>(rhs);
        }
        return std::get<0>(lhs) < std::get<0>(rhs);
    }
};

using ResultQueue = TopKPriorityQueue<ResultComparator::ValueType, ResultComparator>;
using ConcurrentResultQueue = oneapi::tbb::concurrent_priority_queue<ConcurrentResultComparator::ValueType, ConcurrentResultComparator>;

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

void IC2Query::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                       const QueryArgs &args, ResultTable *result_out) {
    execute_v2(parent_benchmark, g, temp_storage, temp_storage_size, args, result_out);
}

void IC2Query::execute_v2(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
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
    
    std::vector<bool> flags(num_persons, false);
    flags[to_zero_based_index(person_vid)] = true;

    std::vector<VID> friend_vids;

    auto out_friends = graph.get_outgoing_edges(person_vid, "person");
    for (auto friend_vid : out_friends) {
        size_t idx = to_zero_based_index(friend_vid);
        if (!flags[idx]) {
            flags[idx] = true;
            friend_vids.push_back(friend_vid);
        }
    }

    // generate 1-hop friends (in-neighbors)
    auto in_friends = graph.get_incoming_edges(person_vid, "person");
    for (auto friend_vid : in_friends) {
        size_t idx = to_zero_based_index(friend_vid);
        if (!flags[idx]) {
            flags[idx] = true;
            friend_vids.push_back(friend_vid);
        }
    }

    std::cout << std::format("num_friends: {}\n", friend_vids.size());

    ConcurrentResultQueue result_queue;

    oneapi::tbb::parallel_for(oneapi::tbb::blocked_range<size_t>(0, friend_vids.size()),
        [&](const oneapi::tbb::blocked_range<size_t> range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                VID vid = friend_vids[i];
                ConcurrentFilterMessageByCreationDate(graph, vid, max_date, result_queue);
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
        ConcurrentResultComparator::ValueType tup;
        result_queue.try_pop(tup);
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

void IC2Query::execute_v1(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
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
    
    std::vector<bool> flags(num_persons, false);
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