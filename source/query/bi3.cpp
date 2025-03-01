#include "bi3.hpp"

#include <charconv>

#include "graph_re.hpp"
#include "utils.hpp"
#include "par/par.hpp"

#include <tbb/global_control.h>

#include "oneapi/tbb/parallel_for_each.h"
#include "oneapi/tbb/concurrent_hash_map.h"
#include "oneapi/tbb/concurrent_vector.h"

#include "oneapi/tbb/concurrent_unordered_set.h"
#include "oneapi/tbb/concurrent_unordered_map.h"

BI3Query::BI3Query()
    : Query(1) {}

const char *BI3Query::name() const noexcept {
    return "business-intelligence-3";
}

bool BI3Query::is_parallel() const noexcept {
    return true;
}

size_t BI3Query::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept {
    return par::get_max_threads();
}

struct ResultComparator {
    using ValueType = std::tuple<uint64_t, uint64_t, VID, VID>;

    bool operator()(const ValueType &lhs, const ValueType &rhs) const {
        if (std::get<0>(lhs) == std::get<0>(rhs)) {
            return std::get<1>(lhs) < std::get<1>(rhs);
        }
        return std::get<0>(lhs) > std::get<0>(rhs);
    }
};

[[nodiscard]] static VID FindVertexByProp(GraphRe &graph, const std::string &vtype, 
                                          const std::string &prop_name, const std::string &prop_val) {
    auto [vid_start, vid_end] = graph.get_vtype_vid_range(vtype);
    for (uint64_t i = vid_start.value(); i < vid_end.value(); ++i) {
        std::string str = graph.get_vertex_prop(VID{i}, prop_name);
        if (str == prop_val) {
            return VID{i};
        }
    }
    throw std::runtime_error{"cannot find vertex"};
    return VID{};
}

void BI3Query::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
    const QueryArgs &args, ResultTable *result_out) {
    const size_t RESULT_LIMIT = 20;

    GraphRe graph{g};

    std::string_view tagclass_arg = args.get_str(0);
    std::string_view country_arg = args.get_str(1);

    VID tagclass_vid = FindVertexByProp(graph, "tagclass", "name", std::string{tagclass_arg});
    VID country_vid = FindVertexByProp(graph, "place", "name", std::string{country_arg});

    std::unordered_set<VID> tag_vids = [&]() {
        auto result = graph.get_incoming_edges(tagclass_vid, "tag");
        return std::unordered_set<VID>{result.begin(), result.end()};
    }();

    //for (const auto &x : tag_vids) {
    //    std::cout << x.value() << " ";
    //} std::cout << '\n';

    auto filter_post = [&](VID post_vid) {
        std::queue<VID> q;
        q.push(post_vid);

        while (!q.empty()) {
            VID vid = q.front();
            q.pop();

            {
                auto tags =  graph.get_outgoing_edges(post_vid, "tag");
                for (auto tag_vid : tags) {
                    if (tag_vids.contains(tag_vid)) {
                        return true;
                    }
                }
            }
            
            auto edges = graph.get_incoming_edges(vid, "comment");
            for (auto vid : edges) {
                q.push(vid);
            }
        }
        return false;
    };

    oneapi::tbb::concurrent_vector<std::tuple<uint64_t, uint64_t, VID, VID>> result_buf; // messageCount, forum.id, forum.VID, person.VID
    using ForumMessageCounter = oneapi::tbb::concurrent_hash_map<uint64_t, uint64_t>;
    ForumMessageCounter forum_message_counter;


    std::unordered_map<uint64_t, uint64_t> immutable_forum_cache;

    {
        oneapi::tbb::concurrent_hash_map<uint64_t, uint64_t> mutable_forum_cache;

        auto city_vids = graph.get_incoming_edges(country_vid, "place");
        //std::cout << std::format("city_vids_size: {}", city_vids.size()) << "\n";

        par::for_each(city_vids, [&](VID city_vid) {
            auto person_vids = graph.get_incoming_edges(city_vid, "person");
            //std::cout << std::format("person_vids_size: {}", person_vids.size()) << "\n";

            par::for_each(person_vids, [&](VID person_vid) {
                auto forum_vids = graph.get_incoming_edges(person_vid, "forum");
                

                par::for_each(forum_vids, [&](VID forum_vid) {
                    mutable_forum_cache.insert(std::pair<uint64_t, uint64_t>{forum_vid.value(), 0});
                    uint64_t forum_id = std::stoull(graph.get_vertex_prop(forum_vid, "id"));
                    result_buf.push_back(std::make_tuple(0, forum_id, forum_vid, person_vid));

#if 0
                    ForumMessageCounter::accessor accessor;
                    if (forum_message_counter.insert(accessor, std::pair<uint64_t, uint64_t>{forum_vid.value(), 0})) {
                        alignas(64) std::atomic<uint64_t> num_messages = 0;

                        auto post_vids = graph.get_outgoing_edges(forum_vid, "post");
    
                        par::for_each(post_vids, [&](VID post_vid) {
                            if (filter_post(post_vid)) {
                                num_messages.fetch_add(1, std::memory_order_relaxed);        
                            }
                        });
    
                        uint64_t final_num_messages = num_messages.load(std::memory_order_relaxed);
                        accessor->second = final_num_messages;
                        //printf("num_messages: %lu\n", final_num_messages);
    
                        if (final_num_messages > 0) {
                            
                        } else {
                            forum_message_counter.erase(forum_vid.value());
                        }
                    }


                    if (accessor->second != 0) {
                        uint64_t forum_id = std::stoull(graph.get_vertex_prop(forum_vid, "id"));
                        result_buf.push_back(std::make_tuple(accessor->second, forum_id, forum_vid, person_vid));
                    }
#endif
                });
            });
        });

        std::vector<std::pair<uint64_t, uint64_t>> tmp_forum_cache;
        tmp_forum_cache.resize(mutable_forum_cache.size());
        for (size_t i = 0; const auto &p : mutable_forum_cache) {
            tmp_forum_cache[i++] = {p.first, 0};
        }

        par::for_each(tmp_forum_cache, [&](std::pair<uint64_t, uint64_t> &p) {
            alignas(64) std::atomic<uint64_t> num_messages = 0;

            auto post_vids = graph.get_outgoing_edges(VID{p.first}, "post");

            par::for_each(post_vids, [&](VID post_vid) {
                if (filter_post(post_vid)) {
                    num_messages.fetch_add(1, std::memory_order_relaxed);        
                }
            });

            uint64_t final_num_messages = num_messages.load(std::memory_order_relaxed);
            p.second = final_num_messages;
        });

        for (const auto &p : tmp_forum_cache) {
            if (p.second == 0) {
                continue;
            }
            immutable_forum_cache.insert(p);
        }
    }

    for (auto &t : result_buf) {
        auto iter = immutable_forum_cache.find(std::get<2>(t).value());
        if (iter != immutable_forum_cache.end()) {
            std::get<0>(t) = iter->second;
        }
    }

    TopKPriorityQueue<ResultComparator::ValueType, ResultComparator> top_queue(RESULT_LIMIT);
    for (const auto &tup : result_buf) {
        if (std::get<0>(tup) == 0) {
            continue;
        }
        top_queue.push(tup);
    }

    
    size_t final_result_size = std::min(RESULT_LIMIT, top_queue.size());
    result_out->resize(final_result_size);
    auto col_forum_id = result_out->get_u64(0);
    auto col_forum_title = result_out->get_str(1);
    auto col_forum_creationDate = result_out->get_u64(2);
    auto col_person_id = result_out->get_u64(3);
    auto col_messageCount = result_out->get_u64(4);

    for (size_t _i = result_out->num_rows(); _i > 0; --_i) {
        auto [message_count, forum_id, forum_vid, person_vid] = top_queue.top(); top_queue.pop();
        size_t i = _i - 1;
        col_forum_id[i] = forum_id;
        col_forum_title[i] = graph.get_vertex_prop(forum_vid, "title");
        col_forum_creationDate[i] = std::stoull(graph.get_vertex_prop(forum_vid, "creationdate"));
        col_person_id[i] = std::stoull(graph.get_vertex_prop(person_vid, "id"));
        //col_forum_creationDate[i] = 0;
        //col_person_id[i] = 0;
        col_messageCount[i] = message_count;
    }
}

#if 0
void BI3Query::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                       const QueryArgs &args, ResultTable *result_out) {
    const size_t RESULT_LIMIT = 20;

    GraphRe graph{g};
    std::string_view tagclass_arg = args.get_str(0);

    std::string_view country_arg = args.get_str(1);

    VID tagclass_vid = FindVertexByProp(graph, "tagclass", "name", std::string{tagclass_arg});
    VID country_vid = FindVertexByProp(graph, "place", "name", std::string{country_arg});

    struct RecordList {
        oneapi::tbb::concurrent_vector<VID> person_vids;
        size_t                              num_messages = 0;
    };

    using ForumCollection = oneapi::tbb::concurrent_hash_map<VID, RecordList>;

    ForumCollection forum_collection;

    // find all forums satisfied provide condition and build hash table, key is Forum.id, it will be used as JOIN condition
    {
        auto city_vids = graph.get_incoming_edges(country_vid, "place");
        par::for_each(city_vids, [&](VID city_vid) {
            auto person_vids = graph.get_incoming_edges(city_vid, "person");
            par::for_each(person_vids, [&](VID person_vid) {
                auto forum_vids = graph.get_incoming_edges(person_vid, "forum");
                for (VID forum_vid : forum_vids) {
                    ForumCollection::accessor accessor;
                    forum_collection.insert(accessor, forum_vid);
                    accessor->second.person_vids.push_back(person_vid);
                }
            });
        });
    }

    auto get_single = [&](VID start, const std::string &neighbor_vtype, bool is_in) -> std::optional<VID> {
        auto vids = is_in ? graph.get_incoming_edges(start, neighbor_vtype) : graph.get_outgoing_edges(start, neighbor_vtype);
        if (vids.size() > 1) { throw std::runtime_error{"the size of single must be 1 or 0"}; }
        return vids.empty() ? std::nullopt : std::make_optional(vids.front());
    };

    auto filter_comment = [&](VID comment_vid) {
        std::optional<VID> post_vid;
        while ((post_vid = get_single(comment_vid, "post", false))) {
            auto forum_vid = get_single(*post_vid, "fourm", true);
            ForumCollection::accessor accessor;
            if (forum_collection.find(accessor, *forum_vid)) {
                ++accessor->second.num_messages;
            }
        }
    };

    {
        auto tag_vids = graph.get_incoming_edges(tagclass_vid, "tag");

        for (VID tag_vid : tag_vids) {
            auto post_vids = graph.get_incoming_edges(tag_vid, "post");
            par::for_each(post_vids, [&](VID post_vid) {
                auto forum_vid = get_single(post_vid, "fourm", true);

                ForumCollection::accessor accessor;
                if (forum_collection.find(accessor, *forum_vid)) {
                    ++accessor->second.num_messages;
                }
            });

            auto comment_vids = graph.get_incoming_edges(tag_vid, "comment");
            par::for_each(comment_vids, [&](VID comment_vid) {
                filter_comment(comment_vid);
            });
        }
    }


    result_out->resize(RESULT_LIMIT);
    
    TopKPriorityQueue<std::tuple<size_t, VID>, ResultComparator> pq(RESULT_LIMIT);
    for (auto iter = forum_collection.begin(); iter != forum_collection.end(); ++iter) {
        pq.push({iter->second.num_messages, iter->first});
    }
    
}

#endif