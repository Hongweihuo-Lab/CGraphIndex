#include "benchmark/bi19_benchmark.hpp"

#include <filesystem>

#include "graph_re.hpp"
#include "utils.hpp"
#include "par/par.hpp"
#include "gollum/graph_project.hpp"
#include "gollum/shortest_path.hpp"

void BI19Benchmark::prepare(Graph &g) {
    GraphRe graph{g};
    
    auto count_interactions = [&](VID person_a, VID person_b) {
        auto person_a_posts = graph.get_incoming_edges(person_a, "post");
        auto person_a_comments = graph.get_incoming_edges(person_a, "comment");

        auto person_b_posts = graph.get_incoming_edges(person_b, "post");
        auto person_b_comments = graph.get_incoming_edges(person_b, "comment");

        auto count_reply_of = [&](const std::vector<VID> &comment_vids, const std::vector<VID> &ref_post_vids,
                                 const std::vector<VID> &ref_comment_vids) {
            auto find_reply_of = [&graph](VID msg_vid, const std::string &msg_type, const std::vector<VID> &ref) {
                auto reply_of_vids = graph.get_outgoing_edges(msg_vid, msg_type);
                
                for (VID vid : reply_of_vids) {
                    auto iter = std::lower_bound(ref.begin(), ref.end(), vid);
                    if (iter != ref.end() && *iter == vid) {
                        return true;
                    }
                }
                return false;
            };

            size_t count = 0;
            for (VID comment_vid : comment_vids) {
                if (find_reply_of(comment_vid, "post", ref_post_vids)) {
                    ++count;
                }
                if (find_reply_of(comment_vid, "comment", ref_comment_vids)) {
                    ++count;
                }
            }
            return count;
        };

        size_t count1 = count_reply_of(person_a_comments, person_b_posts, person_b_comments);
        size_t count2 = count_reply_of(person_b_comments, person_a_posts, person_a_comments);
        return count1 + count2;
    };

    std::string precomputed_graph_filename = get_tmp_dir() + "/bi19_precomputed.graph";
    if (std::filesystem::exists(precomputed_graph_filename)) {
        std::ifstream in{precomputed_graph_filename};
        precomputed_graph_ = gollum::ProjectedGraph::deserialize(in);
    } else {
        precomputed_graph_ = [&]() {
            gollum::ConcurrentProjectedGraphBuilder<true> graph_builder{gollum::ProjectedGraphDirection::kOut};
            auto person_vid_range = graph.get_vtype_vid_range("person");
            std::cout << person_vid_range.second.value() - person_vid_range.first.value() << "\n";

            oneapi::tbb::parallel_for(
                oneapi::tbb::blocked_range<uint64_t>(person_vid_range.first.value(), person_vid_range.second.value()),
                [&](oneapi::tbb::blocked_range<uint64_t> range) {
                    for (uint64_t _vid = range.begin(); _vid < range.end(); ++_vid) {
                        VID vid{_vid};

                        auto friend_vids = graph.get_outgoing_edges(vid, "person");
                        for (VID friend_vid : friend_vids) {
                            size_t num_interactions = count_interactions(vid, friend_vid);
                            float weight = roundf(40.0f - sqrtf(static_cast<float>(num_interactions)));
                            if (weight < 1.0f) {
                                weight = 1.0f;
                            }
                            graph_builder.add_edge(vid, friend_vid, weight);
                        }
                    }
                });
            return std::move(graph_builder).build();
        }();
        std::ofstream out{precomputed_graph_filename};
        precomputed_graph_.serialize(out);
    }

    std::cerr << "prepared\n";
}