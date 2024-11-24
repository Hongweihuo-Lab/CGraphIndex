#include "bi12.hpp"

#include <charconv>

#include "graph_re.hpp"
#include "utils.hpp"
#include "par/par.hpp"

#include <tbb/global_control.h>

BI12Query::BI12Query()
    : Query(1) {}

const char *BI12Query::name() const noexcept {
    return "business-intelligence-12";
}

bool BI12Query::is_parallel() const noexcept {
    return true;
}

size_t BI12Query::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept {
    return par::get_max_threads();
}

static std::vector<VID> GetMessages(GraphRe &graph, uint64_t start_date, uint64_t length_threshold, 
                                    const std::vector<std::string> &languages) {
    std::unordered_set<std::string> langs{languages.begin(), languages.end()};

    auto find_comment_lang = [&](VID vid) {
        while (true) {
            auto post_vids = graph.get_outgoing_edges(vid, "post");
            if (!post_vids.empty()) {
                if (post_vids.size() != 1) {
                    throw std::runtime_error{"bad post"};
                }
                auto language_prop = graph.get_vertex_prop(post_vids.front(), "language");
                return langs.contains(language_prop);
            }

            auto comment_vids = graph.get_outgoing_edges(vid, "comment");
            if (comment_vids.empty()) {
                break;
            }
            if (comment_vids.size() != 1) {
                throw std::runtime_error{"bad comment"};
            }
            vid = comment_vids.front();
        }

        return false;
    };

    std::vector<std::vector<VID>> thread_local_vids(par::get_max_threads());

    auto fetch = [&]<bool IsPost>(VID start, VID end, std::bool_constant<IsPost>) {

        oneapi::tbb::parallel_for(oneapi::tbb::blocked_range<uint64_t>{start.value(), end.value()},
            [&](oneapi::tbb::blocked_range<uint64_t> range) {
                for (uint64_t i = range.begin(); i < range.end(); ++i) {
                    VID vid{i};
                    auto creation_date_prop = graph.get_vertex_prop(vid, "creationdate");
                    auto length_prop = graph.get_vertex_prop(vid, "length");

                    uint64_t creation_date = date_str_to_timestamp(creation_date_prop);
                    uint64_t length = std::stoull(length_prop);

                    if (length == 0 || length >= length_threshold || start_date >= creation_date)
                        continue;
                        
                    if constexpr (IsPost) {
                        auto language_prop = graph.get_vertex_prop(vid, "language");
                        if (!langs.contains(language_prop))
                            continue;
                    } else {
                        if (!find_comment_lang(vid))
                            continue;
                    }
                    thread_local_vids[oneapi::tbb::this_task_arena::current_thread_index()].push_back(vid);
                }
            });
    };

    auto comment_vid_range = graph.get_vtype_vid_range("comment");
    fetch(comment_vid_range.first, comment_vid_range.second, std::false_type{});

    auto post_vid_ranges = graph.get_vtype_vid_range("post");
    fetch(post_vid_ranges.first, post_vid_ranges.second, std::true_type{});

    std::vector<VID> res;
    size_t total = 0;
    for (size_t i = 0; i < thread_local_vids.size(); ++i) {
        total += thread_local_vids[i].size();
    }
    res.reserve(total);
    for (const auto &vids : thread_local_vids) {
        if (!vids.empty()) {
            res.insert(res.end(), vids.begin(), vids.end());
        }
    }

    return res;
}

void BI12Query::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                       const QueryArgs &args, ResultTable *result_out) {

    GraphRe graph{g};

    auto start_date = args.get_u64(0);
    auto length_threshold = args.get_u64(1);
    auto languages = args.get_str_list(2);

    auto message_vids = GetMessages(graph, start_date, length_threshold, languages);

    std::unordered_map<uint64_t, uint32_t> person_message_cnt;    

    for (VID message_vid : message_vids) {
        auto creator_vid = graph.get_outgoing_edges(message_vid, "person");
        if (!creator_vid.empty()) {
            if (creator_vid.size() != 1) {
                throw std::runtime_error{"bad creator"};
            }
            auto vid = creator_vid.front();
            ++person_message_cnt[vid.value()];
        }
    }

    std::unordered_map<uint32_t, uint32_t> message_cnt_with_person_cnt;
    for (const auto &[person_vid, message_cnt] : person_message_cnt) {
        ++message_cnt_with_person_cnt[message_cnt];
    }

    std::vector<std::pair<uint32_t, uint32_t>> message_cnt_with_person_cnt_pairs{
        message_cnt_with_person_cnt.begin(), message_cnt_with_person_cnt.end()};

    std::sort(message_cnt_with_person_cnt_pairs.begin(), message_cnt_with_person_cnt_pairs.end(), 
        [](const auto &lhs, const auto &rhs) -> bool {
            if (lhs.second == rhs.second) {
                return lhs.first > rhs.first;
            }
            return lhs.second > rhs.second;
        }
    );

    result_out->resize(message_cnt_with_person_cnt.size());
    auto message_cnt_col = result_out->get_u64(0);
    auto person_cnt_col = result_out->get_u64(1);

    for (size_t i = 0; i < message_cnt_with_person_cnt_pairs.size(); ++i) {
        message_cnt_col[i] = message_cnt_with_person_cnt_pairs[i].first;
        person_cnt_col[i] = message_cnt_with_person_cnt_pairs[i].second;
    }
}