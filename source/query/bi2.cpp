#include "bi2.hpp"

#include <charconv>

#include "graph_re.hpp"
#include "utils.hpp"
#include "par/par.hpp"

#include <tbb/global_control.h>

BI2Query::BI2Query()
    : Query(1) {}

const char *BI2Query::name() const noexcept {
    return "business-intelligence-2";
}

bool BI2Query::is_parallel() const noexcept {
    return true;
}

size_t BI2Query::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept {
    return par::get_max_threads();
}

struct ResultComparator {
    using ValueType = std::tuple<uint32_t, std::string>;

    bool operator()(const ValueType &lhs, const ValueType &rhs) const {
        if (std::get<0>(lhs) == std::get<0>(rhs)) {
            return std::get<1>(lhs) < std::get<1>(rhs);
        }
        return std::get<0>(lhs) > std::get<0>(rhs);
    }
};

void BI2Query::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                       const QueryArgs &args, ResultTable *result_out) {
    const size_t RESULT_LIMIT = 100;

    GraphRe graph{g};

    uint64_t date = args.get_u64(0);
    std::string tagclass_name{args.get_str(1)};

    VID tagclass_vid = graph.get_vid_by_prop("tagclass", "name", tagclass_name);
    auto tag_vids = graph.get_incoming_edges(tagclass_vid, "tag");

    TopKPriorityQueue<ResultComparator::ValueType, ResultComparator> pq(RESULT_LIMIT);

    auto check_msg = [&](VID msg_vid) -> std::pair<uint32_t, uint32_t> {
        auto prop_raw = graph.get_vertex_prop(msg_vid, "creationdate");
        if (prop_raw.size() == 6) {
            std::cout << graph.get_vertex_props_raw(msg_vid) << '\n';
        }
        
        uint64_t creation_date = date_str_to_timestamp(prop_raw);

        uint32_t count_window1 = 0;
        uint32_t count_window2 = 0;

        if (creation_date >= date && creation_date < date + 8640000000) {
            count_window1 = 1;
        }
        if (creation_date >= date + 8640000000 && creation_date < date + 17280000000) {
            count_window2 = 1;
        }
        return {count_window1, count_window2};
    };

    for (auto tag_vid : tag_vids) {
        auto comment_vids = graph.get_incoming_edges(tag_vid, "comment");
        auto post_vids = graph.get_incoming_edges(tag_vid, "post");

        auto cnt1 = tbb::parallel_reduce(
            tbb::blocked_range<size_t>(0,  comment_vids.size()), 
            std::pair<uint32_t, uint32_t>{0, 0}, 
            [&](tbb::blocked_range<size_t> r, std::pair<uint32_t, uint32_t> x) {
                for (size_t i = r.begin(); i < r.end(); ++i) {
                    auto [c1, c2] = check_msg(comment_vids[i]);
                    x.first += c1;
                    x.second += c2;
                }
                return x;
            },
            [](const std::pair<uint32_t, uint32_t> &a, const std::pair<uint32_t, uint32_t> &b) {
                return std::pair{a.first + b.first, a.second + b.second};
            }
        );

        auto cnt2 = tbb::parallel_reduce(
            tbb::blocked_range<size_t>(0,  post_vids.size()), 
            std::pair<uint32_t, uint32_t>{0, 0}, 
            [&](tbb::blocked_range<size_t> r, std::pair<uint32_t, uint32_t> x) {
                for (size_t i = r.begin(); i < r.end(); ++i) {
                    auto [c1, c2] = check_msg(post_vids[i]);
                    x.first += c1;
                    x.second += c2;
                }
                return x;
            },
            [](const std::pair<uint32_t, uint32_t> &a, const std::pair<uint32_t, uint32_t> &b) {
                return std::pair{a.first + b.first, a.second + b.second};
            }
        );

        uint32_t count_window1 = cnt1.first + cnt2.first;
        uint32_t count_window2 = cnt1.second + cnt2.second;

        auto [min_window, max_window] = std::minmax(count_window1, count_window2);
        uint32_t diff = max_window - min_window;

        std::string tag_name = graph.get_vertex_prop(tag_vid, "name");
        pq.push(std::tuple{diff, tag_name});
    }

    result_out->resize(pq.size());
    auto diff_col = result_out->get_u64(0);
    auto tag_name_col = result_out->get_str(1);

    for (size_t i = result_out->num_rows(); i > 0; --i) {
        auto [diff, tag_name] = pq.top(); pq.pop();
        diff_col[i - 1] = diff;
        tag_name_col[i - 1] = tag_name;
    }
}