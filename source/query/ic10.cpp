#include "query/ic10.hpp"

#include "graph_re.hpp"
#include "utils.hpp"

IC10Query::IC10Query()
    : Query(1) {}

const char *IC10Query::name() const noexcept {
    return "interactive-complex-10";
}

bool IC10Query::is_parallel() const noexcept {
    return false;
}

size_t IC10Query::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept {
    return 0;
}

static std::vector<VID> GetFriends(GraphRe &graph, VID start, uint64_t month) {
    auto vid_range = graph.get_vtype_vid_range("person");
    VID vid_start = vid_range.first, vid_end = vid_range.second;

    size_t num_persons = vid_end.value() - vid_start.value();

    auto to_zero_based_index = [&](const VID &vid) {
        return vid.value() - vid_start.value();
    };

    std::vector<bool> flags(num_persons, false);
    flags[to_zero_based_index(start)] = true;

    std::vector<VID> friends_1hop;

    {
        auto out_friends = graph.get_outgoing_edges(start, "person");
        for (auto friend_vid : out_friends) {
            size_t idx = to_zero_based_index(friend_vid);
            if (!flags[idx]) {
                flags[idx] = true;
                friends_1hop.push_back(friend_vid);
            }
        }
    }

    {
        auto in_friends = graph.get_incoming_edges(start, "person");
        for (auto friend_vid : in_friends) {
            size_t idx = to_zero_based_index(friend_vid);
            if (!flags[idx]) {
                flags[idx] = true;
                friends_1hop.push_back(friend_vid);
            }
        }
    }

    std::vector<VID> friends;
    for (auto friend_vid : friends_1hop) {
        auto out_friends = graph.get_outgoing_edges(friend_vid, "person");
        for (auto vid : out_friends) {
            size_t idx = to_zero_based_index(vid);
            if (!flags[idx]) {
                flags[idx] = true;
                auto birthday_prop = graph.get_vertex_prop(vid, "birthday");

                // 2020-01-03
                uint32_t m = (birthday_prop[5] - '0') * 10 + (birthday_prop[6] - '0');
                uint32_t d = (birthday_prop[8] - '0') * 10 + (birthday_prop[9] - '0');
                if ((m == month && d >= 21) || (m == month + 1 && d < 22)) {
                    friends.push_back(vid);
                }
            }
        }

        auto in_friends = graph.get_incoming_edges(friend_vid, "person");
        for (auto vid : in_friends) {
            size_t idx = to_zero_based_index(vid);
            if (!flags[idx]) {
                flags[idx] = true;
                auto birthday_prop = graph.get_vertex_prop(vid, "birthday");
                // 20200103
                uint32_t m = (birthday_prop[5] - '0') * 10 + (birthday_prop[6] - '0');
                uint32_t d = (birthday_prop[8] - '0') * 10 + (birthday_prop[9] - '0');
                if ((m == month && d >= 21) || (m == month + 1 && d < 22)) {
                    friends.push_back(vid);
                }
            }
        }
    }
    return friends;
}

struct ResultComparator {
    using ValueType = std::tuple<uint64_t, std::string, std::string, int32_t, std::string, std::string>;
    
    bool operator()(const ValueType &lhs, const ValueType &rhs) const {
        if (std::get<3>(lhs) == std::get<3>(rhs)) {
            return std::get<0>(lhs) < std::get<0>(rhs);
        }
        return std::get<3>(lhs) > std::get<3>(rhs);
    }
};

void IC10Query::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                        const QueryArgs &args, ResultTable *result_out) {
    const size_t MAX_LIMIT = 10;

    GraphRe graph{g};
    uint64_t person_id = args.get_u64(0);
    uint64_t month = args.get_u64(1);

    VID person_vid = graph.get_vid("person", person_id);
    auto friends = GetFriends(graph, person_vid, month);

    auto interest_tags = graph.get_outgoing_edges(person_vid, "tag");
    auto interest_tag_range = graph.get_vtype_vid_range("tag");
    size_t num_interest_tags = interest_tag_range.second.value() - interest_tag_range.first.value();

    std::vector<bool> interest_tag_flags(num_interest_tags, false);
    for (auto vid : interest_tags) {
        interest_tag_flags[vid.value() - interest_tag_range.first.value()] = true;
    }

    TopKPriorityQueue<ResultComparator::ValueType, ResultComparator> pq(MAX_LIMIT);

    for (auto friend_vid : friends) {
        int32_t common = 0, uncommon = 0;
        auto posts = graph.get_incoming_edges(friend_vid, "post");

        for (auto post_vid : posts) {
            auto post_tags = graph.get_outgoing_edges(post_vid, "tag");
            
            bool found = false;
            for (auto tag_vid : post_tags) {
                size_t idx = tag_vid.value() - interest_tag_range.first.value();
                if (interest_tag_flags[idx]) {
                    found = true;
                    break;
                }
            }
            if (found) { ++common; } else { ++uncommon; }
        }

        auto id = std::stoull(graph.get_vertex_prop(friend_vid, "id"));
        auto first_name = graph.get_vertex_prop(friend_vid, "firstname");        
        auto last_name = graph.get_vertex_prop(friend_vid, "lastname");
        int32_t common_interest_score = common - uncommon;
        auto gender = graph.get_vertex_prop(friend_vid, "gender");

        auto city_vids = graph.get_outgoing_edges(friend_vid, "place");
        auto city = graph.get_vertex_prop(city_vids.front(), "name");

        auto val = ResultComparator::ValueType{id, first_name, last_name, common_interest_score, gender, city};
        pq.push(val);
    }

    result_out->resize(pq.size());
    auto id_col = result_out->get_u64(0);
    auto first_name_col = result_out->get_str(1);
    auto last_name_col = result_out->get_str(2);
    auto common_interest_score_col = result_out->get_i64(3);
    auto gender_col = result_out->get_str(4);
    auto city_col = result_out->get_str(5);

    for (size_t i = result_out->num_rows(); i > 0; --i) {
        const auto &[id, first_name, last_name, common_interest_score, gender, city] = pq.top();
        size_t j = i - 1;
        id_col[j] = id;
        first_name_col[j] = first_name;
        last_name_col[j] = last_name;
        common_interest_score_col[j] = common_interest_score;
        gender_col[j] = gender;
        city_col[j] = city;
        pq.pop();
    }
}