#pragma once

#include "gollum/vertex_subset.hpp"
#include "gollum/graph_project.hpp"

namespace gollum {

//struct ShortestPathResult {
//    struct Single {
//        float            total_cost;
//        std::vector<VID> path;
//    };
//    struct All {
//        float                         total_cost;
//        std::vector<std::vector<VID>> paths;
//    };
//};

class ShortestPathRunner {
public:
    ShortestPathRunner(std::vector<VID> dsts) noexcept
        : dsts_(std::move(dsts)) { }

    ShortestPathRunner(VID dst) noexcept
        : dsts_(std::vector<VID>{dst}) { }

    virtual ~ShortestPathRunner() = default;

    [[nodiscard]] std::span<const VID> dsts() const noexcept { return dsts_; }
    virtual void set_dst(VID dst) noexcept { dsts_ = std::vector<VID>{dst}; }
    virtual void set_dsts(std::vector<VID> vids) { dsts_ = std::move(vids); }

    virtual std::vector<float> get_total_costs(VID src, const ProjectedGraph &graph) = 0;

private:
    std::vector<VID> dsts_;
};

class SerialDijkstraShortestPathRunner : public ShortestPathRunner {
    class DstSet {
    public:
        DstSet() = default;

        void reset(const std::vector<CSR::internal_id> &dst_ids) {
            existed_ = std::vector<bool>(dst_ids.size());
            for (size_t i = 0; i < dst_ids.size(); ++i) {
                existed_[dst_ids[i]] = true;
            }
        }

        bool contains(CSR::internal_id id) const {
            return existed_[id];
        }
        void remove(CSR::internal_id id) {
            existed_[id] = false;
        }

    private:
        std::vector<bool> existed_;
    };

public:
    SerialDijkstraShortestPathRunner(std::vector<VID> dsts) noexcept
        : ShortestPathRunner(std::move(dsts)) { }

    void set_dst(VID dst) noexcept override { 
        cached_internal_dsts_.clear();
        ShortestPathRunner::set_dst(dst);
    }

    void set_dsts(std::vector<VID> vids) override { 
        cached_internal_dsts_.clear();
        ShortestPathRunner::set_dsts(std::move(vids));
    }

    std::vector<float> get_total_costs(VID src, const ProjectedGraph &graph) override {
        if (!graph.has_outgoing()) {
            throw std::runtime_error{"the graph must has a outgoing adjlist"};
        }
        if (!graph.is_weighted()) {
            throw std::runtime_error{"the graph must be weighted"};
        }

        using PQPair = std::pair<CSR::internal_id, float>;
        struct PQCmp {
            bool operator()(const PQPair &lhs, const PQPair &rhs) const {
                return lhs.second > rhs.second;
            }
        };
    
        std::priority_queue<PQPair, std::vector<PQPair>, PQCmp> pq;
        std::vector<float> distances(graph.num_vertices(), std::numeric_limits<float>::infinity());

        auto src_internal_id = graph.map_outgoing_vid_to_internal_id(src);

        auto dst_ids = dsts();
        if (cached_internal_dsts_.empty()) {
            cached_internal_dsts_.resize(dst_ids.size());
            for (size_t i = 0; i < dst_ids.size(); ++i) {
                cached_internal_dsts_[i] = graph.map_outgoing_vid_to_internal_id(dst_ids[i]);
            }
        }
        std::span<const CSR::internal_id> dst_internal_ids = cached_internal_dsts_;
        std::vector<bool> dst_existed(graph.num_vertices());
        for (size_t i = 0; i < dst_internal_ids.size(); ++i) {
            dst_existed[dst_internal_ids[i]] = true;
        }
        size_t num_unchecked_dsts = dst_internal_ids.size();

        std::unordered_map<CSR::internal_id, float> result_distances;
        for (auto id : dst_internal_ids) {
            result_distances[id] = -1.0;
        }

        distances[src_internal_id] = 0;
        pq.push({src_internal_id, 0});

        while (!pq.empty()) {
            if (num_unchecked_dsts == 0) {
                break;
            }

            auto [u, dist] = pq.top();
            pq.pop();

            //if (u == dst_internal_id) {
            //    return dist;
            //}

            if (dst_existed[u]) {
                dst_existed[u] = false;
                result_distances[u] = dist;
                --num_unchecked_dsts;
                if (num_unchecked_dsts == 0) {
                    break;
                }
            }

            if (dist > distances[u]) {
                continue;
            }

            auto edges = graph.internal_get_outgoing_edges(u);
            auto weights = graph.internal_get_outgoing_weights(u);
            for (size_t i = 0; i < edges.size(); ++i) {
                auto v = edges[i];
                auto w = weights[i];
                auto new_dist = dist + w;
                if (new_dist < distances[v]) {
                    distances[v] = new_dist;
                    pq.push({v, new_dist});
                }
            }
        }

        std::vector<float> result(cached_internal_dsts_.size());
        for (size_t i = 0; i < cached_internal_dsts_.size(); ++i) {
            result[i] = result_distances[cached_internal_dsts_[i]];
        }
        return result;
    }

private:
    std::vector<CSR::internal_id> cached_internal_dsts_;

};

}