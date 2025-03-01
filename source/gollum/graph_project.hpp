#pragma once

#include <optional>
#include <span>
#include <ranges>
#include <fstream>

#include <oneapi/tbb/concurrent_unordered_map.h>
#include <oneapi/tbb/concurrent_vector.h>

#include "graph_re.hpp"

namespace gollum {

template<bool>
class ProjectedGraphBuilder;

template<bool>
class ConcurrentProjectedGraphBuilder;

class CSR {
public:
    template<bool> friend class ProjectedGraphBuilder;
    template<bool> friend class ConcurrentProjectedGraphBuilder;
public:
    using internal_id = uint64_t;
    static constexpr internal_id INVALID_INTERNAL_ID = -1;

public:
    CSR() = default;
    CSR(const CSR &) = delete;
    CSR(CSR &&) = default;

    CSR &operator=(const CSR &) = delete;
    CSR &operator=(CSR &&) = default;

    [[nodiscard]] bool is_weighted() const noexcept { return edge_weights_.has_value(); }

    [[nodiscard]] size_t num_vertices() const noexcept {
        return internal_id_to_vid_.size();
    }

    [[nodiscard]] size_t num_edges() const noexcept {
        return num_edges_;
    }

    [[nodiscard]] internal_id map_vid_to_internal_id(VID vid) const noexcept {
        auto iter = std::lower_bound(internal_id_to_vid_.begin(), internal_id_to_vid_.end(), vid);
        if (iter == internal_id_to_vid_.end()) {
            return INVALID_INTERNAL_ID;
        }
        return std::distance(internal_id_to_vid_.begin(), iter);
    }
    [[nodiscard]] VID map_internal_id_to_vid(internal_id internal_id) const noexcept {
        return internal_id_to_vid_[internal_id];
    }

    [[nodiscard]] size_t size_in_bytes() const noexcept {
        return offsets_.size() * sizeof(size_t) + edge_ids_.size() * sizeof(VID);
    }

    [[nodiscard]] std::span<const internal_id> internal_get_edges(internal_id src) const {
        return unchecked_get_edges(src);
    }
    [[nodiscard]] std::span<const float> internal_get_weights(internal_id src) const {
        return unchecked_get_weights(src);
    }

    //[[nodiscard]] auto get_edges(VID src) const {
    //    internal_id iid = map_vid_to_internal_id(src);
    //    return std::views::transform(unchecked_get_edges(iid), 
    //        [&](internal_id id) -> VID { return map_internal_id_to_vid(id); });        
    //}

    [[nodiscard]] std::span<const float> get_weights(VID src) const {
        internal_id iid = map_vid_to_internal_id(src);
        return unchecked_get_weights(iid);
    }

    [[nodiscard]] bool contains_vid(VID vid) const {
        return map_vid_to_internal_id(vid) != INVALID_INTERNAL_ID;
    }

    void serialize(std::ostream &out) const {
        auto write_vector = [&]<typename T>(const std::vector<T> &vec) {
            size_t num = vec.size();
            out.write((const char *)(&num), sizeof(num));
            out.write((const char *)(vec.data()), sizeof(T) * num);
        };

        out.write((const char *)(&num_edges_), sizeof(num_edges_));

        bool is_weighted = edge_weights_.has_value();
        out.write((const char *)(&is_weighted), sizeof(is_weighted));

        write_vector(internal_id_to_vid_);
        write_vector(offsets_);
        write_vector(edge_ids_);

        if (edge_weights_.has_value())
            write_vector(*edge_weights_);
    }
    
    static CSR deserialize(std::istream &in) {
        auto read_vector = [&]<typename T>(std::vector<T> &vec) {
            size_t num;
            in.read((char *)(&num), sizeof(num));
            vec.resize(num);
            in.read((char *)(vec.data()), sizeof(T) * num);
        };

        size_t num_edges;
        in.read((char *)(&num_edges), sizeof(num_edges));
        bool is_weighted;
        in.read((char *)(&is_weighted), sizeof(is_weighted));

        std::vector<VID> internal_id_to_vid;
        std::vector<size_t> offsets;
        std::vector<internal_id> edge_ids;
        std::optional<std::vector<float>> edge_weights;

        read_vector(internal_id_to_vid);
        read_vector(offsets);
        read_vector(edge_ids);

        if (is_weighted) {
            edge_weights.emplace();
            read_vector(*edge_weights);
        }

        if (is_weighted) {
            return CSR{num_edges, std::move(internal_id_to_vid),
                       std::move(offsets), std::move(edge_ids),
                       std::move(*edge_weights)};
        } else {
            return CSR{num_edges, std::move(internal_id_to_vid),
                       std::move(offsets), std::move(edge_ids)};
        }
    }

private:
    size_t                            num_edges_{0};
    std::vector<VID>                  internal_id_to_vid_;
    std::vector<size_t>               offsets_;
    std::vector<internal_id>          edge_ids_;
    std::optional<std::vector<float>> edge_weights_;
    
    CSR(size_t num_edges, std::vector<VID> vids,
        std::vector<size_t> offsets, std::vector<internal_id> edge_ids)
        : num_edges_(num_edges), internal_id_to_vid_(std::move(vids)),
          offsets_(std::move(offsets)), edge_ids_(std::move(edge_ids)),
          edge_weights_(std::nullopt) { }

    CSR(size_t num_edges, std::vector<VID> vids,
        std::vector<size_t> offsets, std::vector<internal_id> edge_ids,
        std::vector<float> weights)
        : num_edges_(num_edges), internal_id_to_vid_(std::move(vids)),
          offsets_(std::move(offsets)), edge_ids_(std::move(edge_ids)),
          edge_weights_(std::move(weights)) { }

    [[nodiscard]] std::span<const internal_id> unchecked_get_edges(internal_id iid) const {
        size_t start = offsets_[iid];
        size_t end = offsets_[iid + 1];
        return std::span<const internal_id>{edge_ids_.data() + start, edge_ids_.data() + end};
    }

    [[nodiscard]] std::span<const float> unchecked_get_weights(internal_id iid) const {
        size_t start = offsets_[iid];
        size_t end = offsets_[iid + 1];
        return std::span<const float>{edge_weights_->data() + start, edge_weights_->data() + end};
    }
};

enum class ProjectedGraphDirection {
    kIn, kOut, kBoth
};

class ProjectedGraph {
    template<bool> friend class ProjectedGraphBuilder;
    template<bool> friend class ConcurrentProjectedGraphBuilder;
public:
    ProjectedGraph() = default;
    ProjectedGraph(const ProjectedGraph &) = delete; 
    ProjectedGraph(ProjectedGraph &&) = default; 

    ProjectedGraph &operator=(const ProjectedGraph &) = delete;
    ProjectedGraph &operator=(ProjectedGraph &&) = default;

    bool is_weighted() const noexcept {
        auto dir = direction();
        if (dir == ProjectedGraphDirection::kIn) {
            return in_csr_->is_weighted();
        } else if (dir == ProjectedGraphDirection::kOut) {
            return out_csr_->is_weighted();
        }
        return in_csr_->is_weighted();
    }

    ProjectedGraphDirection direction() const noexcept {
        if (in_csr_.has_value() && out_csr_.has_value()) {
            return ProjectedGraphDirection::kBoth;
        } else if (in_csr_.has_value()) {
            return ProjectedGraphDirection::kIn;
        } else {
            return ProjectedGraphDirection::kOut;
        }
    }

    size_t num_vertices() const noexcept {
        return num_vertices_;
    }

    size_t indegrees() const noexcept {
        if (!in_csr_.has_value()) {
            return 0;
        }
        return in_csr_->num_edges();
    }

    size_t outdegrees() const noexcept {
        if (!out_csr_.has_value()) {
            return 0;
        }
        return out_csr_->num_edges();
    }

    bool has_incoming() const noexcept { return in_csr_.has_value(); }
    bool has_outgoing() const noexcept { return out_csr_.has_value(); }

    CSR::internal_id map_incoming_vid_to_internal_id(VID vid) const noexcept {
        assert(has_incoming());
        return in_csr_->map_vid_to_internal_id(vid);
    }

    CSR::internal_id map_outgoing_vid_to_internal_id(VID vid) const noexcept {
        assert(has_outgoing());
        return out_csr_->map_vid_to_internal_id(vid);
    }

    VID map_incoming_internal_id_to_vid(CSR::internal_id id) const noexcept {
        assert(has_incoming());
        return in_csr_->map_internal_id_to_vid(id);
    }
    VID map_outgoing_internal_id_to_vid(CSR::internal_id id) const noexcept {
        assert(has_outgoing());
        return out_csr_->map_internal_id_to_vid(id);
    }

    //auto get_incoming_edges(VID vid) const noexcept {
    //    assert(in_csr_.has_value());
    //    return in_csr_->get_edges(vid);
    //}
    //auto get_outgoing_edges(VID vid) const noexcept {
    //    assert(out_csr_.has_value());
    //    return out_csr_->get_edges(vid);
    //}

    std::span<const CSR::internal_id> internal_get_incoming_edges(CSR::internal_id id) const noexcept {
        assert(in_csr_.has_value());
        return in_csr_->internal_get_edges(id);
    }
    std::span<const CSR::internal_id> internal_get_outgoing_edges(CSR::internal_id id) const noexcept {
        assert(out_csr_.has_value());
        return out_csr_->internal_get_edges(id);
    }

    std::span<const float> internal_get_incoming_weights(CSR::internal_id id) const noexcept {
        assert(in_csr_.has_value());
        return in_csr_->internal_get_weights(id);
    }
    std::span<const float> internal_get_outgoing_weights(CSR::internal_id id) const noexcept {
        assert(out_csr_.has_value());
        return out_csr_->internal_get_weights(id);
    }

    std::span<const float> get_incoming_weights(VID vid) const noexcept {
        assert(in_csr_.has_value());
        return in_csr_->get_weights(vid);
    }

    std::span<const float> get_outgoing_weights(VID vid) const noexcept {
        assert(out_csr_.has_value());
        return out_csr_->get_weights(vid);
    }

    void serialize(std::ostream &out) const {
        out.write((const char *)(&num_vertices_), sizeof(num_vertices_));
        bool has_in = in_csr_.has_value();
        bool has_out = out_csr_.has_value();
        out.write((const char *)(&has_in), sizeof(has_in));
        out.write((const char *)(&has_out), sizeof(has_out));
        
        if (has_in) {
            in_csr_->serialize(out);
        }
        if (has_out) {
            out_csr_->serialize(out);
        }
    }

    static ProjectedGraph deserialize(std::istream &in) {
        size_t num_vertices;
        in.read((char *)(&num_vertices), sizeof(num_vertices));
        bool has_in = false;
        bool has_out = false;
        in.read((char *)(&has_in), sizeof(has_in));
        in.read((char *)(&has_out), sizeof(has_out));

        std::optional<CSR> in_csr;
        std::optional<CSR> out_csr;

        if (has_in) {
            in_csr.emplace(CSR::deserialize(in));
        }
        if (has_out) {
            out_csr.emplace(CSR::deserialize(in));
        }

        return ProjectedGraph{num_vertices, std::move(in_csr), std::move(out_csr)};
    }

private:
    size_t             num_vertices_;
    std::optional<CSR> in_csr_;
    std::optional<CSR> out_csr_;

    ProjectedGraph(size_t num_vertices, std::optional<CSR> in, std::optional<CSR> out)
        : num_vertices_(num_vertices), in_csr_(std::move(in)), out_csr_(std::move(out)) { 
        bool has_in = in_csr_.has_value();
        bool has_out = out_csr_.has_value();
        assert(has_in || has_out);
        if (has_in && has_out) {
            assert(in_csr_->is_weighted() == out_csr_->is_weighted());
        }
    }
};

template<bool Weighted>
class ConcurrentProjectedGraphBuilder {
    using StorageType = std::conditional_t<Weighted, 
        oneapi::tbb::concurrent_unordered_map<VID, oneapi::tbb::concurrent_vector<std::pair<VID, float>>>,
        oneapi::tbb::concurrent_unordered_map<VID, oneapi::tbb::concurrent_vector<VID>>>;
public:
    ConcurrentProjectedGraphBuilder(const ConcurrentProjectedGraphBuilder &) = delete;
    ConcurrentProjectedGraphBuilder(ConcurrentProjectedGraphBuilder &&) = default;
    ConcurrentProjectedGraphBuilder &operator=(const ConcurrentProjectedGraphBuilder &) = delete;
    ConcurrentProjectedGraphBuilder &operator=(ConcurrentProjectedGraphBuilder &&) = default;
    
    ConcurrentProjectedGraphBuilder(ProjectedGraphDirection dir) {
        if (dir == ProjectedGraphDirection::kBoth) {
            support_in_ = support_out_ = true;
        } else if (dir == ProjectedGraphDirection::kIn) {
            support_in_ = true;
            support_out_ = false;
        } else if (dir == ProjectedGraphDirection::kOut) {
            support_out_ = true;
            support_in_ = false;
        }
    }

    void add_edge(VID src, VID dst) requires(!Weighted) {
        if (support_out_) {
            auto iter = out_adjlist_.insert({src, {}}).first;
            out_adjlist_.insert({dst, {}});
            iter->second.push_back(dst);
        }

        if (support_in_) {
            auto iter = in_adjlist_.insert({dst, {}}).first;
            in_adjlist_.insert({src, {}});
            iter->second.push_back(src);
        }
    }

    void add_edge(VID src, VID dst, float weight) requires(Weighted) {
        if (support_out_) {
            auto iter = out_adjlist_.insert({src, {}}).first;
            out_adjlist_.insert({dst, {}});
            iter->second.push_back({dst, weight});
        }

        if (support_in_) {
            auto iter = in_adjlist_.insert({dst, {}}).first;
            in_adjlist_.insert({src, {}});
            iter->second.push_back({src, weight});
        }
    }

    ProjectedGraph build() && {
        auto compact_to_csr = [](StorageType &map) -> CSR {
            std::vector<size_t> offsets;
            std::vector<CSR::internal_id> edge_ids;
            std::vector<float> edge_weights;
            size_t num_edges = 0;
            
            std::vector<VID> all_vids;
            all_vids.reserve(map.size());
            for (const auto &[vid, list] : map) {
                all_vids.push_back(vid);
                num_edges += list.size();
            }
            std::sort(all_vids.begin(), all_vids.end());

            offsets.resize(all_vids.size() + 1);
            edge_ids.reserve(num_edges);

            if constexpr (Weighted) {
                edge_weights.reserve(num_edges);
            }

            auto transform_to_internal_id = [&](VID vid) -> CSR::internal_id {
                auto iter = std::lower_bound(all_vids.begin(), all_vids.end(), vid);
                return std::distance(all_vids.begin(), iter);
            };

            size_t offset = 0;
            for (size_t i = 0; i < all_vids.size(); ++i) {
                auto &list = map.at(all_vids[i]);
                offsets[i] = offset;
                offset += list.size();

                if constexpr (Weighted) {
                    std::sort(list.begin(), list.end(), [](const auto &lhs, const auto &rhs) {
                        return lhs.first < rhs.first; 
                    });
                    auto vids = std::views::common(
                        std::views::transform(std::views::keys(list), transform_to_internal_id));
                    auto weights = std::views::common(std::views::values(list));
                    edge_ids.insert(edge_ids.end(), vids.begin(), vids.end());
                    edge_weights.insert(edge_weights.end(), weights.begin(), weights.end());
                } else {
                    auto vids = std::views::common(
                        std::views::transform(list, transform_to_internal_id));
                    auto iter = edge_ids.insert(edge_ids.end(), vids.begin(), vids.end());
                    std::sort(iter, iter + list.size());
                }
            }
            offsets.back() = num_edges;

            if constexpr (Weighted) {
                return CSR{num_edges, std::move(all_vids), std::move(offsets), 
                           std::move(edge_ids), std::move(edge_weights)};
            } else {
                return CSR{num_edges, std::move(all_vids), std::move(offsets), std::move(edge_ids)};
            }
        };
        
        size_t num_vertices = 0;
        std::optional<CSR> in, out;
        if (support_in_) {
            in = compact_to_csr(in_adjlist_);
        }
        if (support_out_) {
            out = compact_to_csr(out_adjlist_);
        }

        if (support_in_ && support_out_) {
            assert(in->num_vertices() == out->num_vertices());
            num_vertices = in->num_vertices();
        } else if (support_in_) {
            num_vertices = in->num_vertices();
        } else {
            num_vertices = out->num_vertices();
        }

        return ProjectedGraph{num_vertices, std::move(in), std::move(out)};
    }

private:
    bool support_in_{false};
    bool support_out_{false};
    StorageType in_adjlist_;
    StorageType out_adjlist_;
};

template<bool Weighted>
class ProjectedGraphBuilder {
    using StorageType = std::conditional_t<Weighted, 
        std::unordered_map<VID, std::vector<std::pair<VID, float>>>,
        std::unordered_map<VID, std::vector<VID>>>;
public:
    ProjectedGraphBuilder(const ProjectedGraphBuilder &) = delete;
    ProjectedGraphBuilder(ProjectedGraphBuilder &&) = default;
    ProjectedGraphBuilder &operator=(const ProjectedGraphBuilder &) = delete;
    ProjectedGraphBuilder &operator=(ProjectedGraphBuilder &&) = default;
    
    ProjectedGraphBuilder(ProjectedGraphDirection dir) {
        if (dir == ProjectedGraphDirection::kBoth) {
            support_in_ = support_out_ = true;
        } else if (dir == ProjectedGraphDirection::kIn) {
            support_in_ = true;
            support_out_ = false;
        } else if (dir == ProjectedGraphDirection::kOut) {
            support_out_ = true;
            support_in_ = false;
        }
    }

    void add_edge(VID src, VID dst) requires(!Weighted) {
        if (support_out_) {
            auto iter = out_adjlist_.insert({src, {}}).first;
            out_adjlist_.insert({dst, {}});
            iter->second.push_back(dst);
        }

        if (support_in_) {
            auto iter = in_adjlist_.insert({dst, {}}).first;
            in_adjlist_.insert({src, {}});
            iter->second.push_back(src);
        }
    }

    void add_edge(VID src, VID dst, float weight) requires(Weighted) {
        if (support_out_) {
            auto iter = out_adjlist_.try_emplace(src).first;
            out_adjlist_.insert(std::pair<VID, std::vector<std::pair<VID, float>>>{dst, {}});
            iter->second.push_back({dst, weight});
        }

        if (support_in_) {
            auto iter = in_adjlist_.try_emplace(dst).first;
            in_adjlist_.insert(std::pair<VID, std::vector<std::pair<VID, float>>>{src, {}});
            iter->second.push_back({src, weight});
        }
    }

    ProjectedGraph build() && {
        auto compact_to_csr = [](StorageType &map) -> CSR {
            std::vector<size_t> offsets;
            std::vector<CSR::internal_id> edge_ids;
            std::vector<float> edge_weights;
            size_t num_edges = 0;
            
            std::vector<VID> all_vids;
            all_vids.reserve(map.size());
            for (const auto &[vid, list] : map) {
                all_vids.push_back(vid);
                num_edges += list.size();
            }
            std::sort(all_vids.begin(), all_vids.end());

            offsets.resize(all_vids.size() + 1);
            edge_ids.reserve(num_edges);

            if constexpr (Weighted) {
                edge_weights.reserve(num_edges);
            }

            auto transform_to_internal_id = [&](VID vid) -> CSR::internal_id {
                auto iter = std::lower_bound(all_vids.begin(), all_vids.end(), vid);
                return std::distance(all_vids.begin(), iter);
            };

            size_t offset = 0;
            for (size_t i = 0; i < all_vids.size(); ++i) {
                auto &list = map.at(all_vids[i]);
                offsets[i] = offset;
                offset += list.size();

                if constexpr (Weighted) {
                    std::sort(list.begin(), list.end(), [](const auto &lhs, const auto &rhs) {
                        return lhs.first < rhs.first; 
                    });
                    auto vids = std::views::common(
                        std::views::transform(std::views::keys(list), transform_to_internal_id));
                    auto weights = std::views::common(std::views::values(list));
                    edge_ids.insert(edge_ids.end(), vids.begin(), vids.end());
                    edge_weights.insert(edge_weights.end(), weights.begin(), weights.end());
                } else {
                    auto vids = std::views::common(
                        std::views::transform(list, transform_to_internal_id));
                    auto iter = edge_ids.insert(edge_ids.end(), vids.begin(), vids.end());
                    std::sort(iter, iter + list.size());
                }
            }
            offsets.back() = num_edges;

            if constexpr (Weighted) {
                return CSR{num_edges, std::move(all_vids), std::move(offsets), 
                           std::move(edge_ids), std::move(edge_weights)};
            } else {
                return CSR{num_edges, std::move(all_vids), std::move(offsets), std::move(edge_ids)};
            }
        };
        
        size_t num_vertices = 0;
        std::optional<CSR> in, out;
        if (support_in_) {
            in = compact_to_csr(in_adjlist_);
        }
        if (support_out_) {
            out = compact_to_csr(out_adjlist_);
        }

        if (support_in_ && support_out_) {
            assert(in->num_vertices() == out->num_vertices());
            num_vertices = in->num_vertices();
        } else if (support_in_) {
            num_vertices = in->num_vertices();
        } else {
            num_vertices = out->num_vertices();
        }

        return ProjectedGraph{num_vertices, std::move(in), std::move(out)};
    }

private:
    bool support_in_{false};
    bool support_out_{false};
    StorageType in_adjlist_;
    StorageType out_adjlist_;
};

}
