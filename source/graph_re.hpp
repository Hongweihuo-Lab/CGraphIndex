#pragma once

#include <string_view>

#include "Graph.h"

struct VID { 
    VID() { val_ = std::numeric_limits<uint64_t>::max(); }

    VID(uint64_t val) : val_(val) { }

    uint64_t value() const noexcept { return val_; }

    VID &operator++() { ++val_; return *this; }
    VID operator++(int) { VID cpy{*this}; ++val_; return cpy; }
    VID &operator--() { --val_; return *this; }
    VID operator--(int) { VID cpy{*this}; --val_; return cpy; }

private:
    uint64_t val_; 
};

inline bool operator==(VID lhs, VID rhs) noexcept { return lhs.value() == rhs.value(); }
inline auto operator<=>(VID lhs, VID rhs) noexcept { return lhs.value() <=> rhs.value(); }

struct EID { 
    EID(uint64_t val) : val_(val) { }

    uint64_t value() const noexcept { return val_; }

private:
    uint64_t val_; 
};

inline bool operator==(EID lhs, EID rhs) noexcept { return lhs.value() == rhs.value(); }
inline auto operator<=>(EID lhs, EID rhs) noexcept { return lhs.value() <=> rhs.value(); }

class GraphRe {
public:
    GraphRe(Graph &graph) noexcept
        : g_(&graph) { }

    Graph *get_underlying() { return g_; }
    AdjList *get_adjlist() { return g_->Adj; }

    size_t num_vertices() { return g_->Adj->GetVnum(); }
    size_t num_edges() { return g_->Adj->GetEnum(); }

    VID get_vid(const std::string &vtype, uint64_t id) {
        return VID{static_cast<uint64_t>(g_->GetRealId(vtype, id))};
    }

    VID get_vid_by_prop(const std::string &vtype, const std::string &col_name, const std::string &col_val) {
        u64 vid;
        g_->GetRealId(vtype, col_val, col_name, vid);
        return VID{vid};
    }

    std::string get_vertex_props_raw(VID vid) {
        std::string raw;
        u64 val = vid.value();
        g_->GetVInfo(raw, val);
        return raw;
    }

    std::string get_vertex_prop(VID vid, const std::string &col_name) {
        auto raw = get_vertex_props_raw(vid);
        auto vtype = get_vtype(vid);
        return g_->GetRowInfo(col_name, raw, vtype);
    }

    std::string get_vtype(VID vid) {
        std::string res;
        u64 val = vid.value();
        g_->GetVKindsById(res, val);
        return res;
    }

    size_t count_vtype(const std::string &vtype) {
        return g_->vkinds.at(vtype).n;
    }

    std::pair<VID, VID> get_vtype_vid_range(const std::string &vtype) {
        const auto &info = g_->vkinds.at(vtype);
        VID start{info.st};
        VID end{info.st + info.n};
        return {start, end};
    }

    std::string get_edge_props_raw(EID eid) {
        std::string raw;
        u64 val = eid.value();
        g_->GetEInfo(raw, val);
        return raw;
    }

    std::string get_etype(EID eid) {
        std::string res;
        u64 val = eid.value();
        g_->GetEType(res, val);
        return res;
    }

    size_t count_etype(const std::string &etype) {
        return g_->ekinds.at(etype).n;
    }

    std::vector<uint64_t> get_incoming_edges(VID vid) {
        size_t num = g_->Adj->GetInNums(vid.value());
        std::vector<uint64_t> res(num);
        g_->Adj->__get_input_neighbors(vid.value(), reinterpret_cast<u64 *>(res.data()), res.size());
        return res;
    }

    std::vector<uint64_t> get_outgoing_edges(VID vid) {
        size_t num = g_->Adj->GetOutNums(vid.value());
        std::vector<uint64_t> res(num);
        g_->Adj->__get_output_neighbors(vid.value(), reinterpret_cast<u64 *>(res.data()), res.size());
        return res;
    }

    std::vector<VID> get_incoming_edges(VID vid, const std::string &neighbor_vtype) {
        size_t num = g_->Adj->GetInNums(vid.value());
        std::vector<u64> all_neighbors(num);
        g_->Adj->__get_input_neighbors(vid.value(), all_neighbors.data(), all_neighbors.size());

        auto [vid_start, vid_end] = get_vtype_vid_range(neighbor_vtype);
        std::vector<VID> filtered_neighbors;
        for (size_t i = 0; i < all_neighbors.size(); ++i) {
            if (all_neighbors[i] >= vid_start.value() && all_neighbors[i] < vid_end.value()) {
                filtered_neighbors.push_back(VID{all_neighbors[i]});
            }
        }
        return filtered_neighbors;
    }

    std::vector<VID> get_outgoing_edges(VID vid, const std::string &neighbor_vtype) {
        size_t num = g_->Adj->GetOutNums(vid.value());
        std::vector<u64> all_neighbors(num);
        g_->Adj->__get_output_neighbors(vid.value(), all_neighbors.data(), all_neighbors.size());

        auto [vid_start, vid_end] = get_vtype_vid_range(neighbor_vtype);
        std::vector<VID> filtered_neighbors;
        for (size_t i = 0; i < all_neighbors.size(); ++i) {
            if (all_neighbors[i] >= vid_start.value() && all_neighbors[i] < vid_end.value()) {
                filtered_neighbors.push_back(VID{all_neighbors[i]});
            }
        }
        return filtered_neighbors;
    }

private:
    Graph *g_;
};

namespace std {

template<>
struct hash<::VID> {
    size_t operator()(VID x) const noexcept {
        return std::hash<uint64_t>{}(x.value());
    }
};

template<>
struct hash<::EID> {
    size_t operator()(EID x) const noexcept {
        return std::hash<uint64_t>{}(x.value());
    }
};

}