#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <vector>

#include "AdjList.h"

namespace gollum {

// `VertexSet` is a container that represent a set of vertices. It support
// concurrently appending and accessing elements randomly, the vertices in set
// is unordered and unique.
class VertexSubset {
public:
    // `VertexSet::Unsafe` is an unsafe view of the vertex subset, it cannot be
    // used in a multi-threading context. It has no additional synchronization
    // overhead and should be used in a serial context.
    class Unsafe {
        friend class VertexSubset;
    public:
        [[nodiscard]] size_t size() const noexcept { 
            return parent_.shared_buffer_.Size(); 
        }

        [[nodiscard]] bool empty() const noexcept {
            return size() == 0;
        }

        [[nodiscard]] bool contains(uint64_t vid) const noexcept {
            return parent_.flags_[vid];
        }

        [[nodiscard]] size_t total_indegree() const noexcept {
            return parent_.shared_buffer_.TotalIndegree();
        }

        [[nodiscard]] size_t total_outdegree() const noexcept {
            return parent_.shared_buffer_.TotalOutdegree();
        }

        [[nodiscard]] std::span<const uint64_t> span() const noexcept { return parent_.shared_buffer_.Span(); }
        [[nodiscard]] std::span<uint64_t> span() noexcept { return parent_.shared_buffer_.Span(); }

        [[nodiscard]] auto begin() const noexcept { return span().begin(); }
        [[nodiscard]] auto begin() noexcept { return span().begin(); }
        [[nodiscard]] auto end() const noexcept { return span().end(); }
        [[nodiscard]] auto end() noexcept { return span().end(); }

        void add_vertex(uint64_t vid) {
            if (parent_.flags_[vid])
                return;
            parent_.flags_[vid] = true;
            size_t indegree = parent_.adjlist_->GetInNums(vid);
            size_t outdegree = parent_.adjlist_->GetOutNums(vid);
            parent_.shared_buffer_.AddVertex(vid, indegree, outdegree);
        }

        // This method is used to compact the thread-local buffers into the global buffer,
        // it's a part of unsafe view, so that it should be used carefully.
        // A common way to use this is to call it during superstep synchronization.
        void flush_buffers() {
            for (size_t i = 0; i < parent_.local_buffers_.size(); ++i) {
                auto &local_buffer = parent_.local_buffers_[i];
                if (local_buffer.Size() == 0)
                    continue;
                parent_.shared_buffer_.UnsafeEmitVertices(local_buffer.Span(),
                    local_buffer.TotalIndegree(), local_buffer.TotalOutdegree());
                local_buffer.Clear();
            }
        }

        void reset() {
            auto shared = parent_.shared_buffer_.Span();
            for (size_t i = 0; i < shared.size(); ++i)
                parent_.flags_[shared[i]] = false;
            for (size_t i = 0; i < parent_.local_buffers_.size(); ++i) {
                auto local = parent_.local_buffers_[i].Span();
                for (size_t j = 0; j < local.size(); ++i)
                    parent_.flags_[local[j]] = false;
                parent_.local_buffers_[i].Clear();
            }
            parent_.shared_buffer_.Clear();
        }

    private:
        VertexSubset &parent_;

        Unsafe(VertexSubset &parent) noexcept
            : parent_(parent) { }
    };

public:
    VertexSubset(AdjList &adjlist, size_t max_parallelism, std::span<uint64_t> shared_buffer_mem, std::span<bool> flags_mem)
        : adjlist_(std::addressof(adjlist)), shared_buffer_(shared_buffer_mem), flags_(flags_mem) {
        local_buffers_.resize(max_parallelism);
    }

    [[nodiscard]] size_t universe_size() const noexcept {
        return flags_.size();
    }

    [[nodiscard]] size_t max_parallelism() const noexcept { return local_buffers_.size(); }

    [[nodiscard]] bool contains(uint64_t vid) const noexcept {
        std::atomic_ref<bool> atomic_flag_ref{flags_[vid]};
        return atomic_flag_ref.load(std::memory_order_relaxed);
    }

    // Note that what this method does is a buffered appending, which means that
    // the new vertex may be not immediately visible to other threads.
    void add_vertex(size_t thread_index, uint64_t vid) {
        bool expected = false;
        std::atomic_ref<bool> atomic_flag_ref{flags_[vid]};
        if (!atomic_flag_ref.compare_exchange_strong(expected, true, std::memory_order_relaxed))
            return;

        LocalBuffer &local_buffer = local_buffers_[thread_index];
        if (local_buffer.Full()) {
            shared_buffer_.EmitVertices(local_buffer.Span(), local_buffer.TotalIndegree(), 
                                        local_buffer.TotalOutdegree());
            local_buffer.Clear();
        }
        local_buffer.AddVertex(vid, adjlist_->GetInNums(vid), adjlist_->GetOutNums(vid));
    }

    [[nodiscard]] Unsafe unsafe() noexcept {
        return Unsafe{*this};
    }

private:
    class LocalBuffer {
        static constexpr size_t kThreadLocalBufferSize = 102400;

        std::array<size_t, kThreadLocalBufferSize> store_;
        size_t                                     size_;
        size_t                                     total_indegree_;
        size_t                                     total_outdegree_;
    public:
        LocalBuffer() noexcept
            : size_(0), total_indegree_(0), total_outdegree_(0) { }

        [[nodiscard]] const size_t *Data() const noexcept { return store_.data(); }
        [[nodiscard]] size_t *Data() noexcept { return store_.data(); }
        [[nodiscard]] size_t Size() const noexcept { return size_; }
        [[nodiscard]] std::span<const size_t> Span() const noexcept { return {Data(), Size()}; }
        [[nodiscard]] std::span<size_t> Span() noexcept { return {Data(), Size()}; }

        [[nodiscard]] bool Full() const noexcept { return Size() == kThreadLocalBufferSize; }
        [[nodiscard]] size_t TotalIndegree() const noexcept { return total_indegree_; }
        [[nodiscard]] size_t TotalOutdegree() const noexcept { return total_outdegree_; }

        void AddVertex(size_t vid, size_t indegree, size_t outdegree) noexcept {
            assert(Size() + 1 <= kThreadLocalBufferSize);
            store_[size_++] = vid;
            total_indegree_ += indegree;
            total_outdegree_ += outdegree;
        }

        void Clear() noexcept { size_ = total_indegree_ = total_outdegree_ = 0; }
    };

    class SharedBuffer {
        std::span<uint64_t> store_;
        size_t              size_;
        size_t              total_indegree_;
        size_t              total_outdegree_;
    public:
        SharedBuffer(std::span<uint64_t> mem) noexcept
            : store_(mem), size_(0), total_indegree_(0), total_outdegree_(0) { }

        [[nodiscard]] const size_t *Data() const noexcept { return store_.data(); }
        [[nodiscard]] size_t *Data() noexcept { return store_.data(); }
        [[nodiscard]] size_t Size() const noexcept { return size_; }
        [[nodiscard]] std::span<const size_t> Span() const noexcept { return {Data(), Size()}; }
        [[nodiscard]] std::span<size_t> Span() noexcept { return {Data(), Size()}; }
        [[nodiscard]] size_t TotalIndegree() const noexcept { return total_indegree_; }
        [[nodiscard]] size_t TotalOutdegree() const noexcept { return total_outdegree_; }

        void AddVertex(uint64_t vid, size_t indegree, size_t outdegree) noexcept {
            assert(size_ + 1 <= store_.size());
            store_[size_++] = vid;
            total_indegree_ += indegree;
            total_outdegree_ += outdegree;
        }

        void EmitVertices(std::span<const uint64_t> vids, size_t total_indegree, size_t total_outdegree) {
            auto fetch_and_add = [](size_t *v, size_t x, std::memory_order order) {
                std::atomic_ref<size_t> ref{*v};
                return ref.fetch_add(x, order);
            };
            
            size_t n = vids.size();
            size_t start = fetch_and_add(&size_, n, std::memory_order_relaxed);
            std::copy_n(vids.data(), n, store_.data() + start);

            fetch_and_add(&total_indegree_, total_indegree, std::memory_order_relaxed);
            fetch_and_add(&total_outdegree_, total_outdegree, std::memory_order_relaxed);
        }
        
        // Not thread safe.
        void UnsafeEmitVertices(std::span<uint64_t> vids, size_t total_indegree, size_t total_outdegree) {
            size_t n = vids.size();
            std::copy_n(vids.data(), n, store_.data() + size_);
            size_ += n;
            total_indegree_ += total_indegree;
            total_outdegree_ += total_outdegree;
        }

        void Clear() noexcept { size_ = 0; }
    };

    AdjList                  *adjlist_;
    std::vector<LocalBuffer>  local_buffers_;
    SharedBuffer              shared_buffer_;
    std::span<bool>           flags_;
};

}