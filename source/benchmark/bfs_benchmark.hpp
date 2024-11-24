#pragma once

#include "benchmark/benchmark.hpp"
#include "query/bfs.hpp"

class ParallelBfsBenchmark : public Benchmark {
public:
    ParallelBfsBenchmark(uint32_t max_parallelism)
        : Benchmark(std::make_unique<ParallelBfsQuery>(max_parallelism)),
          result_{DataType::kUint64} { }
    
    void prepare(Graph &graph) override {
        num_vertices_= graph.GetAdjList().GetVnum();
        temp_storage_size_ = get_query().compute_temp_storage_size(graph, nullptr);
        temp_storage_mem_ = std::make_unique_for_overwrite<std::byte []>(temp_storage_size_);
        result_.resize(num_vertices_);

        frontier1_flags_mem_ = reinterpret_cast<bool *>(temp_storage_mem_.get() + num_vertices_ * sizeof(uint64_t) * 2);
        frontier2_flags_mem_ = reinterpret_cast<bool *>(frontier1_flags_mem_ + num_vertices_ * sizeof(bool));
    }

    void on_case_enter(const QueryArgs &args) override {
        auto depths = result_.get_u64(0);
        memset(frontier1_flags_mem_, 0, sizeof(bool) * num_vertices_);
        memset(frontier2_flags_mem_, 0, sizeof(bool) * num_vertices_);
        memset(depths.data(), 0xFF, sizeof(uint64_t) * depths.size());
    }

    void on_case_exit(const QueryArgs &args) override { }

    const std::vector<DataType> &get_query_arg_types() const override {
        static std::vector<DataType> types {DataType::kUint64, DataType::kString};
        return types;
    }

    std::span<std::byte> get_temp_storage() override {
        return {temp_storage_mem_.get(), temp_storage_size_};
    }

    ResultTable &get_result_table() override { return result_; }

private:
    ResultTable                    result_;
    std::unique_ptr<std::byte []>  temp_storage_mem_;
    size_t                         temp_storage_size_;
    size_t                         num_vertices_;
    bool                          *frontier1_flags_mem_;
    bool                          *frontier2_flags_mem_;
};

class SerialBfsBenchmark : public Benchmark {
public:
    SerialBfsBenchmark()
        : Benchmark(std::make_unique<SerialBfsQuery>()),
          result_{DataType::kUint64} { }
    
    void prepare(Graph &graph) override {
        num_vertices_= graph.GetAdjList().GetVnum();
        temp_storage_size_ = get_query().compute_temp_storage_size(graph, nullptr);
        temp_storage_mem_ = std::make_unique_for_overwrite<std::byte []>(temp_storage_size_);
        result_.resize(num_vertices_);
    }

    void on_case_enter(const QueryArgs &args) override {
        auto depths = result_.get_u64(0);
        memset(depths.data(), 0xFF, sizeof(uint64_t) * depths.size());
    }

    void on_case_exit(const QueryArgs &args) override { }

    const std::vector<DataType> &get_query_arg_types() const override {
        static std::vector<DataType> types {DataType::kUint64, DataType::kString};
        return types;
    }

    std::span<std::byte> get_temp_storage() override {
        return {temp_storage_mem_.get(), temp_storage_size_};
    }

    ResultTable &get_result_table() override { return result_; }

private:
    ResultTable                    result_;
    std::unique_ptr<std::byte []>  temp_storage_mem_;
    size_t                         temp_storage_size_;
    size_t                         num_vertices_;
};