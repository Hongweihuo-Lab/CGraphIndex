#pragma once

#include "benchmark/benchmark.hpp"
#include "query/ic9.hpp"

#include "graph_re.hpp"

class IC9Benchmark : public Benchmark {
public:
    IC9Benchmark()
        : Benchmark(std::make_unique<IC9ParallelQuery>()),
          result_{DataType::kUint64, DataType::kString, DataType::kString, DataType::kUint64, DataType::kString, DataType::kUint64} { }

    InputFormat get_input_format() const override {
        return InputFormat{true, "|"};
    }

    void prepare(Graph &graph) override {
        temp_storage_size_ = get_query().compute_temp_storage_size(graph, nullptr);
        if (temp_storage_size_ == 0) {
            return;
        }

        temp_storage_mem_ = std::make_unique_for_overwrite<std::byte []>(temp_storage_size_);
        num_vertices_ = GraphRe(graph).count_vtype("person");

        frontier1_flags_mem_ = reinterpret_cast<bool *>(temp_storage_mem_.get() + num_vertices_ * sizeof(uint64_t) * 2);
        frontier2_flags_mem_ = reinterpret_cast<bool *>(frontier1_flags_mem_ + num_vertices_ * sizeof(bool));
    }

    void on_case_enter(const QueryArgs &args) override {
        if (temp_storage_size_ == 0) {
            return;
        }

        memset(frontier1_flags_mem_, 0, sizeof(bool) * num_vertices_);
        memset(frontier2_flags_mem_, 0, sizeof(bool) * num_vertices_);
    }

    void on_case_exit(const QueryArgs &args) override { }

    const std::vector<DataType> &get_query_arg_types() const override {
        static std::vector<DataType> types {DataType::kUint64, DataType::kUint64};
        return types;
    }

    std::span<std::byte> get_temp_storage() override { return {temp_storage_mem_.get(), temp_storage_size_}; }

    ResultTable &get_result_table() override { return result_; }

private:
    ResultTable                    result_;
    size_t                         temp_storage_size_;
    std::unique_ptr<std::byte []>  temp_storage_mem_;
    size_t                         num_vertices_;
    bool                          *frontier1_flags_mem_;
    bool                          *frontier2_flags_mem_;
};