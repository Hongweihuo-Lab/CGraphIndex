#pragma once

#include "benchmark/benchmark.hpp"
#include "query/bi12.hpp"

class BI12Benchmark : public Benchmark {
public:
    BI12Benchmark()
        : Benchmark(std::make_unique<BI12Query>()),
          result_{DataType::kUint64, DataType::kUint64} { }

    InputFormat get_input_format() const override {
        return InputFormat{true, "|"};
    }

    void prepare(Graph &graph) override { }

    void on_case_enter(const QueryArgs &args) override { }

    void on_case_exit(const QueryArgs &args) override { }

    const std::vector<DataType> &get_query_arg_types() const override {
        static std::vector<DataType> types {DataType::kDateAsTimestamp, DataType::kUint64, DataType::kStringList};
        return types;
    }

    std::span<std::byte> get_temp_storage() override { return {}; }

    ResultTable &get_result_table() override { return result_; }

private:
    ResultTable result_;
};