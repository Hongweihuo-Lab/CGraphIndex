#pragma once

#include "benchmark/benchmark.hpp"
#include "query/bi2.hpp"

class BI2Benchmark : public Benchmark {
public:
    BI2Benchmark()
        : Benchmark(std::make_unique<BI2Query>()),
          result_{DataType::kUint64, DataType::kString} { }

    InputFormat get_input_format() const override {
        return InputFormat{true, "|"};
    }

    void prepare(Graph &graph) override { }

    void on_case_enter(const QueryArgs &args) override { }

    void on_case_exit(const QueryArgs &args) override { }

    const std::vector<DataType> &get_query_arg_types() const override {
        static std::vector<DataType> types {DataType::kDateAsTimestamp, DataType::kString};
        return types;
    }

    std::span<std::byte> get_temp_storage() override { return {}; }

    ResultTable &get_result_table() override { return result_; }

private:
    ResultTable result_;
};