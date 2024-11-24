#pragma once

#include "benchmark/benchmark.hpp"
#include "query/bi19.hpp"

#include "graph_re.hpp"
#include "utils.hpp"
#include "par/par.hpp"
#include "gollum/graph_project.hpp"
#include "gollum/shortest_path.hpp"

class BI19Benchmark : public Benchmark {
public:
    BI19Benchmark()
        : Benchmark(std::make_unique<BI19Query>()),
          result_{DataType::kUint64, DataType::kUint64, DataType::kFloat64} { }

    InputFormat get_input_format() const override {
        return InputFormat{true, "|"};
    }

    void prepare(Graph &graph) override;

    void on_case_enter(const QueryArgs &args) override { }

    void on_case_exit(const QueryArgs &args) override { }

    const std::vector<DataType> &get_query_arg_types() const override {
        static std::vector<DataType> types {DataType::kUint64, DataType::kUint64};
        return types;
    }

    std::span<std::byte> get_temp_storage() override { return {}; }

    ResultTable &get_result_table() override { return result_; }

    const gollum::ProjectedGraph &precomputed_graph() const noexcept { return precomputed_graph_; }

private:
    ResultTable            result_;
    gollum::ProjectedGraph precomputed_graph_;
};