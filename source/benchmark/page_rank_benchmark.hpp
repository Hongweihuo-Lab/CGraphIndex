// Copyright (C) 2024  Yongze Yu, Zongtao He, Hongwei Huo, and Jeffrey S. Vitter
// 
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
// USA

#pragma once

#include "benchmark/benchmark.hpp"
#include "query/page_rank.hpp"

class ParallelPageRankBenchmark : public Benchmark {
public:
    ParallelPageRankBenchmark(uint32_t max_parallelism)
        : Benchmark(std::make_unique<ParallelPageRankQuery>(max_parallelism)),
          result_{DataType::kFloat64} { }
    
    void prepare(Graph &graph) override {
        temp_storage_size_ = get_query().compute_temp_storage_size(graph, nullptr);
        temp_storage_mem_ = std::make_unique_for_overwrite<std::byte []>(temp_storage_size_);
        result_.resize(graph.GetAdjList().GetVnum());
    }

    const std::vector<DataType> &get_query_arg_types() const override {
        static std::vector<DataType> types {DataType::kUint64, DataType::kFloat64};
        return types;
    }

    void on_case_enter(const QueryArgs &args) override { }

    void on_case_exit(const QueryArgs &args) override { }

    std::span<std::byte> get_temp_storage() override {
        return {temp_storage_mem_.get(), temp_storage_size_};
    }

    ResultTable &get_result_table() override { return result_; }

private:
    ResultTable                   result_;
    std::unique_ptr<std::byte []> temp_storage_mem_;
    size_t                        temp_storage_size_;
};

class SerialPageRankBenchmark : public Benchmark {
public:
    SerialPageRankBenchmark()
        : Benchmark(std::make_unique<SerialPageRankQuery>()),
          result_{DataType::kFloat64} { }
    
    void prepare(Graph &graph) override {
        temp_storage_size_ = get_query().compute_temp_storage_size(graph, nullptr);
        temp_storage_mem_ = std::make_unique_for_overwrite<std::byte []>(temp_storage_size_);
        result_.resize(graph.GetAdjList().GetVnum());
    }

    const std::vector<DataType> &get_query_arg_types() const override {
        static std::vector<DataType> types {DataType::kUint64, DataType::kFloat64};
        return types;
    }

    void on_case_enter(const QueryArgs &args) override { }

    void on_case_exit(const QueryArgs &args) override { }

    std::span<std::byte> get_temp_storage() override {
        return {temp_storage_mem_.get(), temp_storage_size_};
    }

    ResultTable &get_result_table() override { return result_; }

private:
    ResultTable                   result_;
    std::unique_ptr<std::byte []> temp_storage_mem_;
    size_t                        temp_storage_size_;
};