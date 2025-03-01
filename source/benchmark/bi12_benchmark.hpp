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