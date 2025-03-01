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

#include "benchmark/benchmark.hpp"

[[nodiscard]] BenchmarkResult
run_benchmark(Graph &graph, Benchmark &benchmark, const std::string &input_filename, 
              const std::string &tmp_dir,
              bool with_header, const std::string &arg_sep, size_t max_num) {
    size_t num = 0;
    std::string line;
    std::ifstream input{input_filename};
    std::vector<QueryArgs> case_args;

    benchmark.set_tmp_dir(tmp_dir);

    if (with_header) {
        std::getline(input, line);
    }

    while (std::getline(input, line)) {
        case_args.push_back(QueryArgs::parse_ldbc(benchmark.get_query_arg_types(), line, arg_sep));
        ++num;
        if (num >= max_num)
            break;
    }
    benchmark.set_cases(std::move(case_args));
    auto [prepare_time, total_time, num_succ] = benchmark.run(graph);
    double avg_time = total_time / static_cast<double>(num_succ);
    return {prepare_time, total_time, avg_time, num_succ};
}