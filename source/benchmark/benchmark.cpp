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