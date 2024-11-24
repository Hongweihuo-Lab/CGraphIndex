#pragma once

#include <chrono>

#include "Graph.h"

#include "query/query.hpp"

struct InputFormat { bool has_header; std::string sep; };

class Benchmark {
public:
    Benchmark(std::unique_ptr<Query> &&query)
        : query_(std::move(query)) { }

    Benchmark(std::unique_ptr<Query> &&query, const std::vector<QueryArgs> &case_args)
        : query_(std::move(query)), cases_(case_args) { }

    Benchmark(std::unique_ptr<Query> &&query, std::vector<QueryArgs> &&case_args) noexcept
        : query_(std::move(query)), cases_(std::move(case_args)) { }

    virtual ~Benchmark() = default;

    virtual InputFormat get_input_format() const { return InputFormat{false, " "}; };

    void set_cases(const std::vector<QueryArgs> &case_args) {
        cases_ = case_args;
    }

    void set_cases(std::vector<QueryArgs> &&case_args) {
        cases_ = std::move(case_args);
    }

    void set_tmp_dir(const std::string &dir) {
        tmp_dir_ = dir;
    }

    const std::string &get_tmp_dir() const { return tmp_dir_; }

    virtual void prepare(Graph &graph) { }

    virtual void on_case_enter(const QueryArgs &args) { }
    virtual void on_case_exit(const QueryArgs &args) { }

    virtual std::span<std::byte> get_temp_storage() = 0;
    virtual ResultTable &get_result_table() = 0;

    Query &get_query() const noexcept { return *query_.get(); }

    virtual const std::vector<DataType> &get_query_arg_types() const = 0;

    [[nodiscard]] virtual std::tuple<double, double, size_t> run(Graph &graph) {
        double prepare_time = MeasureTime([&]() {
            prepare(graph);
        });

        double total_time = 0.0;
        size_t num = 0;
        auto temp = get_temp_storage();

        for (const auto &args : cases_) {
            on_case_enter(args);
            
            try {
                double time = MeasureTime([&]() {
                    query_->execute(this, graph, temp.data(), temp.size(), args, std::addressof(get_result_table()));
                });
                total_time += time;
                ++num;
            } catch (const std::exception &e) {
                printf("an error occurred and skipped: %s\n", e.what());
            }
            
            on_case_exit(args);
        }
        return {prepare_time, total_time, num};
    }

private:
    std::unique_ptr<Query> query_;
    std::vector<QueryArgs> cases_;
    std::string            tmp_dir_;

    template<typename F, typename ...ArgTs>
    [[nodiscard]] static double MeasureTime(F &&f, ArgTs &&...args) {
        auto start = std::chrono::high_resolution_clock::now();
        std::invoke(std::forward<F>(f), std::forward<ArgTs>(args)...);
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start).count();
    }
};

struct BenchmarkResult { double prepare_time, total_time, avg_time; size_t num; };

[[nodiscard]] BenchmarkResult
run_benchmark(Graph &graph, Benchmark &benchmark, const std::string &input_filename, 
              const std::string &tmp_dir,
              bool with_header = false,
              const std::string &arg_sep = " ",
              size_t max_num = std::numeric_limits<size_t>::max());