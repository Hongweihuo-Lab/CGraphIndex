#pragma once

#include "query/query.hpp"

class IC9Query : public Query {
public:
    IC9Query();

    const char *name() const noexcept override;
    bool is_parallel() const noexcept override;
    size_t compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept override;
    void execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                 const QueryArgs &args, ResultTable *result_out) override;
};

class IC9ParallelQuery : public Query {
public:
    IC9ParallelQuery();

    const char *name() const noexcept override;
    bool is_parallel() const noexcept override;
    size_t compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept override;
    void execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                 const QueryArgs &args, ResultTable *result_out) override;

private:
    void execute_v1(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                 const QueryArgs &args, ResultTable *result_out);
    void execute_v2(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                 const QueryArgs &args, ResultTable *result_out);
};