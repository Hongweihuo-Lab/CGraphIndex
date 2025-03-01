#pragma once

#include "query/query.hpp"

class BI3Query : public Query {
public:
    BI3Query();

    const char *name() const noexcept override;
    bool is_parallel() const noexcept override;
    size_t compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept override;
    void execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                 const QueryArgs &args, ResultTable *result_out) override;
};