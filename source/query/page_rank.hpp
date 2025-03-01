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

#include <cstdint>

#include "../AdjList.h"

#include "query/query.hpp"

class SerialPageRankQuery : public Query {
public:
    SerialPageRankQuery();

    const char *name() const noexcept override;
    bool is_parallel() const noexcept override;
    size_t compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept override;
    void execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                 const QueryArgs &args, ResultTable *result_out) override;
};

class ParallelPageRankQuery : public Query {
public:
    ParallelPageRankQuery(uint32_t max_parallelism);

    const char *name() const noexcept override;
    bool is_parallel() const noexcept override;
    size_t compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept override;
    void execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                 const QueryArgs &args, ResultTable *result_out) override;
};