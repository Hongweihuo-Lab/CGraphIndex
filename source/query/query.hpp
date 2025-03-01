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

#include <cassert>
#include <cstdlib>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <span>
#include <variant>
#include <vector>

#include "AdjList.h"
#include "Graph.h"

enum class DataType : uint32_t {
    kUint64, kInt64, kFloat64, kString, kStringList,
    
    
    kDateAsTimestamp
};

class QueryArgs {
    using VarType = std::variant<uint64_t, int64_t, double, std::string, std::vector<std::string>>;
public:
    [[nodiscard]] size_t size() const noexcept { return args_.size(); }

    [[nodiscard]] bool is_u64(size_t index) const noexcept { 
        assert(index < size());
        return args_[index].index() == static_cast<uint32_t>(DataType::kUint64);
    }
    [[nodiscard]] bool is_i64(size_t index) const noexcept {
        assert(index < size());
        return args_[index].index() == static_cast<uint32_t>(DataType::kInt64);
    }
    [[nodiscard]] bool is_f64(size_t index) const noexcept {
        assert(index < size());
        return args_[index].index() == static_cast<uint32_t>(DataType::kFloat64);
    }
    [[nodiscard]] bool is_str(size_t index) const noexcept {
        assert(index < size());
        return args_[index].index() == static_cast<uint32_t>(DataType::kString);
    }
    [[nodiscard]] bool is_str_list(size_t index) const noexcept {
        assert(index < size());
        return args_[index].index() == static_cast<uint32_t>(DataType::kStringList);
    }

    [[nodiscard]] uint64_t get_u64(size_t index) const noexcept {
        assert(index < size());
        const uint64_t *ptr = std::get_if<0>(std::addressof(args_[index]));
        assert(ptr != nullptr);
        return *ptr;
    }

    [[nodiscard]] int64_t get_i64(size_t index) const noexcept {
        assert(index < size());
        const int64_t *ptr = std::get_if<1>(std::addressof(args_[index]));
        assert(ptr != nullptr);
        return *ptr;
    }

    [[nodiscard]] double get_f64(size_t index) const noexcept {
        assert(index < size());
        const double *ptr = std::get_if<2>(std::addressof(args_[index]));
        assert(ptr != nullptr);
        return *ptr;
    }

    [[nodiscard]] std::string_view get_str(size_t index) const noexcept {
        assert(index < size());
        const std::string *ptr = std::get_if<3>(std::addressof(args_[index]));
        assert(ptr != nullptr);
        return *ptr;
    }

    [[nodiscard]] const std::vector<std::string> &get_str_list(size_t index) const noexcept {
        assert(index < size());
        const std::vector<string> *ptr = std::get_if<4>(std::addressof(args_[index]));
        assert(ptr != nullptr);
        return *ptr;
    }

    void append_u64(uint64_t x) {
        args_.emplace_back(std::in_place_index<0>, x);
    }
    void append_i64(int64_t x) {
        args_.emplace_back(std::in_place_index<1>, x);
    }
    void append_f64(double x) {
        args_.emplace_back(std::in_place_index<2>, x);
    }
    void append_str(const std::string &x) {
        args_.emplace_back(std::in_place_index<3>, x);
    }
    void append_str(std::string &&x) {
        args_.emplace_back(std::in_place_index<3>, std::move(x));
    }
    void append_str(std::string_view x) {
        append_str(std::string{x.begin(), x.end()});
    }
    void append_str_list(const std::vector<string> &x) {
        args_.emplace_back(std::in_place_index<4>, x);
    }
    void append_str_list(std::vector<string> &&x) {
        args_.emplace_back(std::in_place_index<4>, std::move(x));
    }

    static QueryArgs parse(std::string_view s);

    static QueryArgs parse_ldbc(const std::vector<DataType> &types, std::string_view s, const std::string &sep = " ");

public:
    std::vector<VarType> args_;
};

class ResultTable {
    using VarType = std::variant<std::unique_ptr<uint64_t []>, std::unique_ptr<int64_t []>, 
                                 std::unique_ptr<double []>, std::unique_ptr<std::string []>>;
public:
    template<typename InputIterT>
    ResultTable(InputIterT first, InputIterT last)
        : num_rows_(0) {
        using enum DataType;
        size_t n = std::distance(first, last);
        columns_.resize(n);
        for (size_t i = 0; i < n; ++i) {
            switch (*first++) {
            case kUint64  : columns_[i].emplace<0>(); break;
            case kInt64   : columns_[i].emplace<1>(); break;
            case kFloat64 : columns_[i].emplace<2>(); break;
            case kString  : columns_[i].emplace<3>(); break;
            }
        }
    }

    ResultTable(const std::vector<DataType> &table_schema)
        : ResultTable(table_schema.begin(), table_schema.end()) { }
    
    ResultTable(std::initializer_list<DataType> table_schema)
        : ResultTable(table_schema.begin(), table_schema.end()) { }

    [[nodiscard]] size_t num_columns() const noexcept { return columns_.size(); }
    [[nodiscard]] size_t num_rows() const noexcept { return num_rows_; }

    [[nodiscard]] bool is_u64(size_t col_index) const noexcept { 
        assert(col_index < num_columns());
        return columns_[col_index].index() == static_cast<uint32_t>(DataType::kUint64);
    }
    [[nodiscard]] bool is_i64(size_t col_index) const noexcept { 
        assert(col_index < num_columns());
        return columns_[col_index].index() == static_cast<uint32_t>(DataType::kInt64);
    }
    [[nodiscard]] bool is_f64(size_t col_index) const noexcept { 
        assert(col_index < num_columns());
        return columns_[col_index].index() == static_cast<uint32_t>(DataType::kFloat64);
    }
    [[nodiscard]] bool is_str(size_t col_index) const noexcept { 
        assert(col_index < num_columns());
        return columns_[col_index].index() == static_cast<uint32_t>(DataType::kString);
    }

    [[nodiscard]] std::span<const uint64_t> get_u64(size_t col_index) const noexcept {
        assert(col_index < num_columns());
        const std::unique_ptr<uint64_t []> *ptr = std::get_if<0>(std::addressof(columns_[col_index]));
        assert(ptr != nullptr);
        return {ptr->get(), num_rows()};
    }
    [[nodiscard]] std::span<uint64_t> get_u64(size_t col_index) noexcept {
        assert(col_index < num_columns());
        std::unique_ptr<uint64_t []> *ptr = std::get_if<0>(std::addressof(columns_[col_index]));
        assert(ptr != nullptr);
        return {ptr->get(), num_rows()};
    }
    [[nodiscard]] std::span<const int64_t> get_i64(size_t col_index) const noexcept {
        assert(col_index < num_columns());
        const std::unique_ptr<int64_t []> *ptr = std::get_if<1>(std::addressof(columns_[col_index]));
        assert(ptr != nullptr);
        return {ptr->get(), num_rows()};
    }
    [[nodiscard]] std::span<int64_t> get_i64(size_t col_index) noexcept {
        assert(col_index < num_columns());
        std::unique_ptr<int64_t []> *ptr = std::get_if<1>(std::addressof(columns_[col_index]));
        assert(ptr != nullptr);
        return {ptr->get(), num_rows()};
    }
    [[nodiscard]] std::span<const double> get_f64(size_t col_index) const noexcept {
        assert(col_index < num_columns());
        const std::unique_ptr<double []> *ptr = std::get_if<2>(std::addressof(columns_[col_index]));
        assert(ptr != nullptr);
        return {ptr->get(), num_rows()};
    }
    [[nodiscard]] std::span<double> get_f64(size_t col_index) noexcept {
        assert(col_index < num_columns());
        std::unique_ptr<double []> *ptr = std::get_if<2>(std::addressof(columns_[col_index]));
        assert(ptr != nullptr);
        return {ptr->get(), num_rows()};
    }
    [[nodiscard]] std::span<const std::string> get_str(size_t col_index) const noexcept {
        assert(col_index < num_columns());
        const std::unique_ptr<std::string []> *ptr = std::get_if<3>(std::addressof(columns_[col_index]));
        assert(ptr != nullptr);
        return {ptr->get(), num_rows()};
    }
    [[nodiscard]] std::span<std::string> get_str(size_t col_index) noexcept {
        assert(col_index < num_columns());
        std::unique_ptr<std::string []> *ptr = std::get_if<3>(std::addressof(columns_[col_index]));
        assert(ptr != nullptr);
        return {ptr->get(), num_rows()};
    }

    void resize(size_t rows) {
        assert(rows >= num_rows_);

        num_rows_ = rows;
        for (size_t i = 0; i < num_columns(); ++i) {
            std::visit([rows](auto &v) {
                using ValueType = typename std::remove_cvref_t<decltype(v)>::element_type;
                v = std::make_unique_for_overwrite<ValueType []>(rows);
            }, columns_[i]);
        }
    }

    void print() {
        if (num_rows_ == 0) {
            std::cout << "EMPTY TABLE\n";
        }

        for (size_t i = 0; i < num_rows_; ++i) {
            for (size_t j = 0; j < columns_.size(); ++j) {
                const auto &var = columns_[j];
                std::visit([&](const auto &x) {
                    using ElemType = typename std::remove_cvref_t<decltype(x)>::element_type;

                    std::cout << x[i];
                    if (j != columns_.size() - 1) {
                        std::cout << "|";
                    }
                }, var);
            }
            std::cout << "\n";
        }
    }

private:
    std::vector<VarType> columns_;
    size_t               num_rows_;
};

class Benchmark;

class Query {
public:
    Query();
    Query(uint32_t max_parallelism) noexcept;

    virtual ~Query() = default;

    [[nodiscard]] virtual const char *name() const noexcept = 0;
    [[nodiscard]] virtual bool is_parallel() const noexcept = 0;
    [[nodiscard]] virtual size_t compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept = 0;
    
    virtual void execute(Benchmark *parent_benchmark, Graph &graph, std::byte *temp_storage, size_t temp_storage_size, 
                         const QueryArgs &args, ResultTable *result_out) = 0;

    [[nodiscard]] uint32_t max_parallelism() const noexcept { return max_parallelism_; }

    void execute(Benchmark *parent_benchmark, Graph &graph, const QueryArgs &args, ResultTable *result_out);

protected:
    uint32_t max_parallelism_;
};

