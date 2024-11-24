#include "query.hpp"

#include <cassert>
#include <charconv>
#include <chrono>
#include <ranges>

#include "par/par.hpp"
#include "utils.hpp"

Query::Query()
    : max_parallelism_(par::get_max_threads() - 1) {}

Query::Query(uint32_t max_parallelism) noexcept
    : max_parallelism_(max_parallelism) {}

void Query::execute(Benchmark *parent_benchmark, Graph &graph, const QueryArgs &args, ResultTable *result_out) {
    size_t temp_storage_size = compute_temp_storage_size(graph, std::addressof(args));
    if (temp_storage_size == 0) {
        execute(parent_benchmark, graph, nullptr, 0, args, result_out);
        return;
    }
    auto temp_storage = std::make_unique_for_overwrite<std::byte []>(temp_storage_size);
    execute(parent_benchmark, graph, temp_storage.get(), temp_storage_size, args, result_out);
}

void ParseSingleArg(QueryArgs &args, DataType type, std::string_view x) {
    switch (type) {
    case DataType::kUint64  : {
        uint64_t val;
        auto result = std::from_chars(x.data(), x.data() + x.size(), val);
        assert(result.ec == std::errc{});
        args.append_u64(val);
    } break;
    case DataType::kInt64   : {
        int64_t val;
        auto result = std::from_chars(x.data(), x.data() + x.size(), val);
        assert(result.ec == std::errc{});
        args.append_i64(val);
    } break;
    case DataType::kFloat64 : {
        double val;
        auto result = std::from_chars(x.data(), x.data() + x.size(), val);
        assert(result.ec == std::errc{});
        args.append_f64(val);
    } break;
    case DataType::kString  : {
        args.append_str(x);
    } break;
    case DataType::kDateAsTimestamp : {
        auto year_str = x.substr(0, 4);
        auto month_str = x.substr(5, 2);
        auto day_str = x.substr(8, 2);

        int32_t year;
        uint32_t month, day;
        std::from_chars(year_str.data(), year_str.data() + year_str.size(), year); 
        std::from_chars(month_str.data(), month_str.data() + month_str.size(), month); 
        std::from_chars(day_str.data(), day_str.data() + day_str.size(), day); 

        auto ymd = std::chrono::year_month_day{std::chrono::year{year}, std::chrono::month{month}, std::chrono::day{day}};
        assert(ymd.ok());

        auto days = std::chrono::sys_days{ymd};
        auto ms = std::chrono::time_point_cast<std::chrono::milliseconds>(days);
        auto val = ms.time_since_epoch().count() - 28800000;
        args.append_u64(val);
    } break;
    case DataType::kStringList : {
        auto str_list = split_str(x, ";");
        args.append_str_list(std::move(str_list));
    } break;
    }
}

QueryArgs QueryArgs::parse_ldbc(const std::vector<DataType> &types, std::string_view s, const std::string &sep) {
    QueryArgs args;

    size_t pos = 0, nxt = 0, idx = 0;
    while (true) {
        nxt = s.find(sep, pos);
        if (nxt == std::string_view::npos)
            break;
        std::string_view part = s.substr(pos, nxt - pos);
        ParseSingleArg(args, types[idx], part);
        pos = nxt + 1;
        ++idx;
    }
    std::string_view last_part = s.substr(pos);
    ParseSingleArg(args, types[idx], last_part);
    return args;
}