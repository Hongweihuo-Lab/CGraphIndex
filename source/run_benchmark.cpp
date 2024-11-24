#include <charconv>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <vector>

#include "Graph.h"

static void PrintHelpMessage() {
    const char *msg = 
R"(Usage: cgi_benchmark [-h] [<data_dir> <query> <input> <output> [max_num]]
  -h, --help - show help message and exits
  data_dir   - the CGraphIndex storage directory
  query      - allowed options: {"startup", "bfs", "page_rank", "serial_bfs", "serial_page_rank", "khops", "ic1", "ic5", "ic10", "ic13", "bi2", "bi3", "bi10", "bi12", "bi16", "bi19"}
  input      - input filename
  output     - output filename
  max_num    - maximum number of test cases)";
    std::cerr << msg << "\n";
}

static const std::vector<std::string_view> kAllowedQueryNames {
    "startup", "bfs", "page_rank", "serial_bfs", "serial_page_rank", "khops", "ic1", "ic5", "ic10", "ic13", "bi2", "bi3", "bi12", "bi10", "bi16", "bi19"
};

template<typename F, typename ...ArgTs>
[[nodiscard]] static inline double MeasureTimeInSecs(F &&f, ArgTs &&...args) {
    auto start = std::chrono::high_resolution_clock::now();
    std::invoke(std::forward<F>(f), std::forward<ArgTs>(args)...);
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
}

int main(int argc, const char* argv[]) {
    std::vector<std::string> args; args.resize(argc - 1);
    for (int i = 0; i < argc - 1; ++i)
        args[i] = argv[i + 1];

    if (args.size() == 1 && (args.front() == "-h" || args.front() == "--help")) {
        PrintHelpMessage();
        return 0;
    } else if (args.size() != 4 && args.size() != 5 && args.size() != 2) {
        std::cerr << "[ERROR] invalid arguments\n\n";
        PrintHelpMessage();
        return 0;
    } else {
        const std::string &data_dir = args[0];
        if (!std::filesystem::exists(data_dir)) {
            std::cerr << "[ERROR] `data_dir` is not existed\n";
            return 0;
        }

        std::string tmp_dir = data_dir + "/cgi_tmp/";
        if (!std::filesystem::exists(tmp_dir)) {
            std::filesystem::create_directory(tmp_dir);
        }

        const std::string &query = args[1];
        if (std::find(kAllowedQueryNames.begin(), kAllowedQueryNames.end(), query) == kAllowedQueryNames.end()) {
            std::cerr << "[ERROR] `query` is not allowed\n";
            std::cerr << R"(allowed options: {"startup", "bfs", "page_rank", "serial_bfs", "serial_page_rank", "khops", "ic1", "ic5", "ic10", "ic13", "bi2", "bi3", "bi10", "bi12", "bi16", "bi19"})"
                      << "\n";
            return 0;
        }

        if (query == "startup") {
            std::unique_ptr<Graph> graph;

            double startup_time_in_secs = MeasureTimeInSecs([&] () {
                graph = std::make_unique<Graph>();
                graph->LoadFiles(data_dir);
            });
            std::cout << "startup time: " << std::fixed << std::setprecision(3) << startup_time_in_secs << " secs\n";
        } else {
            const std::string &input = args[2];
            if (!std::filesystem::exists(input)) {
                std::cerr << "[ERROR] `input` is not existed\n";
                return 0;
            }
            const std::string &output = args[3];
            if (!std::filesystem::exists(output)) {
                std::cerr << "[ERROR] `output` is not existed\n";
                return 0;
            }

            size_t max_num = std::numeric_limits<size_t>::max();
            if (args.size() == 5) {
                size_t val;
                const std::string &s = args.back();
                auto parse_result = std::from_chars(s.data(), s.data() + s.size(), val);
                if (parse_result.ec != std::errc{}) {
                    std::cerr << "[ERROR] `max_num` is invalid\n";
                    return 0;
                }
                max_num = val;
            }

            std::unique_ptr<Graph> graph;

            double startup_time_in_secs = MeasureTimeInSecs([&] () {
                graph = std::make_unique<Graph>();
                graph->LoadFiles(data_dir);
            });
            std::cout << "startup time: " << std::fixed << std::setprecision(3) << startup_time_in_secs << " secs\n";
            graph->TestRun(query, input, output, max_num, tmp_dir, false);
        }
    }
    return 0;
}