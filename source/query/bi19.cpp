#include "bi19.hpp"

#include <charconv>
#include <cmath>

#include "graph_re.hpp"
#include "utils.hpp"
#include "par/par.hpp"
#include "gollum/graph_project.hpp"
#include "gollum/shortest_path.hpp"

#include "benchmark/bi19_benchmark.hpp"

BI19Query::BI19Query() {

}

const char *BI19Query::name() const noexcept {
    return "business-intelligence-19";
}

bool BI19Query::is_parallel() const noexcept {
    return false;
}

size_t BI19Query::compute_temp_storage_size(Graph &graph, const QueryArgs *args) const noexcept {
    return 0;
}

void BI19Query::execute(Benchmark *parent_benchmark, Graph &g, std::byte *temp_storage, size_t temp_storage_size, 
                        const QueryArgs &args, ResultTable *result_out) {
    auto *benchmark = static_cast<BI19Benchmark *>(parent_benchmark);
    const auto &projected_graph = benchmark->precomputed_graph();
    GraphRe graph{g};

    uint64_t city1_id = args.get_u64(0);
    uint64_t city2_id = args.get_u64(1);
    VID city1_vid = graph.get_vid("place", city1_id);
    VID city2_vid = graph.get_vid("place", city2_id);

    auto person_vids1 = graph.get_incoming_edges(city1_vid, "person");
    auto person_vids2 = graph.get_incoming_edges(city2_vid, "person");

    result_out->resize(person_vids1.size() * person_vids2.size());
    auto person1_id_out = result_out->get_u64(0);
    auto person2_id_out = result_out->get_u64(1);
    auto total_weight_out = result_out->get_f64(2);

    if (person_vids1.size() > person_vids2.size()) {
        person_vids1.swap(person_vids2);
        person1_id_out = result_out->get_u64(1);
        person2_id_out = result_out->get_u64(0);
    }

    gollum::SerialDijkstraShortestPathRunner sp_runner{person_vids2};

    oneapi::tbb::parallel_for(oneapi::tbb::blocked_range<size_t>{0, person_vids1.size()},
        [&](oneapi::tbb::blocked_range<size_t> person1_range) {
            for (size_t i = person1_range.begin(); i < person1_range.end(); ++i) {
                VID person_vid1 = person_vids1[i];

                auto weights = sp_runner.get_total_costs(person_vid1, projected_graph);
                for (size_t j = 0; j < weights.size(); ++j) {
                    size_t idx = i * person_vids2.size() + j;
                    person1_id_out[idx] = person_vid1.value();
                    person2_id_out[idx] = person_vids2[j].value();
                    total_weight_out[idx] = weights[j];
                }
            }
        });
}