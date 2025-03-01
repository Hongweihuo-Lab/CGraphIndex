cmake_minimum_required(VERSION 3.15)

project(CGraphIndex)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)

add_subdirectory(deps)

set(CGI_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/source")

set(CGI_HEADERS
    "${CGI_SOURCE_DIR}/ABS_Phi.h"
    "${CGI_SOURCE_DIR}/AdjList.h"
    "${CGI_SOURCE_DIR}/BaseClass.h"
    "${CGI_SOURCE_DIR}/BooPHF.h"
    "${CGI_SOURCE_DIR}/CSA.h"
    "${CGI_SOURCE_DIR}/Gen_Phi.h"
    "${CGI_SOURCE_DIR}/Graph.h"
    "${CGI_SOURCE_DIR}/InArray.h"
    "${CGI_SOURCE_DIR}/InputInit.h"
    "${CGI_SOURCE_DIR}/loadkit.h"
    "${CGI_SOURCE_DIR}/parmaters.h"
    "${CGI_SOURCE_DIR}/Phi.h"
    "${CGI_SOURCE_DIR}/savekit.h"
    "${CGI_SOURCE_DIR}/select.h"

    "${CGI_SOURCE_DIR}/benchmark/benchmark.hpp"
    "${CGI_SOURCE_DIR}/benchmark/bfs_benchmark.hpp"
    "${CGI_SOURCE_DIR}/benchmark/page_rank_benchmark.hpp"
    "${CGI_SOURCE_DIR}/benchmark/bi19_benchmark.hpp"

    "${CGI_SOURCE_DIR}/query/bfs.hpp"
    "${CGI_SOURCE_DIR}/query/query.hpp"
    "${CGI_SOURCE_DIR}/query/page_rank.hpp"
    "${CGI_SOURCE_DIR}/query/bi2.hpp"
    "${CGI_SOURCE_DIR}/query/ic2.hpp"
    "${CGI_SOURCE_DIR}/query/ic9.hpp"
    "${CGI_SOURCE_DIR}/query/ic10.hpp"
    "${CGI_SOURCE_DIR}/query/bi12.hpp"
    "${CGI_SOURCE_DIR}/query/bi19.hpp"
)

set(CGI_SOURCES
    "${CGI_SOURCE_DIR}/AdjList.cpp"
    "${CGI_SOURCE_DIR}/CSA.cpp"
    "${CGI_SOURCE_DIR}/Gen_CSA.cpp"
    "${CGI_SOURCE_DIR}/Gen_Phi.cpp"
    "${CGI_SOURCE_DIR}/Graph.cpp"
    "${CGI_SOURCE_DIR}/InArray.cpp"
    "${CGI_SOURCE_DIR}/InputInit.cpp"
    "${CGI_SOURCE_DIR}/loadkit.cpp"
    "${CGI_SOURCE_DIR}/Phi.cpp"
    "${CGI_SOURCE_DIR}/savekit.cpp"

    "${CGI_SOURCE_DIR}/benchmark/benchmark.cpp"
    "${CGI_SOURCE_DIR}/benchmark/bi19_benchmark.cpp"

    "${CGI_SOURCE_DIR}/query/bfs.cpp"
    "${CGI_SOURCE_DIR}/query/query.cpp"
    "${CGI_SOURCE_DIR}/query/page_rank.cpp"
    "${CGI_SOURCE_DIR}/query/bi2.cpp"
    "${CGI_SOURCE_DIR}/query/bi3.cpp"
    "${CGI_SOURCE_DIR}/query/bi12.cpp"
    "${CGI_SOURCE_DIR}/query/bi19.cpp"
    "${CGI_SOURCE_DIR}/query/ic2.cpp"
    "${CGI_SOURCE_DIR}/query/ic9.cpp"
    "${CGI_SOURCE_DIR}/query/ic10.cpp"
)

add_library(CGraphIndex STATIC ${CGI_SOURCES})
target_include_directories(CGraphIndex PUBLIC "${CGI_SOURCE_DIR}")
target_compile_options(CGraphIndex PRIVATE -pedantic -funroll-loops -march=native)
target_link_libraries(CGraphIndex PRIVATE divsufsort pSAscan pthread PUBLIC BBHash)

find_package(TBB REQUIRED)
if(TBB_FOUND)
    target_link_libraries(CGraphIndex PRIVATE TBB::tbb)
else()
    message(FATAL_ERROR "Cannot find TBB library")
endif()

find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(CGraphIndex PRIVATE OpenMP::OpenMP_CXX)
else()
    message(FATAL_ERROR "Cannot find OpenMP library")
endif()

add_executable(create_init "${CGI_SOURCE_DIR}/create_init.cpp")
target_link_libraries(create_init PRIVATE CGraphIndex)

add_executable(create_adjlist "${CGI_SOURCE_DIR}/create_adjlist.cpp")
target_link_libraries(create_adjlist PRIVATE CGraphIndex)

add_executable(create_index "${CGI_SOURCE_DIR}/create_index.cpp")
target_link_libraries(create_index PRIVATE CGraphIndex)

add_executable(run_benchmark "${CGI_SOURCE_DIR}/run_benchmark.cpp")
target_link_libraries(run_benchmark PRIVATE CGraphIndex)

add_executable(add_prefix "add_prefix.cpp")