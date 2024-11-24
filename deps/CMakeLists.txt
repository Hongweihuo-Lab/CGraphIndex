# Build BBHash library.
add_library(BBHash INTERFACE)
target_include_directories(BBHash INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/BBHash")

# Build divsufsort library.
set(DIVSUFSORT_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/divsufsort/divsufsort.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/divsufsort/sssort.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/divsufsort/trsort.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/divsufsort/utils.c"
)
add_library(divsufsort STATIC ${DIVSUFSORT_SOURCES})
target_include_directories(divsufsort PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/divsufsort")
target_compile_options(divsufsort PRIVATE -DHAVE_CONFIG_H)

# Build pSAscan library.
add_library(pSAscan STATIC "${CMAKE_CURRENT_SOURCE_DIR}/psascan/src/utils.cpp")
target_include_directories(pSAscan PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/psascan/src")
target_compile_options(pSAscan PRIVATE -march=native)