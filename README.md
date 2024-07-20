# CGraphIndex
CGraphIndex is a novel compressed self-index for labeled property multidigraphs. It for the first time achieves the kth-order entropy space bound for multidigraph properties (the dominant term in practice) and the 1st-order graph entropy for multidigraph structures. A self-index actually encodes the original input and thus there is no need to store the input separately.

CGraphIndex supports fundamental and navigational operations on the structures and on the properties in constant time, supports fast property extraction on vertices and edges, and efficiently supports typical interactive complex and business intelligence queries [1,2], as well as BFS and PageRank [3,4]. Experimental results on the tested large LDBC SNB data [5] demonstrate that substantial improvements in practice.

## 1. Compile and Build
### 1.1 System requirements
 - Ubuntu 20.04
 - GCC 10.0 or higher with C++20 standard support
 - CMake3.15 or higher
 - ninja-build

### 1.2 Third-party dependencies
 - oneTBB [6] : for parallel sorting and parallel primitives
 - OpenMP [7] : to support multithreading
 - BBHash [8] : for vertex ID remapping
 - pSAscan [9] : for building suffix arrays in external memoryre
 - divsufsort [10] : for building suffix arrays in memory

BBHash, pSAscan and divsufsort have been placed in the CGraphIndex directory in source code form. OpenMP and oneTBB need to be installed by the user and make CMake be able to find the corresponding configuration files. OpenMP is typically installed with common compiler toolchains such as GCC. For oneTBB, you can install it via a package manager or an installer script available at [onetbb-install](https://www.intel.com/content/www/us/en/docs/onetbb/get-started-guide/2021-12/install-on-linux-os.html).

### 1.3 Compilation method
To enable Debug messages, use the following commands:
```shell
1 cd CGraphIndex
2 cmake --build "$(pwd)/build" --config Debug --target all -j $(expr $(nproc) / 2) --
```
To execute compile in optimized mode, use the following commands:
```shell
1 cd CGraphIndex
2 cmake --build "$(pwd)/build" --config Release --target all -j $(expr $(nproc) / 2) --
```
The above commands will use half of the logical threads of the current machine to build, and after the build is completed, 4 executable files will be generated in the **CGraphIndex/build** directory:
 - **create_init** : Used to preprocess LDBC datasets and split them into graph structure information, vertex property text, and edge property text.
 - **create_adjlist** : Used to construct the compressed adjacency list structure gStruct/gInStruct, the primary input of which is the TmpAdjTable file generated in the preprocessing.
 - **create_index** : used to build the compressed self-indexes.
 - **run_benchmark** : for batch testing.

## 2. Test process
### 2.1 Dataset preparation
It is recommended to use a Docker image of [ldbc/datagen-standalone](https://hub.docker.com/r/ldbc/datagen-standalone) for dataset preparation, after pulling the image, run the following command to generate a dataset of the specified size:
```shell
mkdir -p "sf${SF}"
docker run \
  --mount type=bind,source="$(pwd)/sf${SF}",target=/out \
  ldbc/datagen-standalone --parallelism 1 -- --format csv --scale-factor ${SF} –mode interactive
```
Before running the above command, you need to set the variable **SF** in the shell, given the specified scale, for example, to generate a dataset of SF30, you need to set **SF=30**, and the generated files will be placed in the **./sf30** directory. 

### 2.2 Data preprocessing
Use the create_init program to preprocess LDBC raw data by:
```shell
1 create_init <data_dir> <dst_dir> <cut_size>
```
 - **data_dir** is the directory where the LDBC raw datasets are stored.
 - **dst_dir**  indicates the directory where the preprocessed files are stored.
 - **cut_size** represents the cut size of the vertex/edge property text in gigabytes. 

Since the peak memory for building the GIndex index is about 18 times the size of the input text, it is not recommended to set the text cut size greater than 50GB on machines with 1TB of memory.

After the **create_init** operation is completed, several files will be generated in the specified directory:
 - **Vertex_x/Edge_x** : the text that holds the vertex/edge properties, which is the input to GIndex to generate VIndex_x / RIndex_x.
 - **Vrows.myg/Erows.myg** : Holds the number of lines (i.e., the number) of the vertex/edge properties contained in each Vertex_x/Edge_x text. When you search for vertex/edge property data based on vertex Vid or edge Eid, you will determine on which line on which index file you need to search for based on the two files.
 - **Vkinds.myg** : Hold information about vertex labels; details see the code comments on the Vdetail class.
 - **Ekinds.myg** : Store edge-related information; details see the code comments on the Edetail class.
 - **Vmap.myg** : Store all MPhash.
 - **Emap.myg** : Hold the fixed-length code for each edge relation property.
 - **Etype.myg** : Store the edge relationship properties in increasing order of Eid, and uses an InArray to store.
 - **EHasdes.myg** : The B array that indicates if an edge contains the residual properties, and supports the rank operation.
 - **TmpAdjTable** : A temporary adjacency list file that is used to build a compressed adjacency list later.

### 2.3 Build compressed adjacency list structures
Use **create_adjlist** to build a compressed adjacency table structure gStruct/gInStruct in the following ways:
```shell
1 create_adjlist <tmp_adj_table_dir>c <dst_dir>
```
 - **tmp_adj_table_dir** : The directory where the TmpAdjTable generated for the preprocessing is located. 
 - **dst_dir** : The directory where the gStruct/gInStruct will be generated after the build is complete.

After the build is complete, the file is stored in the **dst_dir** directory and the file name is **AdjList.myg**.

### 2.4 Building indexes VIndex and RIndex
Use **create_index** to compress the text in the dataset using GIndex. How to use:
```shell
1 create_index <block_size> <sample_size> <dst_dir> <src_file1> [src_files...]
```
 - **block_size** : The chunk size of GIndex, recommend it to 128.
 - **sample_size** : The sample size of the suffix array in GIndex, recommend it to 64.
 - **dst_dir** : The directory for storing the generated GIndex compressed index.
 - **src_file1** ... : Text files that need to be processed, which are Vertex_x/Edge_x files resulting from the preprocessing part. 

After the build is complete, the file is stored in the **dst_dir** directory with the suffix **.geindex**.

### 2.5 Query tests
**run_benchmark** is used to test the query time performance of CGraphIndex, and output the time for the test in millisecond (ms), as follows:
```shell
1 run_benchmark <data_dir> <query> <input> <output> [max_num]
```
 - **data_dir**  is the directory where CGraphIndex's data files are located.
 - **query**  is the name of the query to be made, data type string, and its value must be one of the following table:
 - **input**  is the name of the input file, and each line of the input file represents an input, and each parameter in the input is separated by a space.
 - **output** is the name of the resulting output file to which the test results will be written.
 - **max_num**  is an optional parameter that indicates the maximum number of tests, and the final number of tests will not exceed the value of this parameter. If this parameter is not provided, every piece of data in the input file will be tested.

| query | descriptions |
| ---  | --- |
| bfs  |  BFS | 
| page_rank | PageRank |
| khops| k-hops friends query |
| ic1  | LDBC SNB-interactive-complex-1 query |
| ic5  | LDBC SNB-interactive-complex-5 query |
| ic13 | LDBC SNB-interactive-complex-13 query |
| bi3  | LDBC SNB-business-intelligence-3 query |
| bi10 | LDBC SNB-business-intelligence-10 query |
| bi16 | LDBC SNB-business-intelligence-16 query |


Input File Format Description:      
Different from the substitution_parameter generated in LDBC that uses | The format of the split parameters, run_benchmark requires the input parameters to be separated by spaces, and a Python3 script convert_sep.py is provided in the CGraphIndex/scripts directory to complete the process, and the script usage is:
```shell
1 python3 convert_sep.py -p <filename>
```
where **filename** is the input file to be processed.

## References
[1] Orri Erling et al., The LDBC Social Network Benchmark: Interactive Workload, In SIGMOD, pages 619–630, 2015. https://doi.org/10.1145/2723372.2742786

[2] Szárnyas et al., The LDBC Social Network Benchmark: Business Intelligence Workload. PVLDB 16(4): 877–890, 2022. https://doi.org/10.14778/3574245.3574270

[3]  Lawrence Page and Sergey Brin and Rajeev Motwani and Terry Winograd, The PageRank Citation Ranking: Bringing Order to the Web, The Web Conference, 1999.https://api.semanticscholar.org/CorpusID:1508503.     

[4]  Iosup et al., The LDBC Graphalytics Benchmark, 2023, https://arxiv.org/abs/2011.15028(https://doi.org/10.14778/3574245.3574270)  

[5] The LDBC Social Network Benchmark, 2024, https://arxiv.org/abs/2001.02299

[6] oneTBB, https://github.com/oneapi-src/oneTBB

[7] OpenMP, https://github.com/OpenMP

[8] Antoine Limasset, Guillaume Rizk, Rayan Chikhi, and Pierre Peterlongo, Fast and Scalable Minimal Perfect Hashing for Massive Key Sets, In SEA, pages 25:1–25:16, 2017. https://doi.org/10.4230/LIPIcs.SEA.2017.25.

[9] Juha Kärkkäinen, Dominik Kempa, and Simon J. Puglisi, Parallel External Memory Suffix Sorting, In CPM, pages 329--342, 2015, https://doi.org/10.1007/978-3-319-19929-0_28.

[10] Yuta Mori, A lightweight suffix-sorting library, 2008. https://github.com/y-256/libdivsufsort/.

