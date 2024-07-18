# CGraphIndex
CGraphIndex is a novel compressed self-index for labeled property multidigraphs. It for the first time achieves the kth-order entropy space bound for multidigraph properties (the dominant term in practice) and the 1st-order graph entropy for multidigraph structures. A self-index actually encodes the original input and thus there is no need to store the input separately.

CGraphIndex supports fundamental and navigational operations on the structures and on the properties in constant time, supports fast property extraction on vertices and edges, and efficiently supports typical interactive complex and business intelligence queries [1,2], as well as BFS and PageRank [3,4]. Experimental results on the tested large LDBC SNB data [5] demonstrate that substantial improvements in practice.

## 1. Compile and Build
### 1.1 System requirements
 - Ubuntu 20.04
 - GCC 10.0 or higher with C++20 standard support
 - CMake3.15 or higher
 - ninja-build

### 1.2 Third-party dependency
 - oneTBB: for parallel sorting and parallel primitives
 - OpenMP: to support multithreading
 - pSAscan: for building suffix arrays in external memory
 - divsufsort: for building suffix arrays in memory

### 1.3 Compilation method
To enable Debug messages, use the following commands:
```shell
1 cd CGraphIndex
2 cmake --build "$(pwd)/build" --config Debug --target all -j $(expr $(nproc) / 2) --
```
To execute compile in optimized mode when needed, use the following commands:
```shell
1 cd CGraphIndex
2 cmake --build "$(pwd)/build" --config Release --target all -j $(expr $(nproc) / 2) --
```
The above commands will use half of the logical threads of the current machine to build, and after the build is completed, 4 executable files will be generated in the CGraphIndex/build directory:
 - create_init: Used to preprocess LDBC datasets and split them into graph structure information, vertex property text, and edge property text.
 - create_adjlist: Used to construct the compressed adjacency list structure gStruct/gInStruct, the primary input of which is the TmpAdjTable file generated in the preprocessing.
 - create_index: used to build the compressed self-indexes.
 - run_benchmark: for batch testing.

## 2. Test process
### 2.1 Dataset preparation
It is recommended to use a Docker image of ldbc/datagen-standalone (https://hub.docker.com/r/ldbc/datagen-standalone) for deployment, and after pulling the image, run the following command to generate a dataset of the specified size:
```shell
1 mkdir -p "sf${SF}"
2 docker run \
3     --mount type=bind,source="$(pwd)/sf${SF}",target=/out \
4     ldbc/datagen-standalone --parallelism 1 -- --format csv --scale-factor ${SF} –mode interactive
```
Before running the above command, you need to set the variable SF in the shell, given the specified scale, for example, to generate a dataset of SF30, you need to set SF=30. 

### 2.2 Data preprocessing
Use the create_init program to preprocess LDBC raw data by:
```shell
1 create_init <data_dir> <dst_dir> <cut_size>
```
 - data_dir is the directory of the LDBC raw dataset.
 - dst_dir  indicates the directory where the preprocessed files are stored.
 - cut_size represents the cut size of the vertex/edge property text in gigabytes. 

Since the peak memory for building the GIndex index is about 18 times the size of the input text, it is not recommended to set the text cut size greater than 50GB on machines with 1TB of memory.

 - The PreProcess function is a constructor of the class, which classifies LDBC raw files according to the input directory parameters, and stores and sorts the files about vertices and edges separately.
 - The InitRun function is the main processing function, which mainly sets the number of multithreads and determines the data type of each column in the LDBC vertex and edge properties.
 - The HandleVertex function handles vertex properties, which is primarily a traversal process that performs the following operations on all vertices of each vertex label class: using MPHash for vertex mapping; Saves the vertex properties to Vertex_x in numbered order.
 - The HandleEdge function handles edge properties, which processes all outdegree vertex files for each vertex label class, for example, first processing the comment vertex, it will read and process all the edge files of the comment_xxxx_yyyy of this kind of comment outdegree, and generate a temporary external adjacency list TmpAdjTable; during which there may be insufficient memory, so the external memory is used, and part of the processing results are written to the external memory first. Finally merged.

After the create_init operation is completed, several files will be generated in the specified directory:
 - Vertex_x/Edge_x : the text that holds the vertex/edge properties, which is the input to GIndex to generate VIndex_x / RIndex_x.
 - Vrows.myg/Erows.myg : Holds the number of lines (i.e., the number) of the vertex/edge properties contained in each Vertex_x/Edge_x text. When you search for vertex/edge property data based on vertex Vid or edge Eid, you will determine on which line on which index file you need to search for based on the two files.
 - Vkinds.myg : Hold information about vertex labels; details see the code comments on the Vdetail class.
 - Ekinds.myg : Store edge-related information; details see the code comments on the Edetail class.
 - Vmap.myg : Store all MPhash.
 - Emap.myg : Hold the fixed-length code for each edge relation property.
 - Etype.myg : Store the edge relationship properties in increasing order of Eid, and uses an InArray to store.
 - EHasdes.myg : The B array that indicates if an edge contains the residual properties, and supports the rank operation.
 - TmpAdjTable : A temporary adjacency list file that is used to build a compressed adjacency list later.

### 2.3 Build Compressed Adjacency List Structures
Use create_adjlist to build a compressed adjacency table structure gStruct/gInStruct in the following ways:
```shell
1 create_adjlist <tmp_adj_table_dir> <dst_dir>
```
 - tmp_adj_table_dir : The directory where the TmpAdjTable generated for the preprocessing is located. 
 - dst_dir : The directory where the gStruct/gInStruct will be generated after the build is complete.

After the build is complete, the file is stored in the dst_dir directory and the file name is AdjList.myg.

### 2.4 Building indexes VIndex and RIndex
Use create_index to compress the text in the dataset using GIndex. How to use:
```shell
1 create_index <block_size> <sample_size> <dst_dir> <src_file1> [src_files...]
```
 - block_size : The chunk size of GIndex, recommend it to 128.
 - sample_size : The sample size of the suffix array in GIndex, recommend it to 64.
 - dst_dir : The directory for storing the generated GIndex compressed index.
 - src_file1 ... : Text files that need to be processed, which are Vertex_x/Edge_x files resulting from the preprocessing part. 

After the build is complete, the file is stored in the dst_dir directory with the suffix .geindex.

### 2.5 Query tests
run_benchmark Used to test the time performance of CGraphIndex, and output the time used for the test, in millisecond (ms), as follows:
```shell
1 run_benchmark <data_dir> <query> <input> <output> [max_num]
```
 - data_dir  is the directory where CGraphIndex's data files are located.
 - query  is the name of the query to be made, data type string, and its value must be one of the following table:

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

- input  is the name of the input file, and each line of the input file represents an input, and each parameter in the input is separated by a space.
 - output is the name of the resulting output file to which the test results will be written.
 - max_num  is an optional parameter that indicates the maximum number of tests, and the final number of tests will not exceed the value of this parameter. If this parameter is not provided, every piece of data in the input file will be tested.

Input File Format Description: 

Different from the substitution_parameter generated in LDBC that uses | The format of the split parameters, run_benchmark requires the input parameters to be separated by spaces, and a Python3 script convert_sep.py is provided in the CGraphIndex/scripts directory to complete the process, and the script usage is:
```shell
1 python3 convert_sep.py -p <filename>
```
where filename is the input file to be processed.



## Environments 
Experiments were done on Dell R750 server equipped with two 32-core 2.6-3.4GHZ Intel Xeon 8358P CPUs with 80MB L2 cache, 1TB RAM, one 960GB NVMe SSD for system,
and two 3.84TB NVMe SSDs in RAID0 configuration. The OS used was Ubuntu 20.04, 64 bit. The algorithms were implemented in C++11 and the programs were compiled using g++ 9.4.0 with -O3. We use parallel external memory suffix array construction [5] to build the suffix array for large input texts.  

## Related Directories

> ### Code for CGraphIndex
 The code for CGraphIndex is stored in the directory /home/hzt/code.

> ### LDBC date sets
The directory /home/hzt/data/  is the directory for the LDBC Datasets. There are also several folders in the LDBCxx directory (/home/hzt/data/LDBCxx)
under this directory, where the important folders include:
* ./BvGraph/ : This folder stores the index files of CGraphIndex;
* ./res/ : This folder stores test cases for various query problems; and
* ./social_network/ : This folder stores the LDBC raw datasets.

## Compile
The makefile file has been created which contains a set of tasks to be executed
for the compilation process of CGraphIndex.   
Enter the command

   **make**   

from the command line in the CGraphIndex directory to do the compilation and to generate four executable files: **create_init**,  **create_adjlist**, **create_index**, and **do_search**, each of which is described in the follwoing.

## Instructions for Usage
> ### Data Preprocessing
The executable file **create_init** is used to preprocess the LDBC dataset. The process reorganizes the LDBC dataset to generate graph structure information,
vertex property text, and edge property text. The primary code files include **InputInit.h** and **InputInit.cpp**, and the related file is **create_init.cpp**.

Enter the command   

 **.create_init Datadir Objdir Cutsize**   
 
from the command line to execute the preprocessing process,     
where     
**Datadir** is the directory where the LDBC raw datasets are stored;  
**Objdir** is the directory where the preprocessed/generated files are stored; and     
**Cutsize** indicates the partition size for the vertex/edge property text in gigabytes.

The partition size for the property text is **50** in GB for the available memory described in environment section, since the peak memory for building **GeCSA** is about~18 times the input text size.

After running **create_init**, some files are generated in the specified directory, including **TmpAdjTable**, a temporary adjacency list file that is used to build
a compressed adjacency list later.

> ### Creating Compressed Adjacency List Structures
The executable file **create_adjlist** is used to build the compressed adjacency lists **gStruct/gInStruct**, whose input is **TmpAdjTable**, the generated file in the preprocessing. The code files involved are **AdjList.h** and **AdjList.cpp**, and the related function file is **create_adjlist.cpp**.
Compile and generate the executable file **create_adjlist**.

Enter the command

**./create\_adjlist TmpAdjTabledir Objdir**

from the command line to build **gStruct/gInStruct**,   
where    
**TmpAdjTabledir** is the directory where the **TmpAdjTable** file is store; and     
**Objdir** is the directory where the generated **gStruct/gInStruct** are stored after the execution. 

Afrer running, some files, including **AdjList.myg** are generated in the specified directory.

> ### Building Self-indexes
The executable file **create_index** is used to build the compressed self-indexes using **GeCSA**. The code files involved are **CSA.h** and **CSA.cpp**, and the related main function file is **create_index.cpp**. Compile and generate the executable file **create_index**.

Enter the command

**./create\_index blockSize Samplesize Objdir srcfile1 srcfile2 srcfile3 ...**

from the command line to build **VIndex/RIndex**,   
where   
**blockSize** is the block size for the $\Phi$ structure;   
**Samplesize** is the suffix array/inverse suffix array sampling step;   
**Objdir** is the directory where the generated indexes are stored; and   
**srcfile1 srcfile2 srcfile3 ...** are multiple parameters, which represent the texts that need to be created into compressed indexes using **GeCSA**.  

After running, there are index files generated in the directory /home/hzt/data/LDBCxx/ BvGraph/, in which all the index files have a suffix **.geindex**.

> ### Query Processing
The executable file **do_search** is used for batch testing of **CGraphIndex**. The primary code files involved are **Graph.h** and **Graph.cpp**, and the related function file is **do_search.cpp**. Compile and generate an executable file **do_search**.
The relevant query interfaces provided by **CGraphIndex** are all in **Graph.h**, which also has relevant comments, and if you want to perform 
real-time queries and other operations, you can write your own main function to run. 
Here the **do\_search** is only for batch testing.

Enter the command   

**./do_search indexdir mod testcasefile searchtimefile > AAARES**

from the command line,   
where    
**indexdir** indicates the directory where the index files for **CGraphIndex** are stored;    
**mod** is the specific query problems, see the Table below for batch testing;    
**testcasefile** is the specific directory for the test cases;    
**searchtimefile** indicates which file the timing of the query is stored in; and    
The redirected **AAARES** is used to temporarily save the specific output of the query.

| MOD| Query |
| --- | --- |
| 14 |  BFS | 
| 15 | PageRank |
| 3 | kHopFriends |
| 4 | IC1 |
| 6 | IC5 |
| 8 | IC13 |
| 11 | BI3 |
| 12 | BI10 |
| 13 | BI16 |


> ### Batch Testing
In the directory for **CGraphIndex**, there is a script file called **auto.sh**, which consists of many commands,
including preprocessing, generating compressed adjacency lists, building **GeCSA**, and bulk querying.
When performing batch testing, select the corresponding query problem according to the above Table, remove the comment 
at the beginning of the line, and run it directly.


## References
[1] Orri Erling et al., The LDBC Social Network Benchmark: Interactive Workload, In SIGMOD, pages 619–630, 2015. https://doi.org/10.1145/2723372.2742786

[2] Szárnyas et al., The LDBC Social Network Benchmark: Business Intelligence Workload. PVLDB 16(4): 877–890, 2022. https://doi.org/10.14778/3574245.3574270

[3]  Lawrence Page and Sergey Brin and Rajeev Motwani and Terry Winograd, The PageRank Citation Ranking: Bringing Order to the Web, The Web Conference, 1999.https://api.semanticscholar.org/CorpusID:1508503.     

[4]  Iosup et al., The LDBC Graphalytics Benchmark, 2023, https://arxiv.org/abs/2011.15028(https://doi.org/10.14778/3574245.3574270)  

[5] The LDBC Social Network Benchmark, 2024, https://arxiv.org/abs/2001.02299

