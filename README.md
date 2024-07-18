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

