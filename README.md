# CGraphIndex
CGraphIndex is an entropy-compressed self-index for labeled property multidigraphs [1]. The space usage for CGraphIndex achieves the kth-order entropy space bound for multidigraph properties (the dominant term in practice) and the 1st-order entropy space bound for multidigraph structures. A self-index actually encodes the original input and thus there is no need to store the input separately. CGraphIndex supports fundamental and navigational operations on the structures and on the properties in constant time, supports substring extraction in near-optimal time, and efficiently supports BFS, PageRank [2], k-hop friends query, and typical interactive complex queries [3] and business intelligence queries [4].

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

After running **create_init**, some files are generated in the specified directory, inclusing **TmpAdjTable**, a temporary adjacency list file that is used to build
a compressed adjacency list later.

> ### Creating Compressed Adjacency List Structures, gStruct/gInStruct
The executable file **create_adjlist** is used to build the compressed adjacency lists **gStruct/gInStruct**, whose input is **TmpAdjTable**, the generated file in the preprocessing. The code files involved are **AdjList.h** and **AdjList.cpp**, and the related function file is **create_adjlist.cpp**.
Compile and generate the executable file **create_adjlist**.

Enter the command

**./create\_adjlist TmpAdjTabledir Objdir**

from the command line to build **gStruct/gInStruct**,   
where    
**TmpAdjTabledir** is the directory where the **TmpAdjTable** file is store; and     
**Objdir** is the directory where the generated **gStruct/gInStruct** are stored after the execution. 

Afrer running, some files, including **AdjList.myg** are generated in the specified directory.

> ### Building Index Structures, VIndex/RIndex
The executable file **create_index** is used to build the compressed self-indexes using **GeCSA**. The code files involved are **CSA.h** and **CSA.cpp**, and the related main function file is **create_index.cpp**. Compile and generate the executable file **create_index**.

Enter the command

**./create\_index blockSize Samplesize Objdir srcfile1 srcfile2 srcfile3 ...**

from the command line to build **VIndex/RIndex**,   
where   
**blockSize** is the block size for the $\Phi$ structure;   
**Samplesize** is the suffix array/inverse suffix array sampling step;   
**Objdir** is the directory where the generated indexes are stored; and   
**srcfile1 srcfile2 srcfile3 ...** are multiple parameters, which represents the texts that need to be created into compressed indexes using **GeCSA**.  

After running, there are index files generated in the directory /home/hzt/data/LDBCxx/ BvGraph/, in which all the index files have a suffix **.geindex**.

## Paper
[1] Hongwei Huo, Zongtao He, and Jeffrey S. Vitter,  Indexing Labeled Property Multidigraphs in Entropy Space, with Applications,  June 13, 2023.      

## References
[2] L. Page, S. Brin, R. Motwani and T. Winograd, The PageRank Citation Ranking : Bringing Order to the Web, In The Web Conference, 1999. [PDF](https://api.semanticscholar.org/CorpusID:1508503)
[3] O. Erling, A. Averbuch, J. Larriba-Pey, et al., The LDBC Social Network Benchmark: Interactive Workload, In SIGMOD, pp. 619-630, 2015. [PDF](https://doi.org/10.1145/2723372.2742786)     
[4] G. Szarnyas, J. Waudby, B.A. Steer, et al., The LDBC Social Network Benchmark: Business Intelligence Workload, In PVLDB, pp. 877-890, 2022. [PDF](https://doi.org/10.14778/3574245.3574270)  
[5] J. K"arkk"ainen, D. Kempa, and S. J. Puglisi, Parallel External Memory Suffix Sorting, In CPM, pp. 329-342, 2015. [PDF](https://doi.org/10.1007/978-3-319-19929-0_28)
