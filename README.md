# CGraphIndex
CGraphIndex is an entropy-compressed self-index for labeled property multidigraphs [1]. The space usage for CGraphIndex achieves the kth-order entropy space bound for multidigraph properties (the dominant term in practice) and the 1st-order entropy space bound for multidigraph structures. A self-index actually encodes the original input and thus there is no need to store the input separately. CGraphIndex supports fundamental and navigational operations on the structures and on the properties in constant time, supports substring extraction in near-optimal time, and efficiently supports BFS, PageRank [2], k-hop friends query, and typical interactive complex queries [3] and business intelligence queries [4].

## Environments 
Experiments were done on Dell R750 server equipped with two 32-core 2.6-3.4GHZ Intel Xeon 8358P CPUs with 80MB L2 cache, 1TB RAM, one 960GB NVMe SSD for system,
and two 3.84TB NVMe SSDs in RAID0 configuration. The OS used was Ubuntu 20.04, 64 bit. The algorithms were implemented in C++11 and the programs were compiled using g++ 9.4.0 with -O3. We use parallel external memory suffix array construction [5] to build the suffix array for large input texts.  

## Related Directories

### Code for CGraphIndex
The code for CGraphIndex is stored in the directory /home/hzt/code.

### LDBC date sets
The directory /home/hzt/data/ \ is the directory for the LDBC Datasets. There are also several folders in the LDBCxx directory (/home/hzt/data/LDBCxx)
under this directory, where the important folders include:
> - ./BvGraph/ : This folder stores the index files of CGraphIndex;
* ./res/ : This folder stores test cases for various query problems; and
* ./social\_network/ : This folder stores the LDBC raw datasets.

## Compile
The makefile file has been created which contains a set of tasks to be executed
for the compilation process of CGraphIndex.   
Enter the command

**make**   

from the command line in the CGraphIndex directory to do the compilation and to generate four executable files: **create_init**,  **create_adjlist**, **create_index**, and **do_search**, each of which is described in the follwoing.


## Paper
[1] Hongwei Huo, Zongtao He, and Jeffrey S. Vitter,  Indexing Labeled Property Multidigraphs in Entropy Space, with Applications,  June 13, 2023.      

## References
[2] L. Page, S. Brin, R. Motwani and T. Winograd, The PageRank Citation Ranking : Bringing Order to the Web, In The Web Conference, 1999. [PDF](https://api.semanticscholar.org/CorpusID:1508503)
[3] O. Erling, A. Averbuch, J. Larriba-Pey, et al., The LDBC Social Network Benchmark: Interactive Workload, In SIGMOD, pp. 619-630, 2015. [PDF](https://doi.org/10.1145/2723372.2742786)     
[4] G. Szarnyas, J. Waudby, B.A. Steer, et al., The LDBC Social Network Benchmark: Business Intelligence Workload, In PVLDB, pp. 877-890, 2022. [PDF](https://doi.org/10.14778/3574245.3574270)  
[5] J. K"arkk"ainen, D. Kempa, and S. J. Puglisi, Parallel External Memory Suffix Sorting, In CPM, pp. 329-342, 2015. [PDF](https://doi.org/10.1007/978-3-319-19929-0_28)
