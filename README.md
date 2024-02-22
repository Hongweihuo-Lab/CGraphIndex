# CGraphIndex
CGraphIndex is an entropy-compressed self-index for labeled property multidigraphs [1]. The space usage for CGraphIndex achieves the kth-order entropy space bound for multidigraph properties (the dominant term in practice) and the 1st-order entropy space bound for multidigraph structures. A self-index actually encodes the original input and thus there is no need to store the input separately. CGraphIndex supports fundamental and navigational operations on the structures and on the properties in constant time, supports substring extraction in near-optimal time, and efficiently supports PageRank, k-hop friends query, and typical interactive complex queries [2] and business intelligence queries [3].

## Paper
[1] H. Huo, Z. He, and J.S. Vitter,  Indexing Labeled Property Multidigraphs in Entropy Space, with Applications,  June 13, 2023.  [PDF]     

References
## References
[2] O. Erling, A. Averbuch, J. Larriba-Pey, et. al., The LDBC Social Network Benchmark: Interactive Workload, in SIGMOD, pp. 619--630, 2015. [PDF](https://doi.org/10.1145/2723372.2742786)     
[3] G. Szarnyas, J. Waudby, B.A. Steer, et. al., The LDBC Social Network Benchmark: Business Intelligence Workload, in PVLDB, pp. 877--890, 2022. [PDF](https://doi.org/10.14778/3574245.3574270)
