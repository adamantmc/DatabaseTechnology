# DatabaseTechnology
Project for Database Technology class. Partially described in dbtproj.h.

We had to implement 4 algorithms that exist in a typical DBMS. These algorithms were:

1. MergeSort (external -  should work for much more data than the available memory)
2. MergeJoin
3. EliminateDuplicates
4. HashJoin

In MergeJoin we suppose that we have at least 4 blocks of available memory, and no more than 128 records can have the same key. That is because in MergeJoin for each record of file 1, we output all records of same key from file 2. If we reach the end of the current block from file 2, we gotta keep going to find more possible records that satisfy the join condition. This could keep on going forever, so we limit it by saying that we will only read one more block (maximum 128 records that satisfy the join condition - 128 is the default size for a block).

You can generate any data you like randomly, the only condition is the above. No more than 128 records (or block's max size) with the same key, or else the join algorithms will loop infinitely :).

Here are some benchmarks:

| Memory available in blocks |      Size of Input File 1 in blocks     |      Size of Input File 2 in blocks     | MergeSort for file 1 | MergeSort for file 2 | MergeJoin | Eliminate Duplicates for file 1 | HashJoin |
|:--------------------------:|:---------------------------------------:|:---------------------------------------:|:--------------------:|:--------------------:|:---------:|:------------------------------:|:--------:|
|             400            |        7664 Valid Records: 799137       |       16812 Valid Records: 1747161      | 2.6s                 | 6.1s                 | 9.3s      | 2.4s                           | 60.5s    |
|            4000            |                   Same                  |                   Same                  | 2.3s                 | 5.3s                 | 8s        | 2.5s                           | 6.6s     |
|            4000            | 239030 (1.60GB) Valid Records: 24796769 | 161413 (1.08GB) Valid Records: 16749615 | 215.6s               | 113.5s               | 337.6s    | 237.4s                         | 1946s    |
|            40000           |                   Same                  |                   Same                  | 158.8s               | 100.4s               | 288.5s    | 180.8s                         | 237.7s   |

Algorithm implementations in dbtproj.cpp. <br />
Functions related to hash join in hash.cpp. <br />
Functions related to merge join in merge.cpp. <br />
Functions to compare records based on a field in compare.cpp. <br />
Read and write operations in disk_io.cpp. <br />

To compile, you have to add -std=c++11 in your compiler's options/flags you name it. 
