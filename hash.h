#ifndef _HASH_H
#define _HASH_H

#include <iostream>
#include "dbtproj.h"

unsigned int hashFunction(record_t record, int field, unsigned int limit);

unsigned int hashWithId(record_t record, unsigned int limit);
unsigned int hashWithNum(record_t record, unsigned int limit);
unsigned int hashWithStr(record_t record, unsigned int limit);
unsigned int hashWithStrAndNum(record_t record, unsigned int limit);

unsigned int buildHashTable(std::ifstream& hash_rel_input, int field, unsigned int *capacities, block_t *buffer, unsigned int start, unsigned int memory, unsigned int *nios);

unsigned int nestedLoop(block_t *buffer, unsigned int *capacities, unsigned int output_index, char field, unsigned int memory, unsigned int *nres, unsigned int *nios, std::ofstream& output);

#endif
