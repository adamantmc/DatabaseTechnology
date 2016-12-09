#ifndef _MERGE_H
#define _MERGE_H

#include "dbtproj.h"
#include <vector>

void sortBlock(block_t *block, int mode);

unsigned int mergeGroups(block_t *buffer, std::vector<unsigned int> groups, unsigned int start, unsigned int end, char *infile, char *outfile, int field, unsigned int *nios);
unsigned int mergeBlocks(block_t *buffer, unsigned int blocks, char *outfile, int field, unsigned int *nios);

#endif
