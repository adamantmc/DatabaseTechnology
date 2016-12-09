#ifndef _DISK_IO_H
#define _DISK_IO_H

#include <fstream>
#include "dbtproj.h"

void readBlock(std::istream& input, unsigned int offset, bool seq, block_t *buffer, unsigned int spot);
void readMultipleBlocks(std::istream& input, block_t *buffer, unsigned int blocks);
void writeBlock(std::ofstream& output, block_t *block);
void init_block_id();

#endif
