#include "merge.h"
#include "dbtproj.h"

#include <fstream>

//Only for generator
#include <cstdlib>
#include <ctime>
#include <iostream>

using namespace std;

unsigned int block_id;

void init_block_id() {
    block_id = 0;
}

void readBlock(std::istream& input, unsigned int offset, bool seq, block_t *buffer, unsigned int spot)
{
    //The seq boolean indicates whether we read sequentially or with an offset.
    if(!seq) {
        input.seekg(offset, input.beg);
    }

    input.read((char *) &buffer[spot], sizeof(block_t));
}

void readMultipleBlocks(std::istream& input, block_t *buffer, unsigned int blocks)
{
    input.read((char *) buffer, sizeof(block_t)*blocks);
}

void writeBlock(std::ofstream& output, block_t *block)
{
    block->blockid = block_id;
    block_id++;
    output.write((char *) block, sizeof(block_t));
}
