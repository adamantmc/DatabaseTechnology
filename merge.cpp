#include <iostream>
#include <string.h>
#include <vector>
#include <queue>
#include <functional>

#include "dbtproj.h"
#include "merge.h"
#include "compare.h"
#include "disk_io.h"
using namespace std;

void sortBlock(block_t *block, int mode)
{
    if(mode == RECID) qsort(block->entries, MAX_RECORDS_PER_BLOCK, sizeof(record_t), compareRecords_recid);
    else if(mode == NUM) qsort(block->entries, MAX_RECORDS_PER_BLOCK, sizeof(record_t), compareRecords_num);
    else if(mode == STR) qsort(block->entries, MAX_RECORDS_PER_BLOCK, sizeof(record_t), compareRecords_str);
    else if(mode == NUM_AND_STR) qsort(block->entries, MAX_RECORDS_PER_BLOCK, sizeof(record_t), compareRecords_num_str);
}

unsigned int mergeBlocks(block_t *buffer, unsigned int blocks, char *outfile, int field, unsigned int *nios)
{
    std::priority_queue<block_t*, std::vector<block_t*>, std::function<bool(block_t*, block_t*)>> min_heap;

    //Add the proper compare function to our priority queue
    switch(field) {
        case NUM:
        min_heap = std::priority_queue<block_t*, std::vector<block_t*>, std::function<bool(block_t*, block_t*)>>(b_greater_num);
        break;
        case NUM_AND_STR:
        min_heap = std::priority_queue<block_t*, std::vector<block_t*>, std::function<bool(block_t*, block_t*)>>(b_greater_num_str);
        break;
        case RECID:
        min_heap = std::priority_queue<block_t*, std::vector<block_t*>, std::function<bool(block_t*, block_t*)>>(b_greater_id);
        break;
        case STR:
        min_heap = std::priority_queue<block_t*, std::vector<block_t*>, std::function<bool(block_t*, block_t*)>>(b_greater_str);
        break;
    }

    bool merging = true;
    unsigned int output_pointer = 0;
    unsigned int written_blocks = 0;

    std::ofstream output(outfile, std::ofstream::out | std::ofstream::binary | std::ofstream::app);

    //We add each block to the heap. The blocks are stored in the heap
    //according to the value of their first record. The bdummy1 variable
    //is used as a pointer to the next record of that block.
    for(unsigned int i = 0; i < blocks; i++) {
        if(buffer[i].valid) {
            buffer[i].bdummy1 = 0;
            min_heap.push(&buffer[i]);
        }
    }

    block_t *temp_block;

    //Each time we find the minimum record, by getting the block containing it from the heap.
    //After we find the minimum record, we write it to the output block and increment the output index.
    //If we haven't reached the end of the block containing the minimum record, then we put it back in
    //the heap after we increment the bdummy1 variable.
    while(merging)
    {
        //If heap is empty, then we have read all records from all blocks
        if(min_heap.empty()) {
            break;
        }

        temp_block = min_heap.top();
        min_heap.pop();

        if(temp_block->entries[temp_block->bdummy1].valid) {
            buffer[blocks].entries[output_pointer] = temp_block->entries[temp_block->bdummy1];
            output_pointer++;
        }

        //If output block is full, write it to output file
        if(output_pointer == MAX_RECORDS_PER_BLOCK)
        {
            buffer[blocks].valid = true;
            buffer[blocks].nreserved = MAX_RECORDS_PER_BLOCK;
            writeBlock(output, &buffer[blocks]);
            (*nios)++;
            output_pointer = 0;
            written_blocks++;
        }

        temp_block->bdummy1++;
        if(temp_block->bdummy1 != MAX_RECORDS_PER_BLOCK) {
            min_heap.push(temp_block);
        }

    }

    //If there is anything left in output buffer
    if(output_pointer != 0) {
        for(unsigned int i = output_pointer; i < MAX_RECORDS_PER_BLOCK; i++) {
            buffer[blocks].entries[i].valid = false;
        }
        buffer[blocks].valid = true;
        buffer[blocks].nreserved = output_pointer;
        writeBlock(output, &buffer[blocks]);
        (*nios)++;
        written_blocks++;
    }

    return written_blocks;
}

unsigned int mergeGroups(block_t *buffer, std::vector<unsigned int> groups, unsigned int start, unsigned int end, char *infile, char *outfile, int field, unsigned int *nios)
{
    unsigned int group_num = end - start + 1;

    //The start and end are the range of groups we will be merging.
    //The range is always the number of blocks available in memory, minus an output block.
    //So if the end parameter is bigger than the actual group number, we have to trim it.
    if(end >= groups.size())
    {
        end = groups.size() - 1;
        group_num = end - start + 1;
    }

    //Each group_pointers array points to a block from a certain group. This way we keep track of how many blocks we have read from a group.
    unsigned int *group_pointers = new unsigned int[groups.size()];
    //Contains the offsets at which each group begins.
    unsigned int *group_offsets = new unsigned int[groups.size()];
    //Contains the pointers to records of the blocks in memory.
    unsigned int *block_rec_pointers = new unsigned[group_num+1];
    unsigned int blocks = 0, min_rec_block, written_blocks = 0;

    //Indicates whether a buffer slot is used
    bool *occupied = new bool[group_num];
    bool done = false;
    bool merging;

    std::ifstream input(infile, std::ifstream::binary | std::ifstream::in);
    std::ofstream output(outfile, std::ofstream::binary | std::ofstream::out | std::ofstream::app);

    //Intialize occupied and block_rec_pointers array
    for(int i = 0; i < group_num; i++)
    {
        occupied[i] = false;
        block_rec_pointers[i] = 0;
    }

    block_rec_pointers[group_num] = 0;

    for(int i = 0; i < groups.size(); i++)
    {
        group_pointers[i] = 0;
        if(i!=0) group_offsets[i] = groups.at(i-1) * sizeof(block_t) + group_offsets[i-1];
        else group_offsets[i] = 0;
    }

    //Number of blocks read at each iteration
    blocks = 0;

    //This time we merge blocks like the mergeBlocks function but when we are done with a block
    //we read another one from the specified offset. This happens because the sorted groups are
    //all in the same file so each time we seek to a certain offset to read the next block in the group.
    while(!done)
    {
        //Read a block into an unoccupied slot
        for(int i = 0; i < group_num; i++)
        {
            if(!occupied[i] && group_pointers[i+start] < groups.at(i+start))
            {
                readBlock(input, group_offsets[i+start] + sizeof(block_t) * group_pointers[i+start], false, buffer, i);
                (*nios)++;
                group_pointers[i+start]++;
                blocks++;
                occupied[i] = true;
                block_rec_pointers[i] = 0;
            }
        }

        //If we have not read any blocks
        if(blocks == 0) done = true;

        else
        {
            merging = true;

            while(merging)
            {
                min_rec_block = -1;

                //Find min block
                for(unsigned int i = 0; i < group_num; i++)
                {
                    if(occupied[i] && block_rec_pointers[i] != MAX_RECORDS_PER_BLOCK)
                    {
                        if(min_rec_block == -1) min_rec_block = i;
                        else
                        {
                            //Compare the two records based on field
                            int cmp = compareRecords(&buffer[min_rec_block].entries[block_rec_pointers[min_rec_block]], &buffer[i].entries[block_rec_pointers[i]], field);
                            if(cmp == 1) min_rec_block = i;
                        }
                    }
                }

                if(min_rec_block != -1)
                {
                    //Write to output block
                    if(buffer[min_rec_block].entries[block_rec_pointers[min_rec_block]].valid)
                    {
                        buffer[group_num].nreserved++;
                        buffer[group_num].entries[block_rec_pointers[group_num]] = buffer[min_rec_block].entries[block_rec_pointers[min_rec_block]];
                        block_rec_pointers[group_num]++;
                    }

                    //If output block is full, write it to output file
                    if(block_rec_pointers[group_num] == MAX_RECORDS_PER_BLOCK)
                    {
                        buffer[group_num].valid = true;
                        buffer[group_num].nreserved = MAX_RECORDS_PER_BLOCK;
                        writeBlock(output, &buffer[group_num]);
                        (*nios)++;
                        buffer[group_num].nreserved = 0;
                        block_rec_pointers[group_num] = 0;
                        written_blocks++;
                    }

                    block_rec_pointers[min_rec_block]++;
                }

                for(int i = 0; i < group_num; i++)
                    if(block_rec_pointers[i] == MAX_RECORDS_PER_BLOCK && occupied[i])
                    {
                        occupied[i] = false;
                        blocks--;
                        merging = false;
                    }
            }
        }
    }

    if(block_rec_pointers[group_num] != 0) {
        for(unsigned int i = block_rec_pointers[group_num]; i < MAX_RECORDS_PER_BLOCK; i++) {
            buffer[group_num].entries[i].valid = false;
        }
        buffer[group_num].valid = true;
        buffer[group_num].nreserved = block_rec_pointers[group_num];
        writeBlock(output, &buffer[group_num]);
        (*nios)++;
        written_blocks++;
    }

    input.close();
    output.close();

    return written_blocks;
    //cout << "Finished. ";
    //printArray(occupied, group_num);
}
