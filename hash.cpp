#include <string.h>
#include <fstream>
#include "hash.h"
#include "merge.h"
#include "disk_io.h"
#include "compare.h"

using namespace std;

#define HASH_NUM 541

unsigned int hashFunction(record_t record, int field, unsigned int limit)
{
    if(field == NUM) hashWithNum(record, limit);
    else if(field == NUM_AND_STR) hashWithStrAndNum(record, limit);
    else if(field == RECID) hashWithId(record, limit);
    else if(field == STR) hashWithStr(record, limit);
}

unsigned int hashWithId(record_t record, unsigned int limit)
{
    return (record.recid*HASH_NUM) % limit;
}

unsigned int hashWithNum(record_t record, unsigned int limit)
{
    return (record.num*HASH_NUM) % limit;
}

unsigned int hashWithStr(record_t record, unsigned int limit)
{
    unsigned int str_sum = 0;

    for(int i = 0; i < strlen(record.str); i++)
    {
        str_sum += record.str[i];
    }

    return (str_sum*HASH_NUM) % limit;
}

unsigned int hashWithStrAndNum(record_t record, unsigned int limit)
{
    return (hashWithNum(record, limit) + hashWithStr(record, limit)) % limit;
}

unsigned int buildHashTable(std::ifstream& hash_rel_input, int field, unsigned int *capacities, block_t *buffer, unsigned int start, unsigned int memory, unsigned int *nios)
{
    /*
    * Build a hash table in buffer by hashing records and adding them to the proper
    * block. The first 2 blocks are not used in this process, as we use them later on
    * for input and output.
    */

    bool done = false;
    unsigned int hash_num;

    for(int i = 2; i < memory; i++) capacities[i] = 0;

    //If start != 0, that means we have stopped at a certain record of a block when building a previous hash table.
    //So we have to continue from that specific record. To do that, we seek 1 block back, and read again.
    if(start != 0) hash_rel_input.seekg(-sizeof(block_t), ios::cur);

    //Read a block
    readBlock(hash_rel_input, 0, true, buffer, 0);
    (*nios)++;

    //If the block read is not valid, read again
    while(!buffer[0].valid && !hash_rel_input.eof()) {
        readBlock(hash_rel_input, 0, true, buffer, 0);
        (*nios)++;
    }

    if(hash_rel_input.eof()) done = true;

    //Hash a valid record and put it in a memory block depending on the hash.
    while(!done)
    {
        for(unsigned int i = start; i < MAX_RECORDS_PER_BLOCK; i++)
        {
            if(buffer[0].entries[i].valid)
            {
                hash_num = hashFunction(buffer[0].entries[i], field, memory-2) + 2;
                buffer[hash_num].entries[capacities[hash_num]] = buffer[0].entries[i];
                capacities[hash_num]++;
                if(capacities[hash_num] == MAX_RECORDS_PER_BLOCK)
                {
                    //Return the current record index, so it will be passed as the start parameter
                    //in the next buildHashTable call.
                    return (i+1);
                }
            }
        }

        //Read another block
        readBlock(hash_rel_input, 0, true, buffer, 0);
        (*nios)++;

        //If we read a non valid block, read again.
        while(!buffer[0].valid && !hash_rel_input.eof()) {
            readBlock(hash_rel_input, 0, true, buffer, 0);
            (*nios)++;
        }

        //We are done with this block, so we set start to 0.
        start = 0;

        if(hash_rel_input.eof()) done = true;
    }

    return 0;
}

unsigned int nestedLoop(block_t *buffer, unsigned int *capacities, unsigned int output_index, char field, unsigned int memory, unsigned int *nres, unsigned int *nios, std::ofstream& output)
{
    /*
    * Probe a given block with a classic loop. For each record
    * in the given block, hash it and find all records that
    * satisfy the join condition.
    */
    unsigned int hash_num;
    if(output_index == 0) buffer[1].nreserved = 0;
    for(unsigned int i = 0; i < MAX_RECORDS_PER_BLOCK; i++)
    {
        if(buffer[0].entries[i].valid)
        {
            hash_num = hashFunction(buffer[0].entries[i], field, memory-2) + 2;
            for(unsigned int j = 0; j < capacities[hash_num]; j++)
            {
                //If we find a two same records (in terms to our field) we output the 1st record
                //and we set rdummy1 and rdummy2 to recid and num of record 2 respectively.
                if(compareRecords(&buffer[0].entries[i], &buffer[hash_num].entries[j], field) == 0)
                {
                    (*nres)++;
                    buffer[1].entries[output_index] = buffer[0].entries[i];
                    buffer[1].entries[output_index].rdummy1 = buffer[hash_num].entries[j].recid;
                    buffer[1].entries[output_index].rdummy2 = buffer[hash_num].entries[j].num;
                    output_index++;
                    if(output_index == MAX_RECORDS_PER_BLOCK)
                    {
                        buffer[1].valid = true;
                        buffer[1].nreserved = MAX_RECORDS_PER_BLOCK;
                        writeBlock(output, &buffer[1]);
                        (*nios)++;
                        output_index = 0;
                        buffer[1].nreserved = 0;
                    }
                }
            }
        }
    }

    //We return the index of the latest record in the output block. We only write when the output
    //block is full, so the next time this function is called we will continue adding records to the
    //output block from this point on.
    return output_index;
}
