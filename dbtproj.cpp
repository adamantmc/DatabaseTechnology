#include "dbtproj.h"
#include "merge.h"
#include "disk_io.h"
#include "hash.h"
#include "compare.h"
#include <fstream>

using namespace std;


void MergeSort (char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nsorted_segs, unsigned int *npasses, unsigned int *nios)
{
    char *tempfile = "mergesort_tempfile.bin";

    std::ifstream input(infile, std::ifstream::binary | std::ifstream::in | std::ifstream::ate);
    std::ofstream output(outfile, std::ofstream::binary | std::ofstream::out);
    std::vector<unsigned int> groups, new_groups;

    std::fstream::pos_type file_size = input.tellg();
    input.seekg(0, ios::beg);

    unsigned int block_num = file_size / sizeof(block_t);
    unsigned int sorted_groups = 0;
    unsigned int written_blocks;
    unsigned int blocks_left = block_num;
    unsigned int blocks_read;

    bool sorted = false;
    bool trigger = true;

    /*
    * 1st pass. While there are blocks left in the input file,
    * read up to nmem_blocks - 1, sort and merge them, and write.
    * If a non-valid block is read, do not sort it, it will be ignored
    * in the mergeBlocks function.
    */
    init_block_id();
    while(blocks_left != 0)
    {
        if(nmem_blocks-1 > blocks_left)
        {
            readMultipleBlocks(input, buffer, blocks_left);
            blocks_read = blocks_left;
            blocks_left = 0;
            (*nios)++;
        }
        else
        {
            readMultipleBlocks(input, buffer, nmem_blocks-1);
            blocks_read = nmem_blocks-1;
            blocks_left -= nmem_blocks-1;
            (*nios)++;
        }

        for(int i = 0; i < blocks_read; i++)
        {
            if(buffer[i].valid) sortBlock(&buffer[i], field);
        }

        written_blocks = mergeBlocks(buffer, blocks_read, outfile, field, nios);
        groups.push_back(written_blocks);
    }

    (*nsorted_segs) += groups.size();
    (*npasses)++;

    input.close();
    output.close();

    if(groups.size() == 1) sorted = true;

    /*
    * Merge groups until only one group remains.
    * Each time all the groups are merged (each pass) we flip the input and output files.
    * The groups vector is updated with the new_groups vector, which contains the new groups.
    * The new_groups vector is filled with the number returned by mergeGroups, which is the
    * group's capacity.
    */
    while(!sorted)
    {
        init_block_id();
        if(trigger)
        {
            std::ofstream output(tempfile, std::ofstream::binary | std::ofstream::out | std::ofstream::trunc);
            output.close();
        }
        else
        {
            std::ofstream output(outfile, std::ofstream::binary | std::ofstream::out | std::ofstream::trunc);
            output.close();
        }

        while(sorted_groups < groups.size())
        {
            if(trigger) written_blocks = mergeGroups(buffer, groups, sorted_groups, nmem_blocks-2+sorted_groups, outfile, tempfile, field, nios);
            else written_blocks = mergeGroups(buffer, groups, sorted_groups, nmem_blocks-2+sorted_groups, tempfile, outfile, field, nios);
            new_groups.push_back(written_blocks);
            sorted_groups += nmem_blocks - 1;
        }

        groups = new_groups;
        new_groups.clear();

        (*nsorted_segs) += groups.size();

        sorted_groups = 0;
        trigger = !trigger;
        if(groups.size() == 1) sorted = true;

        (*npasses)++;
    }

    //Remove outfile and rename tempfile to outfile, or remove tempfile.

    if(!trigger)
    {
        remove(outfile);
        rename(tempfile, outfile);
    }
    else
    {
        remove(tempfile);
    }
}

void MergeJoin (char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios)
{
    bool done = false, same = false, readNextBlock = false, read_1 = true, read_2 = true;
    //Indexes to the buffer
    unsigned int in1 = 0, in2 = 1, output_block = 2, temp = 3;
    unsigned int rec_ptr_1 = 0, rec_ptr_2 = 0, rec_ptr_out = 0, join_ptr = 0;
    unsigned int nsorted_segs = 0, npasses = 0;
    int result;

    MergeSort(infile1,field,buffer,nmem_blocks,"mergejoin_temp_output1.bin",&nsorted_segs,&npasses,nios);
    MergeSort(infile2,field,buffer,nmem_blocks,"mergejoin_temp_output2.bin",&nsorted_segs,&npasses,nios);

    std::ifstream input1("mergejoin_temp_output1.bin", std::ifstream::binary | std::ifstream::in);
    std::ifstream input2("mergejoin_temp_output2.bin", std::ifstream::binary | std::ifstream::in);
    std::ofstream output(outfile, std::ofstream::binary | std::ofstream::out);

    init_block_id();

    /*
    * Read records, compare and update pointer depending on result.
    * If the records are equal in terms of the join field, then output
    * n*m records, where n is the number of records with same join field from
    * input 1, and m the number of records with same join field from input 2.
    */
    while(!done)
    {
        if(read_1)
        {
            read_1 = false;
            readBlock(input1, 0, true, buffer, in1);
            (*nios)++;
            if(input1.eof())
            {
                break;
            }
        }

        if(read_2)
        {
            read_2 = false;
            readBlock(input2, 0, true, buffer, in2);
            (*nios)++;
            if(input2.eof())
            {
                break;
            }
        }

        //Reaching a non valid record means we have used all valid records
        if(!buffer[in1].entries[rec_ptr_1].valid || !buffer[in2].entries[rec_ptr_2].valid)
        {
            break;
        }

        result = compareRecords(&(buffer[in1].entries[rec_ptr_1]), &(buffer[in2].entries[rec_ptr_2]), field);

        if(result == -1)
        {
            rec_ptr_1++;
        }
        else if(result == 1)
        {
            rec_ptr_2++;
        }
        else if(result == 0)
        {
            readNextBlock = false;

            join_ptr = rec_ptr_2;
            same = true;

            while(same)
            {
                //Output all records that satisfy the join condition.
                 do {
                    (*nres)++;
                    buffer[output_block].entries[rec_ptr_out] = buffer[in1].entries[rec_ptr_1];
                    buffer[output_block].entries[rec_ptr_out].rdummy1 = buffer[in2].entries[rec_ptr_2].recid;
                    buffer[output_block].entries[rec_ptr_out].rdummy2 = buffer[in2].entries[rec_ptr_2].num;

                    rec_ptr_2++;
                    rec_ptr_out++;

                    if(rec_ptr_out == MAX_RECORDS_PER_BLOCK)
                    {
                        buffer[output_block].valid = true;
                        buffer[output_block].nreserved = MAX_RECORDS_PER_BLOCK;
                        writeBlock(output, &buffer[output_block]);
                        (*nios)++;
                        rec_ptr_out = 0;
                    }

                    if(rec_ptr_2 == MAX_RECORDS_PER_BLOCK || !buffer[in2].entries[rec_ptr_2].valid) break;
                } while(compareRecords(&(buffer[in1].entries[rec_ptr_1]), &(buffer[in2].entries[rec_ptr_2]), field) == 0);

                //If we reached the end of the block of the second input, then we must read the next one.
                //We keep the previous block because we may need to roll back to it.
                if(rec_ptr_2 == MAX_RECORDS_PER_BLOCK) {
                    if(!readNextBlock) {
                        buffer[temp] = buffer[in2];
                        readBlock(input2, 0, true, buffer, in2);
                        (*nios)++;

                        if(input2.eof())
                        {
                            buffer[in2] = buffer[temp];
                        }

                        else
                        {
                            readNextBlock = true;
                            rec_ptr_2 = 0;
                        }
                    }
                    //If we have already read the next block, then we have gone to the next record from in2.
                    //We don't want to re-read any block, so we just switch the two pointers. Remember, the
                    //block we read previously was stored in in2, and the older block is stored in temp. Right now
                    //the 2 pointers are switched, meaning that in2 = 3 and temp = 1. So we just probe'd the older block.
                    //Now we switch again to probe the next block as well. We are actually doing the same process as reading
                    //the next block, without reading it, as we have already read it before.
                    else {
                        unsigned int foo = in2;
                        in2 = temp;
                        temp = foo;
                        rec_ptr_2 = 0;
                    }
                }

                //If we have read another block, we check for records from that block that satisfy the join condition as well
                if(readNextBlock) {
                    while(compareRecords(&(buffer[in1].entries[rec_ptr_1]), &(buffer[in2].entries[rec_ptr_2]), field) == 0) {
                        (*nres)++;
                        buffer[output_block].entries[rec_ptr_out] = buffer[in1].entries[rec_ptr_1];
                        buffer[output_block].entries[rec_ptr_out].rdummy1 = buffer[in2].entries[rec_ptr_2].recid;
                        buffer[output_block].entries[rec_ptr_out].rdummy2 = buffer[in2].entries[rec_ptr_2].num;

                        rec_ptr_2++;
                        rec_ptr_out++;

                        if(rec_ptr_out == MAX_RECORDS_PER_BLOCK)
                        {
                            buffer[output_block].valid = true;
                            buffer[output_block].nreserved = MAX_RECORDS_PER_BLOCK;
                            writeBlock(output, &buffer[output_block]);
                            (*nios)++;
                            rec_ptr_out = 0;
                        }

                        if(rec_ptr_2 == MAX_RECORDS_PER_BLOCK || !buffer[in2].entries[rec_ptr_2].valid) break;
                    }
                }

                //We are done with the current record of the 1st block. Moving on to next one.
                if(rec_ptr_1 == MAX_RECORDS_PER_BLOCK-1) {
                    readBlock(input1, 0, true, buffer, in1);
                    (*nios)++;
                    if(input1.eof()) break;
                    rec_ptr_1 = 0;
                }
                else rec_ptr_1++;

                //If we have read the next block, then if we want to roll back we must go to the block we stored in temp.
                //To do that we just switch the 2 pointers so in2 points to the previous block, and temp points to the next block.
                //This switch happens only if the next record in in1 (from input file 1) satisfies the join condition with the first record from the
                //second input file that triggered it.
                if(readNextBlock) {
                    if(compareRecords(&buffer[in1].entries[rec_ptr_1], &buffer[temp].entries[join_ptr], field) == 0) {
                        //Rolling back
                        unsigned int foo = temp;
                        temp = in2;
                        in2 = foo;
                        rec_ptr_2 = join_ptr;
                    }
                    else same = false;
                }
                else {
                    if(compareRecords(&buffer[in1].entries[rec_ptr_1], &buffer[in2].entries[join_ptr], field) == 0) {
                        //Rolling back. We have not read another block, so we just set rec_ptr_2 to the index of
                        //the record that triggered the joining process.
                        rec_ptr_2 = join_ptr;
                    }
                    else same = false;
                }
            }

        }


        if(rec_ptr_1 == MAX_RECORDS_PER_BLOCK)
        {
            read_1 = true;
            rec_ptr_1 = 0;
        }
        if(rec_ptr_2 == MAX_RECORDS_PER_BLOCK)
        {
            read_2 = true;
            rec_ptr_2 = 0;
        }

        if(rec_ptr_out == MAX_RECORDS_PER_BLOCK)
        {
            buffer[output_block].valid = true;
            buffer[output_block].nreserved = MAX_RECORDS_PER_BLOCK;
            writeBlock(output, &buffer[output_block]);
            rec_ptr_out = 0;
        }
    }

    if(rec_ptr_out != 0)
    {
        for(int i = rec_ptr_out; i < MAX_RECORDS_PER_BLOCK; i++)
        {
            buffer[output_block].entries[i].valid = false;
        }
        buffer[output_block].valid = true;
        buffer[output_block].nreserved = rec_ptr_out;
        writeBlock(output, &buffer[output_block]);
        (*nios)++;
    }

    output.close();
    input1.close();
    input2.close();

    remove("mergejoin_temp_output1.bin");
    remove("mergejoin_temp_output2.bin");
}

void EliminateDuplicates(char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nunique, unsigned int *nios)
{
    unsigned int nsorted_segs = 0, npasses = 0;
    unsigned int output_pointer = 0;
    unsigned int output_block = nmem_blocks - 1;
    unsigned int curr_block, blocks_read;
    unsigned int block_num;

    bool write = false, read = true;
    int result;

    MergeSort(infile,field,buffer,nmem_blocks,"el_d_temp_output.bin",&nsorted_segs,&npasses,nios);

    init_block_id();

    std::ifstream input("el_d_temp_output.bin", std::ifstream::binary | std::ifstream::in | std::ifstream::ate);
    block_num = input.tellg() / sizeof(block_t);
    input.seekg(0, ios::beg);
    std::ofstream output(outfile, std::ofstream::binary | std::ofstream::out);

    /*
    * While there are still unread blocks, do the following.
    * If the output block is empty, add the read record from the relation.
    * Then for all the other records, if they are not the equal (in terms to our field)
    * to the last record that was added to the output block, add them, else ignore them.
    */
    while(block_num != 0)
    {
        //Read up to nmem_blocks-1 blocks.
        if(read)
        {
            if(block_num > nmem_blocks - 1)
            {
                readMultipleBlocks(input, buffer, nmem_blocks-1);
                blocks_read = nmem_blocks-1;
                block_num -= nmem_blocks-1;
            }
            else
            {
                readMultipleBlocks(input, buffer, block_num);
                blocks_read = block_num;
                block_num = 0;
            }

            (*nios)++;
            curr_block = 0;
            read = false;
        }

        //For each block read
        for(unsigned int curr_block = 0; curr_block < blocks_read; curr_block++)
        {
            //Loop over the records
            for(int i = 0; i < MAX_RECORDS_PER_BLOCK && !read; i++)
            {
                if(buffer[curr_block].entries[i].valid)
                {
                    //If the output pointer is zero, add immediately. There is nothing else to check for.
                    if(output_pointer == 0)
                    {
                        buffer[output_block].entries[output_pointer] = buffer[curr_block].entries[i];
                        (*nunique)++;
                        output_pointer++;
                    }
                    //Else check it against the first record in the output block. If they are not equal, add it.
                    else
                    {
                        result = compareRecords(&buffer[curr_block].entries[i], &buffer[output_block].entries[output_pointer-1], field);
                        if(result != 0)
                        {
                            if(output_pointer == MAX_RECORDS_PER_BLOCK)
                            {
                                buffer[output_block].valid = true;
                                buffer[output_block].nreserved = MAX_RECORDS_PER_BLOCK;
                                writeBlock(output, &buffer[output_block]);
                                (*nios)++;
                                output_pointer = 0;
                            }
                            buffer[output_block].entries[output_pointer] = buffer[curr_block].entries[i];
                            (*nunique)++;
                            output_pointer++;
                        }
                    }
                }
            }
        }

        read = true;
    }

    if(output_pointer != 0)
    {
        for(int i = output_pointer; i < MAX_RECORDS_PER_BLOCK; i++) buffer[output_block].entries[i].valid = false;
        buffer[output_block].valid = true;
        buffer[output_block].nreserved = output_pointer;
        writeBlock(output, &buffer[output_block]);
        (*nios)++;
    }

    input.close();
    output.close();

    remove("el_d_temp_output.bin");
}

void HashJoin(char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios)
{

    unsigned int hash_num, output_index = 0, start = 0;
    unsigned int *hash_capacities = new unsigned int[nmem_blocks-1];
    bool file_switch = true, done = false;

    int count = 0;
    std::ifstream input;
    std::ifstream hash_rel_input;

    std::ifstream input1(infile1, std::ifstream::in | std::ifstream::binary | std::ifstream::ate);
    std::fstream::pos_type file_size1 = input1.tellg();
    input1.close();

    std::ifstream input2(infile2, std::ifstream::in | std::ifstream::binary | std::ifstream::ate);
    std::fstream::pos_type file_size2 = input2.tellg();
    input2.close();

    std::ofstream output(outfile, std::ofstream::out | std::ofstream::binary);

    if(file_size1 >= file_size2)
    {
        file_switch = false;
    }

    if(file_switch)
    {
        input.open(infile2, std::ifstream::in | std::ifstream::binary);
        hash_rel_input.open(infile1, std::ifstream::in | std::ifstream::binary);
    }
    else
    {
        input.open(infile1, std::ifstream::in | std::ifstream::binary);
        hash_rel_input.open(infile2, std::ifstream::in | std::ifstream::binary);
    }

    //Hash rel input is the smallest of the two relations, and input is the other one.
    init_block_id();
    while(!done)
    {
        count++;

        //Build hash table on memory, and return the record on which we stopped.
        //The building process stops when a block is filled with records.

        start = buildHashTable(hash_rel_input, field, hash_capacities, buffer, start, nmem_blocks, nios);
        if(start == MAX_RECORDS_PER_BLOCK) start = 0;

        //Probe the other relation
        while(!input.eof())
        {
            readBlock(input, 0, true, buffer, 0);
            (*nios)++;

            //Read until we reach a valid block.
            while(!buffer[0].valid && !input.eof())
            {
                readBlock(input, 0, true, buffer, 0);
                (*nios)++;
            }


            if(input.eof()) break;

            //The fist empty record of the output block.
            output_index = nestedLoop(buffer, hash_capacities, output_index, field, nmem_blocks, nres, nios, output);
        }

        if(input.eof())
        {
            input.clear();
            input.seekg(0, ios::beg);
        }

        if(hash_rel_input.eof()) done = true;
    }

    //If the output index is not zero, then the output block has some records that weren't written.
    if(output_index != 0)
    {
        for(unsigned int i = output_index; i < MAX_RECORDS_PER_BLOCK; i++) buffer[1].entries[i].valid = false;
        buffer[1].valid = true;
        buffer[1].nreserved = output_index;
        writeBlock(output, &buffer[1]);
        (*nios)++;
    }

    input.close();
    hash_rel_input.close();
    output.close();
}
