#include "dbtproj.h"
#include "compare.h"
#include <string.h>

int compareRecords(const void* r1, const void* r2, int field)
{
    if(field == RECID) return compareRecords_recid(r1, r2);
    else if(field == NUM) return compareRecords_num(r1, r2);
    else if(field == STR) return compareRecords_str(r1, r2);
    else if(field == NUM_AND_STR) return compareRecords_num_str(r1, r2);
}

int compareRecords_recid(const void* r1, const void* r2)
{
    if(((record_t*) r1)->recid < ((record_t*) r2)->recid) return -1;
    if(((record_t*) r1)->recid == ((record_t*) r2)->recid) return 0;
    return 1;
}

int compareRecords_num(const void* r1, const void* r2)
{
    if(((record_t*) r1)->num < ((record_t*) r2)->num) return -1;
    if(((record_t*) r1)->num == ((record_t*) r2)->num) return 0;
    return 1;
}

int compareRecords_str(const void* r1, const void* r2)
{
    int result = strcmp(((record_t*) r1)->str,((record_t*) r2)->str);
    if(result < 0) return -1;
    else if(result > 0) return 1;
    else return 0;
}

int compareRecords_num_str(const void* r1, const void* r2)
{
    if(((record_t*) r1)->num < ((record_t*) r2)->num) return -1;
    if(((record_t*) r1)->num == ((record_t*) r2)->num) return compareRecords_str(r1,r2);
    return 1;
}

bool b_greater_num(block_t *b1, block_t *b2) {
    if(compareRecords(&b1->entries[b1->bdummy1],&b2->entries[b2->bdummy1], NUM) == 1) return true;
    return false;
}

bool b_greater_num_str(block_t *b1, block_t *b2) {
    if(compareRecords(&b1->entries[b1->bdummy1],&b2->entries[b2->bdummy1], NUM_AND_STR) == 1) return true;
    return false;
}

bool b_greater_id(block_t *b1, block_t *b2) {
    if(compareRecords(&b1->entries[b1->bdummy1],&b2->entries[b2->bdummy1], RECID) == 1) return true;
    return false;
}

bool b_greater_str(block_t *b1, block_t *b2) {
    if(compareRecords(&b1->entries[b1->bdummy1],&b2->entries[b2->bdummy1], STR) == 1) return true;
    return false;
}

