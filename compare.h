#ifndef _CMP_H
#define _CMP_H

#define RECID 0
#define NUM 1
#define STR 2
#define NUM_AND_STR 3

int compareRecords(const void* r1, const void* r2, int field);
int compareRecords_recid(const void* r1, const void* r2);
int compareRecords_num(const void* r1, const void* r2);
int compareRecords_str(const void* r1, const void* r2);
int compareRecords_num_str(const void* r1, const void* r2);

bool b_greater_num(block_t *b1, block_t *b2);
bool b_greater_num_str(block_t *b1, block_t *b2);
bool b_greater_id(block_t *b1, block_t *b2);
bool b_greater_str(block_t *b1, block_t *b2);

#endif
