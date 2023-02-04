#ifndef UDB_PAGER_H
#define UDB_PAGER_H

#include "urchin.h"
#define BLOCK_SIZE 4096
#define SUPER_OFF 0
#define SUPER_SIZE BLOCK_SIZE
#define BLOCKS_MAX BLOCK_SIZE / (sizeof(uint32_t) * 4)
#define BUCKETS_MAX 1024
#define FREELIST_OFF SUPER_SIZE
#define HASHTAB_OFF sizeof(uint32_t) + SUPER_SIZE //first 4 bytes is freelist
#define RECORD_OFF HASHTAB_OFF + sizeof(uint32_t) * BUCKETS_MAX
#define KEY_OFF sizeof(uint32_t) * 3

struct TimeStamp {
    uint32_t seconds;
    uint32_t counter;
};

struct Block {
    struct Block* next;
    char buf[BLOCK_SIZE];
    uint32_t idx;
    struct TimeStamp timestamp;
    bool dirty;
};

void pager_write(struct DB* db, uint32_t file_off, char* buf, uint32_t len);
void pager_read(struct DB* db, uint32_t file_off, char* buf, uint32_t len);
void pager_commit_block(struct DB* db, struct Block* block);

#endif //UDB_PAGER_H
