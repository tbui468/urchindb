#ifndef UDB_TABLE_H
#define UDB_TABLE_H

#include "urchin.h"

struct Record {
    uint32_t next_off;
    uint32_t key_len;
    uint32_t data_len;
};

struct Record table_read_rec(struct DB* db, uint32_t rec_off);
void table_write_rec(struct DB* db, uint32_t rec_off, struct Record r, const char* key, const char* data);
void table_insert_rec(struct DB* db, const char* key, const char* data);
int table_delete_rec(struct DB* db, const char* key);
void table_read_metadata(struct DB* db);
uint32_t table_find_rec(struct DB* db, const char* key);
void table_commit(struct DB* db);

#endif //UDB_TABLE_H
