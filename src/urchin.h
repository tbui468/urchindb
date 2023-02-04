#ifndef UDB_URCHIN_H
#define UDB_URCHIN_H

#include <stdint.h>
#include <stdio.h>

struct DB {
    FILE* idxf;
    uint32_t chain_off;
    uint32_t idxrec_off;
    struct Block* blocks;
    struct Block* super;
};

struct DB* db_open(const char* dbname);
void db_close(struct DB* db);
char* db_fetch(struct DB* db, const char* key);
void db_rewind(struct DB* db);
char* db_nextrec(struct DB* db);
void db_delete(struct DB* db, const char* key);
int db_store(struct DB* db, const char* key, const char* value);


void err_quit(const char* msg);

#endif //UDB_URCHIN_H
