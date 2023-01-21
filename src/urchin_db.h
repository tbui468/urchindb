#ifndef URCHIN_DB_H
#define URCHIN_DB_H

#include <stdint.h>
#define BUCKETS_MAX 1024

struct DB {
    FILE* datf;
    FILE* idxf;
    uint32_t chain_off;
    uint32_t idxrec_off;
};

struct IdxRec {
    uint32_t next_off;
    uint32_t data_off;
    uint32_t data_size;
    uint32_t key_size;
};

struct DB* db_open(const char* dbname);
void db_close(struct DB* db);
char* db_fetch(struct DB* db, const char* key);
void db_rewind(struct DB* db);
char* db_nextrec(struct DB* db);
int db_delete(struct DB* db, const char* key);
int db_store(struct DB* db, const char* key, const char* value);


void err_quit(const char* msg);

#endif //URCHIN_DB_H
