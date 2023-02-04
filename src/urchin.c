#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>

#include "urchin.h"
#include "util.h"
#include "pager.h"
#include "table.h"


static FILE* _db_open(const char* filename, bool fill) {
    FILE* f;
    if (!(f = fopen(filename, "r+"))) {
        f = _fopen(filename, "w");
        if (fill) {
            void* ptr;
            ptr = _calloc(RECORD_OFF, sizeof(uint8_t));
            _write_lock(f, SEEK_SET, 0, 0);
            _fwrite(ptr, sizeof(uint8_t), RECORD_OFF, f);
            _unlock(f, SEEK_SET, 0, 0);
            free(ptr);
        }

        fclose(f);

        f = _fopen(filename, "r+");
    }

    setbuf(f, NULL);

    return f;
}

struct DB* db_open(const char* dbname) {
    struct DB* db;
    db = _calloc(1, sizeof(struct DB));

    char filename[FILENAME_MAX];

    int len = strlen(dbname);
    memcpy(filename, dbname, len);

    memcpy(filename + len, ".idx", 4);
    filename[len + 4] = 0;
    db->idxf = _db_open(filename, true);
    _fseek(db->idxf, 0, SEEK_END);

    db->chain_off = FREELIST_OFF;
    db->idxrec_off = 0;

    struct Block* blocks = _calloc(BLOCKS_MAX, sizeof(struct Block));
    for (int i = 1; i < BLOCKS_MAX - 1; i++) {
        blocks[i].next = &blocks[i + 1];
    }
    blocks[BLOCKS_MAX - 1].next = NULL;
    db->blocks = &blocks[1];
    db->super = blocks;
    db->super->idx = 0;

    return db;
}

void db_close(struct DB* db) {
    fclose(db->idxf);
    free(db->super); //remaining blocks in contiguous memory should be freed too (right???)
}


//the table interface should be the same as that of the tree interface
int db_store(struct DB* db, const char* key, const char* data) {
    _write_lock(db->idxf, SEEK_SET, 0, 0);
    table_read_metadata(db);

    uint32_t rec_off;
    if ((rec_off = table_find_rec(db, key)) == 0) { //record with given key does not exist
        table_insert_rec(db, key, data);
    } else { //record with given key exists
        struct Record r = table_read_rec(db, rec_off);
        if (strlen(data) <= r.data_len) {
            r.data_len = strlen(data);
            table_write_rec(db, rec_off, r, key, data);
        } else {
            table_delete_rec(db, key);
            table_insert_rec(db, key, data);
        }
    }

    table_commit(db);
    _unlock(db->idxf, SEEK_SET, 0, 0);
    return 0;
}

void db_delete(struct DB* db, const char* key) {
    _write_lock(db->idxf, SEEK_SET, 0, 0);
    table_read_metadata(db);

    int res = table_delete_rec(db, key);

    table_commit(db);
    _unlock(db->idxf, SEEK_SET, 0, 0);
}

char* db_fetch(struct DB* db, const char* key) {
    _read_lock(db->idxf, SEEK_SET, 0, 0);
    table_read_metadata(db);

    uint32_t rec_off;
    char* data = NULL;
    if ((rec_off = table_find_rec(db, key)) != 0) {
        uint32_t key_size;
        uint32_t data_size;
        pager_read(db, rec_off + sizeof(uint32_t) , &key_size, sizeof(uint32_t));
        pager_read(db, rec_off + sizeof(uint32_t) * 2, &data_size, sizeof(uint32_t));

        data = _malloc(data_size + 1); //+1 for null terminator
        pager_read(db, rec_off + sizeof(uint32_t) * 3 + key_size, data, data_size);
        data[data_size] = '\0';
    }

    _unlock(db->idxf, SEEK_SET, 0, 0);
    return data;
}

void db_rewind(struct DB* db) {
    db->chain_off = FREELIST_OFF;
    db->idxrec_off = 0;
}

char* db_nextrec(struct DB* db) {
    if (!db->idxrec_off) {
        while (!db->idxrec_off && db->chain_off < RECORD_OFF) {
            db->chain_off += sizeof(uint32_t);
            pager_read(db, db->chain_off, &db->idxrec_off, sizeof(uint32_t));
        }
    }

    if (db->chain_off >= RECORD_OFF)
        return NULL;

    struct Record r = table_read_rec(db, db->idxrec_off);

    char* key = _malloc(r.key_len + 1);
    pager_read(db, db->idxrec_off + KEY_OFF, key, r.key_len);
    key[r.key_len] = '\0';
    db->idxrec_off = r.next_off;

    return key;
}

