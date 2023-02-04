#include <stdbool.h>

#include "table.h"
#include "util.h"
#include "pager.h"


//FNV-1a hash function
static uint32_t _hash_key(const char* key) {
    int len = strlen(key);
    uint32_t hash = 2166136261u;
    for (int i = 0; i < len; i++) {
        hash ^= (uint8_t)key[i];
        hash *= 16777619;
    } 
    return hash;
}

struct Record table_read_rec(struct DB* db, uint32_t rec_off) {
    struct Record r;
    int len = sizeof(uint32_t) * 3;
    char buf[len];
    pager_read(db, rec_off, buf, len);

    r.next_off = *((uint32_t*)(buf));
    r.key_len  = *((uint32_t*)(buf + sizeof(uint32_t)));
    r.data_len = *((uint32_t*)(buf + sizeof(uint32_t) * 2));

    return r;
}

static uint32_t _table_get_free_rec(struct DB* db, uint32_t len) {
    uint32_t cur;
    pager_read(db, FREELIST_OFF, (char*)&cur, sizeof(uint32_t));
    uint32_t prev = FREELIST_OFF;

    //search freelist for empty record with large enough size to hold key and data
    while (cur) {
        struct Record r = table_read_rec(db, cur);

        if (r.data_len + r.key_len >= len) {
            //remove from freelist
            pager_write(db, prev, (char*)&r.next_off, sizeof(uint32_t));
            return cur; 
        }

        prev = cur;
        cur = r.next_off;
    }

    //no free record found - will append to end of file
    _fseek(db->idxf, 0, SEEK_END);
    uint64_t offset = _ftell(db->idxf);
    char buf[sizeof(uint32_t) * 3 + len]; //fill with junk so that file is proper size (probably not ideal)
    _fwrite(buf, sizeof(char), sizeof(uint32_t) * 3 + len, db->idxf);
    return offset;
}

void table_write_rec(struct DB* db, uint32_t rec_off, struct Record r, const char* key, const char* data) {
    uint32_t len = sizeof(uint32_t) * 3 + r.key_len + r.data_len;
    char buf[len];

    *((uint32_t*)buf) = r.next_off; 
    *((uint32_t*)(buf + sizeof(uint32_t))) = r.key_len;
    *((uint32_t*)(buf + sizeof(uint32_t) * 2)) = r.data_len;
    memcpy(buf + sizeof(uint32_t) * 3, key, r.key_len);
    memcpy(buf + sizeof(uint32_t) * 3 + r.key_len, data, r.data_len);
    pager_write(db, rec_off, buf, len);
}

void table_insert_rec(struct DB* db, const char* key, const char* data) {
    uint32_t chain_off = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    uint32_t head_off;
    pager_read(db, chain_off, &head_off, sizeof(uint32_t));

    struct Record new_rec;
    new_rec.next_off = head_off;
    new_rec.key_len = strlen(key);
    new_rec.data_len = strlen(data);

    uint32_t new_off = _table_get_free_rec(db, new_rec.key_len + new_rec.data_len);
    pager_write(db, chain_off, &new_off, sizeof(uint32_t));
    table_write_rec(db, new_off, new_rec, key, data);
}

int table_delete_rec(struct DB* db, const char* key) {
    uint32_t chain_off = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    uint32_t cur;
    pager_read(db, chain_off, &cur, sizeof(uint32_t));
    uint32_t prev = chain_off;

    while (cur) {
        struct Record r = table_read_rec(db, cur);
        char rec_key[r.key_len];
        pager_read(db, cur + KEY_OFF, rec_key, r.key_len);

        if (strncmp(rec_key, key, r.key_len) == 0) {
            //remove from chain
            pager_write(db, prev, (char*)&r.next_off, sizeof(uint32_t)); 
            //insert into freelist
            uint32_t next_free;
            pager_read(db, FREELIST_OFF, &next_free, sizeof(uint32_t));
            pager_write(db, cur, &next_free, sizeof(uint32_t));
            pager_write(db, FREELIST_OFF, &cur, sizeof(uint32_t));
            return 0;
        }

        prev = cur;
        cur = r.next_off;
    }

    return -1;
}

void table_read_metadata(struct DB* db) {
    _fseek(db->idxf, SUPER_OFF, SEEK_SET);
    _fread((void*)db->super->buf, sizeof(char), BLOCK_SIZE, db->idxf);
}

uint32_t table_find_rec(struct DB* db, const char* key) {
    uint32_t chain_off = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    uint32_t rec_off;
    pager_read(db, chain_off, &rec_off, sizeof(uint32_t));

    while (rec_off) {
        struct Record r = table_read_rec(db, rec_off);
        char rec_key[r.key_len];
        pager_read(db, rec_off + KEY_OFF, rec_key, r.key_len);

        if (strncmp(rec_key, key, r.key_len) == 0)
            return rec_off;
        rec_off = r.next_off;
    }

    return 0;
}

void table_commit(struct DB* db) {

    struct Block* cur = db->blocks;
    while (cur) {
        if (cur->dirty) {
            pager_commit_block(db, cur);
            /*testing*/
            //printf("block %u\n", cur->idx);
            //_fseek(db->idxf, 0, SEEK_END);
            //uint32_t max = _ftell(db->idxf) - cur->idx * BLOCK_SIZE;
            //_fseek(db->idxf, cur->idx * BLOCK_SIZE, SEEK_SET);
            //printf("commiting %u bytes from block %u\n", max < BLOCK_SIZE ? max : BLOCK_SIZE, cur->idx);
            /*end testing*/

            cur->dirty = false;
        }
        cur = cur->next;
    }

    pager_commit_block(db, db->super);
}
