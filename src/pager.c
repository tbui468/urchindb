#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#include "pager.h"
#include "util.h"

inline static uint32_t _pager_off_to_idx(uint32_t file_off) {
    return file_off / BLOCK_SIZE;
}

static bool _pager_block_is_stale(struct Block* meta, struct Block* b) {
    uint32_t block_meta_off = sizeof(uint32_t) * 2 * b->idx;
    uint32_t seconds = *((uint32_t*)(&meta->buf[block_meta_off]));
    uint32_t counter = *((uint32_t*)(&meta->buf[block_meta_off + sizeof(uint32_t)]));
    return seconds > b->timestamp.seconds || (seconds == b->timestamp.seconds && counter > b->timestamp.counter);
}

static struct Block* _pager_find_block(struct DB* db, uint32_t blockidx) {
    struct Block* cur = db->blocks;

    while (cur) {
        if (cur->idx == blockidx)
            break;

        cur = cur->next;
    }

    return cur;
}

inline static uint64_t _pager_file_size(FILE* f) {
    _fseek(f, 0, SEEK_END);
    return _ftell(f); 
}

static void _pager_write_from_block(struct DB* db, struct Block* b, struct TimeStamp ts) {
    uint32_t ts_off = b->idx * sizeof(uint32_t) * 2;
    *((uint32_t*)(&db->super->buf[ts_off])) = ts.seconds;
    *((uint32_t*)(&db->super->buf[ts_off + sizeof(uint32_t)])) = ts.counter;

    uint32_t to_end = _pager_file_size(db->idxf) - b->idx * BLOCK_SIZE;
    _fseek(db->idxf, b->idx * BLOCK_SIZE, SEEK_SET);
    _fwrite(b->buf, sizeof(char), to_end < BLOCK_SIZE ? to_end : BLOCK_SIZE, db->idxf);

    b->dirty = false;
    b->timestamp = ts;
}

static void _pager_read_into_block(struct DB* db, struct Block* b, uint32_t idx) {
    uint32_t to_end = _pager_file_size(db->idxf) - idx * BLOCK_SIZE;
    _fseek(db->idxf, idx * BLOCK_SIZE, SEEK_SET);
    _fread(b->buf, sizeof(char), to_end < BLOCK_SIZE ? to_end : BLOCK_SIZE, db->idxf);

    b->dirty = false;
    b->idx = idx;

    _fseek(db->idxf, SUPER_OFF + b->idx * sizeof(uint32_t) * 2, SEEK_SET);
    _fread((void*)&b->timestamp.seconds, sizeof(uint32_t), 1, db->idxf);
    _fread((void*)&b->timestamp.counter, sizeof(uint32_t), 1, db->idxf);
}

//uses least-recently used (LRU) eviction policy, and returns new block
//linked list is rearranged so that this block is now most-recently used,
//caller must set all fields with exception of 'next' field
static struct Block* _pager_new_block(struct DB* db) {
    struct Block* ret = db->blocks;
    db->blocks = ret->next;
    ret->next = NULL;

    struct Block* end = db->blocks;
    while (end->next) {
        end = end->next;
    }
    end->next = ret;

    return ret;
}

static struct TimeStamp _pager_new_stamp(struct TimeStamp prev) {
    struct TimeStamp new;

    new.seconds = time(NULL);
    new.counter = new.seconds == prev.seconds ? prev.counter + 1 : 0;

    return new;
}

//reload cache if stale
//evict block (writing to disk if data is dirty) if more cache space needed
//used by both pager_write and pager_read
//NOTE: file should be write-locked!  Otherwise metadata in super block could be invalid
static struct Block* _pager_prepare_block(struct DB* db, uint32_t idx) {
    struct Block* b = _pager_find_block(db, idx);
    if (b) {
        if (_pager_block_is_stale(db->super, b)) {
            _pager_read_into_block(db, b, idx);
        }
    } else {
        b = _pager_new_block(db);
        if (b->dirty) {
            struct TimeStamp ts = _pager_new_stamp(b->timestamp);
            _pager_write_from_block(db, b, ts);
            struct TimeStamp mts = _pager_new_stamp(db->super->timestamp); 
            _pager_write_from_block(db, db->super, mts);
        }
        _pager_read_into_block(db, b, idx);
    }

    return b;
}

void pager_write(struct DB* db, uint32_t file_off, char* buf, uint32_t len) {
    uint32_t idx_start = _pager_off_to_idx(file_off);
    uint32_t idx_end = _pager_off_to_idx(file_off + len - 1);

    uint32_t bytes_written = 0;
    for (int i = idx_start; i <= idx_end; i++) {
        struct Block* b = _pager_prepare_block(db, i);

        //block buffer offset to read from
        uint32_t block_left_off = b->idx * BLOCK_SIZE;
        uint32_t block_start = 0;
        if (file_off > block_left_off) {
            block_start = file_off - block_left_off;
        }

        //length to read
        uint32_t bytes_to_read = len - bytes_written < BLOCK_SIZE - block_start ? len - bytes_written : BLOCK_SIZE - block_start;

        memcpy(&b->buf[block_start], &buf[bytes_written], bytes_to_read);
        bytes_written += bytes_to_read;
        b->dirty = true;
    }
}

void pager_read(struct DB* db, uint32_t file_off, char* buf, uint32_t len) {
    uint32_t idx_start = _pager_off_to_idx(file_off);
    uint32_t idx_end = _pager_off_to_idx(file_off + len - 1);

    uint32_t bytes_written = 0;
    for (int i = idx_start; i <= idx_end; i++) {
        struct Block* b = _pager_prepare_block(db, i);

        //block buffer offset to read from
        uint32_t block_left_off = b->idx * BLOCK_SIZE;
        uint32_t block_start = 0;
        if (file_off > block_left_off) {
            block_start = file_off - block_left_off;
        }

        //length to read
        uint32_t bytes_to_read = len - bytes_written < BLOCK_SIZE - block_start ? len - bytes_written : BLOCK_SIZE - block_start;

        memcpy(&buf[bytes_written], &b->buf[block_start], bytes_to_read);
        bytes_written += bytes_to_read;
    }
}

void pager_commit_block(struct DB* db, struct Block* block) {
    struct TimeStamp ts = _pager_new_stamp(block->timestamp);
    _pager_write_from_block(db, block, ts);
}
