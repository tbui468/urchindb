#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>

#define BLOCK_SIZE 4096
#define BLOCKS_MAX BLOCK_SIZE / 16
#define SUPER_OFF 0
#define BUCKETS_MAX 4
#define FREELIST_OFF 0 + BLOCK_SIZE
#define HASHTAB_OFF sizeof(uint32_t) + BLOCK_SIZE //first 4 bytes is freelist
#define RECORD_OFF HASHTAB_OFF + sizeof(uint32_t) * BUCKETS_MAX
#define KEY_OFF sizeof(uint32_t) * 3


static struct TimeStamp {
    uint64_t seconds;
    uint64_t counter;
};

struct Block {
    struct Block* next;
    char buf[BLOCK_SIZE];
    uint32_t idx;
    struct TimeStamp timestamp;
    bool dirty;
};

struct DB {
    FILE* idxf;
    uint32_t chain_off;
    uint32_t idxrec_off;
    struct Block* blocks;
    //TODO: add a pointer to tail of blocks too so most-recently accessed block can be inserted quickly to end of linked list
    struct Block* super;
};

struct Record {
    uint32_t next_off;
    uint32_t key_len;
    uint32_t data_len;
};

void err_quit(const char* msg) {
    perror(msg);
    exit(1);
}

static int _fileno(FILE* f) {
    int res;
    if ((res = fileno(f)) < 0)
        err_quit("fileno failed");
    return res;
}

static int _read_lock(FILE* f, short whence, off_t start, off_t len) {
    struct flock fl;
    fl.l_type = F_RDLCK;
    fl.l_whence = whence;
    fl.l_start = start;
    fl.l_len = len;

    int res;
    if ((res = fcntl(_fileno(f), F_SETLKW, &fl)) < 0)
        err_quit("fcntl failed");
    return res;
}
static int _write_lock(FILE* f, short whence, off_t start, off_t len) {
    struct flock fl;
    fl.l_type = F_WRLCK;
    fl.l_whence = whence;
    fl.l_start = start;
    fl.l_len = len;

    int res;
    if ((res = fcntl(_fileno(f), F_SETLKW, &fl)) < 0)
        err_quit("fcntl failed");
    return res;
}
static int _unlock(FILE* f, short whence, off_t start, off_t len) {
    struct flock fl;
    fl.l_type = F_UNLCK;
    fl.l_whence = whence;
    fl.l_start = start;
    fl.l_len = len;

    int res;
    if ((res = fcntl(_fileno(f), F_SETLKW, &fl)) < 0)
        err_quit("fcntl failed");
    return res;
}

static size_t _fread(void* ptr, size_t size, size_t count, FILE* f) {
    size_t res;
    if ((res = fread(ptr, size, count, f)) != count) {
        err_quit("fread failed");
    }

    return res;
}

static size_t _fwrite(void* ptr, size_t size, size_t count, FILE* f) {
    size_t res;
    if ((res = fwrite(ptr, size, count, f)) != count)
        err_quit("fwrite failed");

    return res;
}

static int _fseek(FILE* f, long offset, int origin) {
    int res;
    if ((res = fseek(f, offset, origin)) != 0)
        err_quit("fseek failed");

    return res;
}

static long _ftell(FILE* f) {
    long res;
    if ((res = ftell(f)) == -1)
        err_quit("ftell failed");
    return res;
}

static FILE* _fopen(const char* filename, const char* mode) {
    FILE* f;
    if (!(f = fopen(filename, mode)))
        err_quit("fopen failed");
    return f;
}

static void* _calloc(size_t count, size_t size) {
    void* ptr;
    if (!(ptr = calloc(count, size)))
        err_quit("calloc failed");

    return ptr;
}

static void* _malloc(size_t size) {
    void* ptr;
    if (!(ptr = malloc(size)))
        err_quit("malloc failed");
    return ptr;
}

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

    db->chain_off = 1;
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

inline static uint32_t _pager_off_to_idx(uint32_t file_off) {
    return file_off / BLOCK_SIZE;
}

static bool _pager_block_is_stale(struct Block* meta, struct Block* b) {
    uint32_t block_meta_off = sizeof(uint64_t) * 2 * b->idx;
    uint64_t seconds = *((uint64_t*)(&meta->buf[block_meta_off]));
    uint64_t counter = *((uint64_t*)(&meta->buf[block_meta_off + sizeof(uint64_t)]));
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
    uint32_t ts_off = b->idx * sizeof(uint64_t) * 2;
    *((uint64_t*)(&db->super->buf[ts_off])) = ts.seconds;
    *((uint64_t*)(&db->super->buf[ts_off + sizeof(uint64_t)])) = ts.counter;

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

    _fseek(db->idxf, SUPER_OFF + b->idx * sizeof(uint64_t) * 2, SEEK_SET);
    _fread((void*)&b->timestamp.seconds, sizeof(uint64_t), 1, db->idxf);
    _fread((void*)&b->timestamp.counter, sizeof(uint64_t), 1, db->idxf);
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

static void pager_write(struct DB* db, uint32_t file_off, char* buf, uint32_t len) {
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

static void pager_read(struct DB* db, uint32_t file_off, char* buf, uint32_t len) {
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

static struct Record _table_read_rec(struct DB* db, uint32_t rec_off) {
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
        struct Record r = _table_read_rec(db, cur);

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

static void _table_write_rec(struct DB* db, uint32_t rec_off, struct Record r, const char* key, const char* data) {
    uint32_t len = sizeof(uint32_t) * 3 + r.key_len + r.data_len;
    char buf[len];

    *((uint32_t*)buf) = r.next_off; 
    *((uint32_t*)(buf + sizeof(uint32_t))) = r.key_len;
    *((uint32_t*)(buf + sizeof(uint32_t) * 2)) = r.data_len;
    memcpy(buf + sizeof(uint32_t) * 3, key, r.key_len);
    memcpy(buf + sizeof(uint32_t) * 3 + r.key_len, data, r.data_len);
    pager_write(db, rec_off, buf, len);
}

static void _table_insert_rec(struct DB* db, const char* key, const char* data) {
    uint32_t chain_off = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    uint32_t head_off;
    pager_read(db, chain_off, &head_off, sizeof(uint32_t));

    struct Record new_rec;
    new_rec.next_off = head_off;
    new_rec.key_len = strlen(key);
    new_rec.data_len = strlen(data);

    uint32_t new_off = _table_get_free_rec(db, new_rec.key_len + new_rec.data_len);
    pager_write(db, chain_off, &new_off, sizeof(uint32_t));
    _table_write_rec(db, new_off, new_rec, key, data);
}

static int _table_delete_rec(struct DB* db, const char* key) {
    uint32_t chain_off = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    uint32_t cur;
    pager_read(db, chain_off, &cur, sizeof(uint32_t));
    uint32_t prev = cur;

    while (cur) {
        struct Record r = _table_read_rec(db, cur);
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

static void _table_read_metadata(struct DB* db) {
    _fseek(db->idxf, SUPER_OFF, SEEK_SET);
    _fread((void*)db->super->buf, sizeof(char), BLOCK_SIZE, db->idxf);
}

static uint32_t _table_find_rec(struct DB* db, const char* key) {
    uint32_t chain_off = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    uint32_t rec_off;
    pager_read(db, chain_off, &rec_off, sizeof(uint32_t));

    while (rec_off) {
        struct Record r = _table_read_rec(db, rec_off);
        char rec_key[r.key_len];
        pager_read(db, rec_off + KEY_OFF, rec_key, r.key_len);

        if (strncmp(rec_key, key, r.key_len) == 0)
            return rec_off;
        rec_off = r.next_off;
    }

    return 0;
}

static void _table_commit(struct DB* db) {

    struct Block* cur = db->blocks;
    while (cur) {
        if (cur->dirty) {
            struct TimeStamp ts = _pager_new_stamp(cur->timestamp);
            _pager_write_from_block(db, cur, ts);

            /*testing*/
            _fseek(db->idxf, 0, SEEK_END);
            uint32_t max = _ftell(db->idxf) - cur->idx * BLOCK_SIZE;
            _fseek(db->idxf, cur->idx * BLOCK_SIZE, SEEK_SET);
            printf("commiting %u bytes from block %u\n", max < BLOCK_SIZE ? max : BLOCK_SIZE, cur->idx);
            /*end testing*/

            cur->dirty = false;
        }
        cur = cur->next;
    }

    struct TimeStamp ts = _pager_new_stamp(db->super->timestamp);
    _pager_write_from_block(db, db->super, ts);
    printf("commiting 4096 bytes from block 0\n");
}

//the table interface should be the same as that of the tree interface
int db_store(struct DB* db, const char* key, const char* data) {
    _write_lock(db->idxf, SEEK_SET, 0, 0);
    _table_read_metadata(db);

    uint32_t rec_off;
    if ((rec_off = _table_find_rec(db, key)) == 0) { //record with given key does not exist
        _table_insert_rec(db, key, data);
    } else { //record with given key exists
        struct Record r = _table_read_rec(db, rec_off);
        if (strlen(data) <= r.data_len) {
            r.data_len = strlen(data);
            _table_write_rec(db, rec_off, r, key, data);
        } else {
            _table_delete_rec(db, key);
            _table_insert_rec(db, key, data);
        }
    }

    _table_commit(db);
    _unlock(db->idxf, SEEK_SET, 0, 0);
    return 0;
}

void db_delete(struct DB* db, const char* key) {
    //need to write lock and also read metadata
    //_table_delete_rec(db, key);
}

char* db_fetch(struct DB* db, const char* key) {
    _read_lock(db->idxf, SEEK_SET, 0, 0);
    _unlock(db->idxf, SEEK_SET, 0, 0);
    return NULL; //TODO: need to return actual data
}

void db_rewind(struct DB* db) {
}

char* db_nextrec(struct DB* db) {
    return NULL;
}

