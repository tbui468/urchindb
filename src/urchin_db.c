#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>

#define BUCKETS_MAX 4

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

void err_quit(const char* msg) {
    printf("%s\n", msg);
    exit(1);
}

static size_t _fread(void* ptr, size_t size, size_t count, FILE* f) {
    size_t res;
    if ((res = fread(ptr, size, count, f)) != count)
        err_quit("fread failed");

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
            ptr = _calloc(BUCKETS_MAX + 1, sizeof(uint32_t));
            _fwrite(ptr, sizeof(uint32_t), BUCKETS_MAX + 1, f);
            free(ptr);
        }

        fclose(f);

        f = _fopen(filename, "r+");
    }

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


static uint32_t _db_read_next_off(struct DB* db, uint32_t idxrec_off) {
    _fseek(db->idxf, idxrec_off, SEEK_SET);
    uint32_t next;
    _fread((void*)&next, sizeof(uint32_t), 1, db->idxf);
    return next;  
}

static uint32_t _db_read_data_off(struct DB* db, uint32_t idxrec_off) {
    _fseek(db->idxf, idxrec_off + sizeof(uint32_t), SEEK_SET);
    uint32_t dat_off;
    _fread((void*)&dat_off, sizeof(uint32_t), 1, db->idxf);
    return dat_off;  
}

static uint32_t _db_read_data_size(struct DB* db, uint32_t idxrec_off) {
    _fseek(db->idxf, idxrec_off + sizeof(uint32_t) * 2, SEEK_SET);
    uint32_t data_size;
    _fread((void*)&data_size, sizeof(uint32_t), 1, db->idxf);
    return data_size;
}

static uint32_t _db_read_key_size(struct DB* db, uint32_t idxrec_off) {
    _fseek(db->idxf, idxrec_off + sizeof(uint32_t) * 3, SEEK_SET);
    uint32_t key_size;
    _fread((void*)&key_size, sizeof(uint32_t), 1, db->idxf);
    return key_size;
}

static struct IdxRec _db_read_idxrec(struct DB* db, uint32_t idxrec_off) {
    struct IdxRec r;

    _fseek(db->idxf, idxrec_off, SEEK_SET);
    _fread((void*)&r.next_off, sizeof(uint32_t), 1, db->idxf);
    _fread((void*)&r.data_off, sizeof(uint32_t), 1, db->idxf);
    _fread((void*)&r.data_size, sizeof(uint32_t), 1, db->idxf);
    _fread((void*)&r.key_size, sizeof(uint32_t), 1, db->idxf);

    return r;
}

static void _db_read_key(struct DB* db, uint32_t idxrec_off, char* key) {
    _fseek(db->idxf, idxrec_off + sizeof(uint32_t) * 3, SEEK_SET);
    uint32_t key_size;
    _fread((void*)&key_size, sizeof(uint32_t), 1, db->idxf);
    _fread((void*)key, sizeof(char), key_size, db->idxf);
}

static void _db_read_data(struct DB* db, uint32_t idxrec_off, char* data) {
    _fseek(db->idxf, idxrec_off + sizeof(uint32_t), SEEK_SET);
    uint32_t data_off, data_size;
    _fread((void*)&data_off, sizeof(uint32_t), 1, db->idxf);
    _fread((void*)&data_size, sizeof(uint32_t), 1, db->idxf);
    _fseek(db->datf, data_off, SEEK_SET);
    _fread((void*)data, sizeof(char), data_size, db->datf);
}

//returns 0 if record not found
//used by db_store (to check for duplicates) and db_fetch
static uint32_t _db_find_idxrec_off(struct DB* db, const char* key) {
    uint32_t bucket_offset = (_hash_key(key) % BUCKETS_MAX + 1) * sizeof(uint32_t);
    _fseek(db->idxf, bucket_offset, SEEK_SET);

    uint32_t next_off;
    _fread((void*)&next_off, sizeof(uint32_t), 1, db->idxf);

    uint32_t prev_off = 0;
    while (next_off != 0) {
        prev_off = next_off;
        struct IdxRec r = _db_read_idxrec(db, next_off);
        next_off = r.next_off;
        char buf[r.key_size + 1];
        _db_read_key(db, prev_off, buf);
        buf[r.key_size] = '\0';
        if (strcmp(buf, key) == 0) {
            break;
        }
        prev_off = 0;
    }

    return prev_off;
}

//return offset of free index record/data record
//internally will rerrange free index records and data records
//returns 0 if no free pair found
static uint32_t _db_new_idxrec(struct DB* db, const char* key, const char* value) {
    assert(!_db_find_idxrec_off(db, key));
    _fseek(db->idxf, 0, SEEK_SET);
    uint32_t next_off;
    _fread((void*)&next_off, sizeof(uint32_t), 1, db->idxf);

    //finds first that fits
    uint32_t idxrec_keylen_fit = 0;
    uint32_t idxrec_vallen_fit = 0;

    uint32_t prev_off = 0;
    uint32_t fit_prev_off;
    while (next_off) {
        struct IdxRec r = _db_read_idxrec(db, next_off);
        uint32_t key_size = r.key_size;

        if (!idxrec_keylen_fit && key_size >= strlen(key)) {
            fit_prev_off = prev_off;
            idxrec_keylen_fit = next_off;
        }

        if (!idxrec_vallen_fit && r.data_size >= strlen(value)) {
            idxrec_vallen_fit = next_off;
        }

        if (idxrec_keylen_fit && idxrec_vallen_fit)
            break;

        prev_off = next_off;
        next_off = _db_read_next_off(db, next_off);
    }

    if (idxrec_keylen_fit != 0 && idxrec_vallen_fit != 0) {
        //swap data offsets so that idxrec_keylen_fit has fitting data record
        uint32_t first = _db_read_data_off(db, idxrec_keylen_fit);
        uint32_t second = _db_read_data_off(db, idxrec_vallen_fit);

        _fseek(db->idxf, idxrec_keylen_fit + sizeof(uint32_t), SEEK_SET);
        _fwrite((void*)&second, sizeof(uint32_t), 1, db->idxf);
        _fseek(db->idxf, idxrec_vallen_fit + sizeof(uint32_t), SEEK_SET);
        _fwrite((void*)&first, sizeof(uint32_t), 1, db->idxf);
      
        //remove index record from free list 
        uint32_t next = _db_read_next_off(db, idxrec_keylen_fit);
        _fseek(db->idxf, fit_prev_off, SEEK_SET);
        _fwrite((void*)&next, sizeof(uint32_t), 1, db->idxf);

        return idxrec_keylen_fit;
    }
    
    //no free index record/data record large enough
    _fseek(db->idxf, 0, SEEK_END);
    uint32_t idx_off = _ftell(db->idxf);
    uint32_t zero = 0;
    _fwrite((void*)&zero, sizeof(uint32_t), 1, db->idxf);
    _fseek(db->datf, 0, SEEK_END);
    uint32_t dat_off = _ftell(db->datf);
    _fwrite((void*)&dat_off, sizeof(uint32_t), 1, db->idxf);
    return idx_off;
}

static int _db_write_records(struct DB* db, uint32_t idxrec_off, const char* key, const char* value) {
    //hash key to get bucket
    uint32_t bucket_offset = (_hash_key(key) % BUCKETS_MAX + 1) * sizeof(uint32_t);
    _fseek(db->idxf, bucket_offset, SEEK_SET);

    //save previous chain offset since new key will now be chain head
    uint32_t next_off;
    _fread((void*)&next_off, sizeof(uint32_t), 1, db->idxf);
    _fseek(db->idxf, bucket_offset, SEEK_SET);
    _fwrite((void*)&idxrec_off, sizeof(uint32_t), 1, db->idxf);

    uint32_t data_off = _db_read_data_off(db, idxrec_off);

    size_t buf_size = sizeof(uint32_t) * 4 + strlen(key);
    uint8_t* buf = _malloc(buf_size);

    //write index record
    memcpy(buf, &next_off, sizeof(uint32_t));
    memcpy(buf + sizeof(uint32_t), &data_off, sizeof(uint32_t));
    uint32_t data_size = strlen(value);
    memcpy(buf + sizeof(uint32_t) * 2, &data_size, sizeof(uint32_t));
    uint32_t key_size = strlen(key);
    memcpy(buf + sizeof(uint32_t) * 3, &key_size, sizeof(uint32_t));
    memcpy(buf + sizeof(uint32_t) * 4, key, strlen(key));
    _fseek(db->idxf, idxrec_off, SEEK_SET);
    _fwrite((void*)buf, sizeof(uint8_t), buf_size, db->idxf);

    free(buf);

    //write value into data record
    _fseek(db->datf, data_off, SEEK_SET);
    _fwrite((void*)value, sizeof(char), strlen(value), db->datf);

    return 0;
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

    memcpy(filename + len, ".dat", 4);
    filename[len + 4] = 0;
    db->datf = _db_open(filename, false);

    db->chain_off = 1;
    db->idxrec_off = 0;

    return db;
}

void db_close(struct DB* db) {
    fclose(db->idxf);
    fclose(db->datf);
}


int db_delete(struct DB* db, const char* key) {
    uint32_t bucket_offset = (_hash_key(key) % BUCKETS_MAX + 1) * sizeof(uint32_t);
    _fseek(db->idxf, bucket_offset, SEEK_SET);

    uint32_t cur_off;
    _fread((void*)&cur_off, sizeof(uint32_t), 1, db->idxf);
    uint32_t prev_off = 0;

    while (cur_off) {
        uint32_t key_size = _db_read_key_size(db, cur_off);
        char buf[key_size + 1];
        _db_read_key(db, cur_off, buf); 
        buf[key_size] = '\0';
        if (strcmp(buf, key) == 0) {
            //remove index record from hash chain
            uint32_t next_off = _db_read_next_off(db, cur_off);

            if (!prev_off) { //index record is head of chain
                _fseek(db->idxf, bucket_offset, SEEK_SET);
            } else { //index record is in middle/end of chain
                _fseek(db->idxf, prev_off, SEEK_SET);
            }
            _fwrite((void*)&next_off, sizeof(uint32_t), 1, db->idxf);

            //insert deleted index record at head of free list
            _fseek(db->idxf, 0, SEEK_SET);
            uint32_t free_off;
            _fread((void*)&free_off, sizeof(uint32_t), 1, db->idxf);
            _fseek(db->idxf, cur_off, SEEK_SET);
            _fwrite((void*)&free_off, sizeof(uint32_t), 1, db->idxf);
            _fseek(db->idxf, 0, SEEK_SET);
            _fwrite((void*)&cur_off, sizeof(uint32_t), 1, db->idxf);
            return 0;
        }

        prev_off = cur_off;
        cur_off = _db_read_next_off(db, cur_off);
    }
    return -1;
}


int db_store(struct DB* db, const char* key, const char* value) {
    uint32_t idxrec_off;
    if ((idxrec_off = _db_find_idxrec_off(db, key)) == 0) { //key doesn't exist - need to check free space
        off_t record_offset = _db_new_idxrec(db, key, value);
        _db_write_records(db, record_offset, key, value);
        return 0;
    } else { //key already exists - overwrite or delete/recreate if value won't fit
        struct IdxRec r = _db_read_idxrec(db, idxrec_off);

        if (strlen(value) <= r.data_size) {
            uint32_t new_size = strlen(value);
            _fseek(db->idxf, idxrec_off + sizeof(uint32_t) * 2, SEEK_SET);
            _fwrite((void*)&new_size, sizeof(uint32_t), 1, db->idxf);
            _fseek(db->datf, r.data_off, SEEK_SET);
            _fwrite((void*)value, sizeof(char), new_size, db->datf);
            return 0;
        } else {
            db_delete(db, key);
            uint32_t idxrec_off = _db_new_idxrec(db, key, value);
            _db_write_records(db, idxrec_off, key, value);
            return 0;
        }
    }
}

char* db_fetch(struct DB* db, const char* key) {
    uint32_t idxrec_off;

    if ((idxrec_off = _db_find_idxrec_off(db, key)) == 0) {
        return NULL;
    }

    uint32_t data_size = _db_read_data_size(db, idxrec_off);
    char* buf = _malloc(data_size + 1);
    _db_read_data(db, idxrec_off, buf);
    buf[data_size] = '\0';
    return buf;
}

void db_rewind(struct DB* db) {
    db->chain_off = 0;
    db->idxrec_off = 0;
}

char* db_nextrec(struct DB* db) {
    //increment db->chain_off until valid index record found
    if (!db->idxrec_off) {
        uint32_t idxrec_off;
        while (true) {
            db->chain_off++;
            if (db->chain_off > BUCKETS_MAX)
                return NULL;
            _fseek(db->idxf, db->chain_off * sizeof(uint32_t), SEEK_SET);
            _fread((void*)&idxrec_off, sizeof(uint32_t), 1, db->idxf);
            if (idxrec_off)
                break;
        }

        uint32_t key_size = _db_read_key_size(db, idxrec_off);
        char* buf = _malloc(key_size + 1);
        _db_read_key(db, idxrec_off, buf);
        buf[key_size] = '\0';
        db->idxrec_off = _db_read_next_off(db, idxrec_off);
        return buf;
    } else { //read index record
        uint32_t key_size = _db_read_key_size(db, db->idxrec_off);
        char* buf = _malloc(key_size + 1);
        _db_read_key(db, db->idxrec_off, buf);
        buf[key_size] = '\0';
        db->idxrec_off = _db_read_next_off(db, db->idxrec_off);
        return buf;
    }
}


