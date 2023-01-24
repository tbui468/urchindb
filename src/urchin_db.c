#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <fcntl.h>

#define BUCKETS_MAX 4
#define UPDATECOUNT_OFF 0
#define FREELIST_OFF 8
#define HASHTAB_OFF 12
#define PAGE_SIZE 4096

struct DB {
    FILE* datf;
    FILE* idxf;
    uint32_t chain_off;
    uint32_t idxrec_off;
    char* dat_buf;
    char* idx_buf;
    uint32_t dat_len;
    uint32_t idx_len;
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

static void _db_read(char* dst, const char* src, size_t count) {
    memcpy(dst, src, count);
}

static void _db_write(char* dst, const char* src, size_t count) {
    memcpy(dst, src, count);
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
            ptr = _calloc(BUCKETS_MAX + 3, sizeof(uint32_t)); //+2 for update count and +1 for freelist
            _write_lock(f, SEEK_SET, 0, 0);
            _fwrite(ptr, sizeof(uint32_t), BUCKETS_MAX + 3, f); //+2 for update count and +1 for freelist
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


static inline uint32_t _db_read_next_off(struct DB* db, uint32_t idxrec_off) {
    return *((uint32_t*)(db->idx_buf + idxrec_off));
}

static inline uint32_t _db_read_data_off(struct DB* db, uint32_t idxrec_off) {
    return *((uint32_t*)(db->idx_buf + idxrec_off + sizeof(uint32_t)));
}

static inline uint32_t _db_read_data_size(struct DB* db, uint32_t idxrec_off) {
    return *((uint32_t*)(db->idx_buf + idxrec_off + sizeof(uint32_t) * 2));
}

static inline uint32_t _db_read_key_size(struct DB* db, uint32_t idxrec_off) {
    return *((uint32_t*)(db->idx_buf + idxrec_off + sizeof(uint32_t) * 3));
}

static struct IdxRec _db_read_idxrec(struct DB* db, uint32_t idxrec_off) {
    struct IdxRec r;
    r.next_off = _db_read_next_off(db, idxrec_off);
    r.data_off = _db_read_data_off(db, idxrec_off);
    r.data_size = _db_read_data_size(db, idxrec_off);
    r.key_size = _db_read_key_size(db, idxrec_off);

    return r;
}

static void _db_read_key(struct DB* db, uint32_t idxrec_off, char* key) {
    uint32_t key_size = _db_read_key_size(db, idxrec_off);
    _db_read(key, db->idx_buf + idxrec_off + sizeof(uint32_t) * 4, key_size);
}

static void _db_read_data(struct DB* db, uint32_t idxrec_off, char* data) {
    uint32_t data_off = _db_read_data_off(db, idxrec_off);
    uint32_t data_size = _db_read_data_size(db, idxrec_off);
    _db_read(data, db->dat_buf + data_off, data_size);
}

//returns 0 if record not found
//used by db_store (to check for duplicates) and db_fetch
static uint32_t _db_find_idxrec_off(struct DB* db, const char* key) {
    uint32_t bucket_offset = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    uint32_t next_off = *((uint32_t*)(db->idx_buf + bucket_offset));

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

static void _db_write_idxrec(struct DB* db, uint32_t off, struct IdxRec* ir) {
    *((uint32_t*)(db->idx_buf + off)) = ir->next_off;
    *((uint32_t*)(db->idx_buf + off + sizeof(uint32_t))) = ir->data_off;
    *((uint32_t*)(db->idx_buf + off + sizeof(uint32_t) * 2)) = ir->data_size;
    *((uint32_t*)(db->idx_buf + off + sizeof(uint32_t) * 3)) = ir->key_size;
}

static void _db_write_key(struct DB* db, uint32_t off, const char* key) {
    _db_write(db->idx_buf + off + sizeof(uint32_t) * 4, key, strlen(key));
}

static void _db_write_data(struct DB* db, uint32_t data_off, const char* data) {
    _db_write(db->dat_buf + data_off, data, strlen(data));
}

static void _db_remove_idxrec_from_chain(struct DB* db, uint32_t idxrec_off, uint32_t bucket_off) {
    uint32_t next = _db_read_next_off(db, idxrec_off);

    uint32_t cur_off = bucket_off;
    uint32_t next_off;
    while ((next_off = _db_read_next_off(db, cur_off)) != idxrec_off) {
        cur_off = next_off;
    }

    *((uint32_t*)(db->idx_buf + cur_off)) = next;
}

static void _db_insert_idxrec_into_chain(struct DB* db, uint32_t idxrec_off, uint32_t bucket_off) {
    uint32_t next_off = *((uint32_t*)(db->idx_buf + bucket_off));
    *((uint32_t*)(db->idx_buf + bucket_off)) = idxrec_off;
    *((uint32_t*)(db->idx_buf + idxrec_off)) = next_off;
}



//return offset of free index record/data record
//internally will rerrange free index records and data records
//returns 0 if no free pair found
static uint32_t _db_new_idxrec(struct DB* db, const char* key, const char* value) {
    assert(!_db_find_idxrec_off(db, key));
    uint32_t next_off = *((uint32_t*)(db->idx_buf + FREELIST_OFF));

    //finds first that fits
    uint32_t idxrec_keylen_fit = 0;
    uint32_t idxrec_vallen_fit = 0;

    while (next_off) {
        struct IdxRec r = _db_read_idxrec(db, next_off);
        uint32_t key_size = r.key_size;

        if (!idxrec_keylen_fit && key_size >= strlen(key)) {
            idxrec_keylen_fit = next_off;
        }

        if (!idxrec_vallen_fit && r.data_size >= strlen(value)) {
            idxrec_vallen_fit = next_off;
        }

        if (idxrec_keylen_fit && idxrec_vallen_fit)
            break;

        next_off = _db_read_next_off(db, next_off);
    }

    if (idxrec_keylen_fit != 0 && idxrec_vallen_fit != 0) {
        //swap data offsets so that idxrec_keylen_fit has fitting data record
        uint32_t first = _db_read_data_off(db, idxrec_keylen_fit);
        uint32_t second = _db_read_data_off(db, idxrec_vallen_fit);

        *((uint32_t*)(db->idx_buf + idxrec_keylen_fit + sizeof(uint32_t))) = second;
        *((uint32_t*)(db->idx_buf + idxrec_vallen_fit + sizeof(uint32_t))) = first;
      
        _db_remove_idxrec_from_chain(db, idxrec_keylen_fit, FREELIST_OFF);
        return idxrec_keylen_fit;
    }
   
    //no free index record/data record large enough
    uint32_t idx_off = db->idx_len;
    uint32_t data_off = db->dat_len;
    db->idx_len += sizeof(uint32_t) * 4 + strlen(key);
    db->dat_len += strlen(value);

    struct IdxRec r = {0, data_off, strlen(value), strlen(key) }; //0 is temporary - caller should fill this in before writing to disk/buffer
    _db_write_idxrec(db, idx_off, &r);
    
    return idx_off;
}

static bool _db_buffers_stale(struct DB* db, uint64_t* bigger_uc) {
    uint64_t file_uc;
    _fseek(db->idxf, UPDATECOUNT_OFF, SEEK_SET);
    _fread((void*)&file_uc, sizeof(uint64_t), 1, db->idxf);
    uint64_t buf_uc = *((uint64_t*)(db->idx_buf + UPDATECOUNT_OFF));

    *bigger_uc = file_uc > buf_uc ? file_uc : buf_uc;

    return file_uc != buf_uc;
}

static void _db_reload_cache(struct DB* db) {
    _fseek(db->idxf, 0, SEEK_END);
    db->idx_len = _ftell(db->idxf);
    _fseek(db->idxf, 0, SEEK_SET);
    _fread(db->idx_buf, sizeof(char), db->idx_len, db->idxf);

    _fseek(db->datf, 0, SEEK_END);
    db->dat_len = _ftell(db->datf); 
    _fseek(db->datf, 0, SEEK_SET);
    _fread(db->dat_buf, sizeof(char), db->dat_len, db->datf);
}

static void _db_write_cache(struct DB* db, uint64_t update_count) {
    *((uint64_t*)(db->idx_buf + UPDATECOUNT_OFF)) = update_count;
    _fseek(db->idxf, 0, SEEK_SET);
    _fwrite((void*)db->idx_buf, sizeof(char), db->idx_len, db->idxf);
    _fseek(db->datf, 0, SEEK_SET);
    _fwrite((void*)db->dat_buf, sizeof(char), db->dat_len, db->datf);
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

    db->idx_buf = _calloc(PAGE_SIZE * PAGE_SIZE, sizeof(char));
    db->dat_buf = _calloc(PAGE_SIZE * PAGE_SIZE, sizeof(char));

    _db_reload_cache(db);

    return db;
}

void db_close(struct DB* db) {
    fclose(db->idxf);
    fclose(db->datf);
    free(db->idx_buf);
    free(db->dat_buf);
}

int _db_dodelete(struct DB* db, const char* key) {
    uint32_t bucket_offset = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    uint32_t cur_off = *((uint32_t*)(db->idx_buf + bucket_offset));
    int result = -1;

    while (cur_off) {
        uint32_t key_size = _db_read_key_size(db, cur_off);
        char buf[key_size + 1];
        _db_read_key(db, cur_off, buf); 
        buf[key_size] = '\0';
        if (strcmp(buf, key) == 0) {
            _db_remove_idxrec_from_chain(db, cur_off, bucket_offset);
            _db_insert_idxrec_into_chain(db, cur_off, FREELIST_OFF);
            result = 0;
            break;
        }

        cur_off = _db_read_next_off(db, cur_off);
    }

    return result;
}


int db_delete(struct DB* db, const char* key) {
    _write_lock(db->idxf, SEEK_SET, 0, 0);
    _write_lock(db->datf, SEEK_SET, 0, 0);

    uint64_t buf_uc;
    if (_db_buffers_stale(db, &buf_uc)) {
        _db_reload_cache(db);
    }

    int result = _db_dodelete(db, key);

    _db_write_cache(db, buf_uc + 1);

    _unlock(db->idxf, SEEK_SET, 0, 0);
    _unlock(db->datf, SEEK_SET, 0, 0);
    return result;
}


static void _db_doinsert(struct DB* db, const char* key, const char* value) {
    off_t record_offset = _db_new_idxrec(db, key, value);
    uint32_t bucket_offset = _hash_key(key) % BUCKETS_MAX * sizeof(uint32_t) + HASHTAB_OFF;
    _db_insert_idxrec_into_chain(db, record_offset, bucket_offset);

    struct IdxRec r = _db_read_idxrec(db, record_offset);
    r.data_size = strlen(value);
    r.key_size = strlen(key);
    _db_write_idxrec(db, record_offset, &r);
    _db_write_key(db, record_offset, key);
    _db_write_data(db, r.data_off, value);
}

int db_store(struct DB* db, const char* key, const char* value) {
    _write_lock(db->idxf, SEEK_SET, 0, 0);
    _write_lock(db->datf, SEEK_SET, 0, 0);

    uint64_t buf_uc;
    if (_db_buffers_stale(db, &buf_uc)) {
        _db_reload_cache(db);
    }

    uint32_t idxrec_off;
    if ((idxrec_off = _db_find_idxrec_off(db, key)) == 0) { //key doesn't exist - need to check free space
        _db_doinsert(db, key, value);
    } else { //key already exists - overwrite or delete/recreate if value won't fit
        struct IdxRec r = _db_read_idxrec(db, idxrec_off);
        if (strlen(value) <= r.data_size) {
            r.data_size = strlen(value);
            _db_write_idxrec(db, idxrec_off, &r);
            _db_write_data(db, r.data_off, value);
        } else {
            _db_dodelete(db, key);
            _db_doinsert(db, key, value);
        }
    }

    _db_write_cache(db, buf_uc + 1);

    _unlock(db->idxf, SEEK_SET, 0, 0);
    _unlock(db->datf, SEEK_SET, 0, 0);
    return 0;
}

char* db_fetch(struct DB* db, const char* key) {
    _read_lock(db->idxf, SEEK_SET, 0, 0);
    _read_lock(db->datf, SEEK_SET, 0, 0);

    uint64_t buf_uc;
    if (_db_buffers_stale(db, &buf_uc)) {
        _db_reload_cache(db);
    }

    uint32_t idxrec_off;

    if ((idxrec_off = _db_find_idxrec_off(db, key)) == 0) {
        return NULL;
    }

    uint32_t data_size = _db_read_data_size(db, idxrec_off);
    char* buf = _malloc(data_size + 1);
    _db_read_data(db, idxrec_off, buf);
    buf[data_size] = '\0';

    _unlock(db->idxf, SEEK_SET, 0, 0);
    _unlock(db->datf, SEEK_SET, 0, 0);
    return buf;
}

void db_rewind(struct DB* db) {
    db->chain_off = 0;
    db->idxrec_off = 0;
}


//NOTE: record will be skipped if inserted before the current db-chain_off
char* db_nextrec(struct DB* db) {
    _read_lock(db->idxf, SEEK_SET, 0, 0);
    _read_lock(db->datf, SEEK_SET, 0, 0);

    uint64_t buf_uc;
    if (_db_buffers_stale(db, &buf_uc)) {
        _db_reload_cache(db);
    }

    char* buf;
    //increment db->chain_off until valid index record found
    if (!db->idxrec_off) {
        uint32_t idxrec_off;
        while (true) {
            db->chain_off++;
            if (db->chain_off > BUCKETS_MAX)
                return NULL;
            idxrec_off = *((uint32_t*)(db->idx_buf + db->chain_off * sizeof(uint32_t) + HASHTAB_OFF));
            if (idxrec_off)
                break;
        }

        uint32_t key_size = _db_read_key_size(db, idxrec_off);
        buf = _malloc(key_size + 1);
        _db_read_key(db, idxrec_off, buf);
        buf[key_size] = '\0';
        db->idxrec_off = _db_read_next_off(db, idxrec_off);
    } else { //read index record
        uint32_t key_size = _db_read_key_size(db, db->idxrec_off);
        buf = _malloc(key_size + 1);
        _db_read_key(db, db->idxrec_off, buf);
        buf[key_size] = '\0';
        db->idxrec_off = _db_read_next_off(db, db->idxrec_off);
    }

    _unlock(db->idxf, SEEK_SET, 0, 0);
    _unlock(db->datf, SEEK_SET, 0, 0);
    return buf;
}


