#include <fcntl.h>

#include "util.h"

void err_quit(const char* msg) {
    perror(msg);
    exit(1);
}

int _fileno(FILE* f) {
    int res;
    if ((res = fileno(f)) < 0)
        err_quit("fileno failed");
    return res;
}

int _read_lock(FILE* f, short whence, off_t start, off_t len) {
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

int _write_lock(FILE* f, short whence, off_t start, off_t len) {
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

int _unlock(FILE* f, short whence, off_t start, off_t len) {
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

size_t _fread(void* ptr, size_t size, size_t count, FILE* f) {
    size_t res;
    if ((res = fread(ptr, size, count, f)) != count) {
        err_quit("fread failed");
    }

    return res;
}

size_t _fwrite(void* ptr, size_t size, size_t count, FILE* f) {
    size_t res;
    if ((res = fwrite(ptr, size, count, f)) != count)
        err_quit("fwrite failed");

    return res;
}

int _fseek(FILE* f, long offset, int origin) {
    int res;
    if ((res = fseek(f, offset, origin)) != 0)
        err_quit("fseek failed");

    return res;
}

long _ftell(FILE* f) {
    long res;
    if ((res = ftell(f)) == -1)
        err_quit("ftell failed");
    return res;
}

FILE* _fopen(const char* filename, const char* mode) {
    FILE* f;
    if (!(f = fopen(filename, mode)))
        err_quit("fopen failed");
    return f;
}

void* _calloc(size_t count, size_t size) {
    void* ptr;
    if (!(ptr = calloc(count, size)))
        err_quit("calloc failed");

    return ptr;
}

void* _malloc(size_t size) {
    void* ptr;
    if (!(ptr = malloc(size)))
        err_quit("malloc failed");
    return ptr;
}
