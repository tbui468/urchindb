#ifndef UDB_UTIL_H
#define UDB_UTIL_H

#include <stddef.h>
#include <stdio.h>

void err_quit(const char* msg);

int _fileno(FILE* f);
int _read_lock(FILE* f, short whence, off_t start, off_t len);
int _write_lock(FILE* f, short whence, off_t start, off_t len);
int _unlock(FILE* f, short whence, off_t start, off_t len);
size_t _fread(void* ptr, size_t size, size_t count, FILE* f);
size_t _fwrite(void* ptr, size_t size, size_t count, FILE* f);
int _fseek(FILE* f, long offset, int origin);
long _ftell(FILE* f);
FILE* _fopen(const char* filename, const char* mode);
void* _calloc(size_t count, size_t size);
void* _malloc(size_t size);

#endif //UDB_UTIL_H
