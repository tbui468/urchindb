#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include "urchin_db.h"

int standard_test() {
    struct DB* db = db_open("test");
    //general testing
    if (db_store(db, "dog", "dog data") != 0)
        err_quit("db_store failed");
    if (db_store(db, "cat", "cat data") != 0)
        err_quit("db_store failed");
    if (db_store(db, "bird", "bird data") != 0)
        err_quit("db_store failed");
    if (db_store(db, "lion", "lion data") != 0)
        err_quit("db_store failed");
    if (db_store(db, "bat", "bat data") != 0)
        err_quit("db_store failed");
    if (db_store(db, "dog", "new dataaaaaaaaaaaaaaaaaa") != 0)
        err_quit("db_store failed");
    if (db_store(db, "hog", "hog data") != 0)
        err_quit("db_store failed");
    if (db_store(db, "hog", "hog data 2") != 0)
        err_quit("db_store failed");

    for (int i = 0; i < 10000; i++) {
        db_rewind(db);
        char* key;
        while ((key = db_nextrec(db))) {
            char* value = db_fetch(db, key);
        //    printf("%s: %s\n", key, value);
            free(key);
            free(value);
        }
    }

    db_close(db);
    return 0;
}

int file_locking_test(int argc, char** argv) {
    struct DB* db = db_open("test");
    const int len = 5000;
    char msg1[len];
    char msg2[len];
    memset(msg1, 'x', len - 1);
    memset(msg2, 'y', len - 1);
    msg1[len - 1] = '\0';
    msg2[len - 1] = '\0';

    if (argc > 1) {
        bool one = true;
        while (true) {
            if (one) {
                if (db_store(db, "dog", msg1) != 0)
                    err_quit("db_store failed");
                one = false;
            } else {
                if (db_store(db, "dog", msg2) != 0)
                    err_quit("db_store failed");
                one = true;
            }
        }
    } else {
        while (true) {
            char* data = db_fetch(db, "dog");
            if (data) {
                if (!(strcmp(data, msg1) == 0 || strcmp(data, msg2) == 0))
                    printf("corrupted data\n");
                else
                    printf("valid data\n");
                free(data);
            }
        }
    }

    db_close(db);
    return 0;
}

int paging_test(uint32_t n) {
    //add n records
    uint32_t count;
    struct DB* db = db_open("test");
    for (count = 0; count < n; count++) {
        char key_buf[1024];
        sprintf(key_buf, "key%d", count);
        char data_buf[1024];
        sprintf(data_buf, "data%d", count);
        if (db_store(db, key_buf, data_buf) != 0)
            err_quit("db_store failed");
    }

    //read n records
    db_rewind(db);
    char* key;
    while ((key = db_nextrec(db))) {
        char* value = db_fetch(db, key);
        free(key);
        free(value);
    }

    bool equal_size = 0;

    for (int i = 0; i < 5 * (int)n; i++) {
        //read random
        uint32_t r = rand() % count; 
        char key_buf[1024];
        sprintf(key_buf, "key%d", r);
        char* res = db_fetch(db, key_buf);
        if (res)
            free(res);

        if (i % 10 == 0) {
            char key_buf[1024];
            sprintf(key_buf, "key%d", count);
            char data_buf[1024];
            sprintf(data_buf, "data%d", count);
            if (db_store(db, key_buf, data_buf) != 0)
                err_quit("db_store failed");
            count++;
        }

        if (i % 20 == 0) {
            uint32_t r = rand() % count; 
            char key_buf[1024];
            sprintf(key_buf, "key%d", r);
            char* res = db_fetch(db, key_buf);
            if (res) {
                if (equal_size) {
                    db_store(db, key_buf, res);
                    equal_size = 0;
                } else {
                    char buf[1024];
                    strcpy(buf, res);
                    strcpy(buf + strlen(res), "a");
                    db_store(db, key_buf, buf);
                    equal_size = 1;
                }
                free(res);
            }
        }

        if (i % 40 == 0) {
            uint32_t r = rand() % count; 
            char key_buf[1024];
            sprintf(key_buf, "key%d", r);
            db_delete(db, key_buf);
        }
    }

    for (int i = 0; i < (int)count; i++) {
        char key_buf[1024];
        sprintf(key_buf, "key%d", i);
        db_delete(db, key_buf);
        if (i < (int)count - 10) {
            for (int j = i; j < i + 10; j++) {
                char key_buf[1024];
                sprintf(key_buf, "key%d", j);
                char* res = db_fetch(db, key_buf);
                if (res)
                    free(res);
            }
        }
    }

    db_close(db);
    return 0;
}

int main(int argc, char** argv) {
    paging_test(500);
    printf("Seconds passed: %f\n", clock() / (double)CLOCKS_PER_SEC);
    return 0;
}
