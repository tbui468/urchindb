#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "urchin_db.h"

int main (int argc, char** argv) {
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

    db_rewind(db);
    char* key;
    while ((key = db_nextrec(db))) {
        char* value = db_fetch(db, key);
        printf("%s: %s\n", key, value);
        free(key);
        free(value);
    }
/*
    //for testing locking of files
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
    }*/

    db_close(db);
    return 0;
}
