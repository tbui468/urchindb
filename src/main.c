#include <stdio.h>
#include <stdlib.h>
#include "urchin_db.h"

int main () {
    struct DB* db = db_open("test");
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

    db_rewind(db);
    char* key;
    while ((key = db_nextrec(db))) {
        char* value = db_fetch(db, key);
        printf("%s: %s\n", key, value);
        free(key);
        free(value);
    }

    db_close(db);
    return 0;
}
