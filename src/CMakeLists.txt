set(Sources
    main.c
    urchin_db.c
    )

set(Headers
    urchin_db.h
    )

add_executable(
    kvdb
    ${Headers}
    ${Sources}
    )
