set(Sources
    main.c
    urchin.c
    util.c
    pager.c
    table.c
    )

set(Headers
    urchin.h
    util.h
    pager.h
    table.h
    )

add_executable(
    urchindb
    ${Headers}
    ${Sources}
    )
