#!/usr/bin/env bash
set -e

OPT_CREATE_PGSQL=yes
OPT_DROP_PGSQL=yes
VERBOSE=no
MODE=setup

while [ -n "$1" ]; do
    if test "$1" = "--nodrop-pg"; then
        OPT_DROP_PGSQL=no
        shift
        continue
    elif test "$1" = "--nocreate-pg"; then
        OPT_CREATE_PGSQL=no
        shift
        continue
    elif test "$1" = "--clean"; then
        MODE=clean
        shift
        continue
    elif test "$1" = "--verbose"; then
        VERBOSE=yes
        shift
        continue
    else
        break
    fi
done

# This is where postgresql connection parameters are read from
TESTENV=./test/test_config

# If the file doesn't exists, copy the template
if [[ ! -f "${TESTENV}.js" ]]; then
    cp "${TESTENV}.sample" "${TESTENV}.js";
fi

# Extract postgres configuration

pgUSER=$(node -e "console.log(require('${TESTENV}').postgres.user || '')");
if [[ -n "${pgUSER}" ]]; then
    export PGUSER=${pgUSER};
    if test x"$VERBOSE" = xyes; then
        echo "PGUSER:     [$PGUSER]";
    fi
fi

pgHOST=$(node -e "console.log(require('${TESTENV}').postgres.host || '')");
if [[ -n "${pgHOST}" ]]; then
    export PGHOST=${pgHOST};
    if test x"$VERBOSE" = xyes; then
        echo "PGHOST:     [$PGHOST]";
    fi
fi

pgPORT=$(node -e "console.log(require('${TESTENV}').postgres.port || '')");
if [[ -n "${pgPORT}" ]]; then
    export PGPORT=${pgPORT};
    if test x"$VERBOSE" = xyes; then
        echo "PGPORT:     [$PGPORT]";
    fi
fi

pgDATABASE=$(node -e "console.log(require('${TESTENV}').postgres.dbname || 'cartodb_query_tables_tests')");
pgFDWDATABASE=$(node -e "console.log(require('${TESTENV}').postgres.fdw_dbname || 'cartodb_query_tables_fdw')");
if [[ -n "${pgDATABASE}" ]]; then
    export PGDATABASE=${pgDATABASE};
    if test x"$VERBOSE" = xyes; then
        echo "PGDATABASE: [$PGDATABASE]";
    fi
fi

if [[ -n "${pgFDWDATABASE}" ]]; then
    export PGFDWDATABASE=${pgFDWDATABASE};
    if test x"$VERBOSE" = xyes; then
        echo "PGFDWDATABASE: [$PGFDWDATABASE]";
    fi
fi

create_db() {
    if test x"$OPT_CREATE_PGSQL" = xyes; then
        createdb -EUTF8 "$PGDATABASE" || die "Could not create test database '$PGDATABASE'. Please review the connection parameters";
        if test x"$VERBOSE" = xyes; then
            echo -e "\nDatabase '$PGDATABASE' created";
        fi
        createdb -EUTF8 "$PGFDWDATABASE" || die "Could not create FDW test database '$PGFDWDATABASE'. Please review the connection parameters";
        if test x"$VERBOSE" = xyes; then
            echo -e "\nDatabase: '$PGFDWDATABASE' created";
        fi
        psql -c 'CREATE EXTENSION IF NOT EXISTS postgis CASCADE' &> /dev/null || die "Could not install postgis in test database server";
        if test x"$VERBOSE" = xyes; then
            echo -e "\Installed extension";
        fi
    fi
}

cleanup() {
    if test x"$OPT_DROP_PGSQL" = xyes; then
        (dropdb --if-exists "$PGDATABASE" &> /dev/null) ||
            (echo -e "\nCould not drop database '$PGDATABASE'. Please review the connection parameters"; exit 1);
        if test x"$VERBOSE" = xyes; then
            echo -e "\nDropped database '$PGDATABASE'";
        fi
        (dropdb --if-exists "$PGFDWDATABASE" &> /dev/null) ||
            (echo -e "\nCould not drop database '$PGFDWDATABASE'. Please review the connection parameters"; exit 1);
        if test x"$VERBOSE" = xyes; then
            echo -e "\nDropped database '$PGFDWDATABASE'";
        fi
    fi
}

die() {
    echo "$1" >&2
    cleanup;
    exit 1;
}

trap 'die' HUP INT QUIT ABRT TERM;

if test x"$MODE" = xsetup; then
    create_db;
else
    cleanup;
fi

exit 0;
