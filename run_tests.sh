#!/usr/bin/env bash
set -e

OPT_CREATE_PGSQL=yes
OPT_DROP_PGSQL=yes
OPT_COVERAGE=no

while [ -n "$1" ]; do
        if test "$1" = "--nocreate-pg"; then
                OPT_CREATE_PGSQL=no
                shift
                continue
        elif test "$1" = "--nodrop-pg"; then
                OPT_DROP_PGSQL=no
                shift
                continue
        elif test "$1" = "--with-coverage"; then
                OPT_COVERAGE=yes
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
    echo "PGUSER:     [$PGUSER]";
fi

pgHOST=$(node -e "console.log(require('${TESTENV}').postgres.host || '')");
if [[ -n "${pgHOST}" ]]; then
    export PGHOST=${pgHOST};
    echo "PGHOST:     [$PGHOST]";
fi

pgPORT=$(node -e "console.log(require('${TESTENV}').postgres.port || '')");
if [[ -n "${pgPORT}" ]]; then
    export PGPORT=${pgPORT};
    echo "PGPORT:     [$PGPORT]";
fi

pgDATABASE=$(node -e "console.log(require('${TESTENV}').postgres.dbname || 'cartodb_query_tables_tests')");
pgFDWDATABASE=$(node -e "console.log(require('${TESTENV}').postgres.fdw_dbname || 'cartodb_query_tables_fdw')");
if [[ -n "${pgDATABASE}" ]]; then
    export PGDATABASE=${pgDATABASE};
    echo "PGDATABASE: [$PGDATABASE]";
fi

if [[ -n "${pgFDWDATABASE}" ]]; then
    export PGFDWDATABASE=${pgFDWDATABASE};
    echo "PGFDWDATABASE: [$PGFDWDATABASE]";
fi


create_db() {
    if test x"$OPT_CREATE_PGSQL" = xyes; then
        echo -e "\nCreating test database: '$PGDATABASE'";
        createdb -EUTF8 "$PGDATABASE" || die "Could not create test database";
        echo -e "\nCreating FDW test database: '$PGFDWDATABASE'";
        createdb -EUTF8 "$PGFDWDATABASE" || die "Could not create FDW test database";
    fi

}

cleanup() {
     if test x"$OPT_DROP_PGSQL" = xyes; then
        (dropdb --if-exists "$PGDATABASE" && echo -e "\nDropped database '$PGDATABASE'") ||
            (echo -e "\nCould not drop database '$PGDATABASE'. Please review the connection parameters"; exit 1);
        (dropdb --if-exists "$PGFDWDATABASE" && echo -e "\nDropped database '$PGFDWDATABASE'") ||
            (echo -e "\nCould not drop database '$PGFDWDATABASE'. Please review the connection parameters"; exit 1);
    fi
}

die() {
    echo "$1" >&2
    cleanup;
    exit 1;
}

trap 'die' HUP INT QUIT ABRT TERM;



# Database setup
cleanup;
create_db;

# echo -e "\nInstalling cartodb extension";

# Install cartodb extension in the new database
 psql -c 'CREATE EXTENSION IF NOT EXISTS postgis CASCADE' ||  die "Could not install postgis in test database";
TEST_RESULT=0;
if test x"$OPT_COVERAGE" = xyes; then
  echo "Running tests with coverage";
  ./node_modules/nyc/bin/nyc.js ./node_modules/.bin/mocha -u bdd --exit -t 5000 "$@" || TEST_RESULT=1;
else
  echo "Running tests"
  ./node_modules/.bin/mocha -u bdd --exit -t 5000 "$@" || TEST_RESULT=1;
fi

cleanup;

exit $TEST_RESULT;
