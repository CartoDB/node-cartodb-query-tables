'use strict';

const assert = require('assert');
const queryTables = require('../../lib/querytables');
const SubstitutionTokens = require('../../lib/utils/substitution_tokens');
const PSQL = require('cartodb-psql');
const { postgres: databaseConfig } = require('../test_config');

/* Auxiliar function to create a mocked connection */
function createMockConnection(err, rows) {
    return {
        query: function (sql, params, callback, readonly) {
            if (typeof params === 'function') {
                readonly = callback;
                callback = params;
                params = [];
            }
            // Queries should never contain tokens
            assert.strictEqual(SubstitutionTokens.hasTokens(sql), false);

            const result = err ? null : { rows: rows };
            return callback(err, result);
        }
    };
}

/* Auxiliar function to create a database connection */
function createDBConnection() {
    const dbParams = Object.assign({}, databaseConfig);
    const dbPoolParams = {};

    return new PSQL(dbParams, dbPoolParams);
}

/* Auxiliar function to create a connection to the FDW database */
function createFDWDBConnection() {
    const dbParams = Object.assign({}, databaseConfig);
    dbParams.dbname = dbParams.fdw_dbname;
    const dbPoolParams = {};

    return new PSQL(dbParams, dbPoolParams);
}

describe('queryTables', function () {
    describe('.getQueryStatements()', function () {
        /* These tests come from cartodb-postgresql (test/CDB_QueryStatementsTest.sql) */
        it('Should work with a standard query', function () {
            const s = queryTables.getQueryStatements('SELECT * FROM geometry_columns;');
            assert.strictEqual(s.length, 1);
            assert.strictEqual(s[0], 'SELECT * FROM geometry_columns');
        });

        it('Should work with a query without ";"', function () {
            const s = queryTables.getQueryStatements('SELECT * FROM geometry_columns');
            assert.strictEqual(s.length, 1);
            assert.strictEqual(s[0], 'SELECT * FROM geometry_columns');
        });

        it('Should work with a query starting with ";"', function () {
            const s = queryTables.getQueryStatements(';;;;SELECT * FROM geometry_columns');
            assert.strictEqual(s.length, 1);
            assert.strictEqual(s[0], 'SELECT * FROM geometry_columns');
        });

        it('Should work with multiqueries', function () {
            const s = queryTables.getQueryStatements(`
                SELECT * FROM geometry_columns;
                SELECT 1;
                SELECT 2 = 3;
            `);
            assert.strictEqual(s.length, 3);
            assert.strictEqual(s[0], `SELECT * FROM geometry_columns`);
            assert.strictEqual(s[1], `SELECT 1`);
            assert.strictEqual(s[2], `SELECT 2 = 3`);
        });

        it('Should work with quoted commands', function () {
            const s = queryTables.getQueryStatements(`
                CREATE table "my'tab;le" ("$" int);
                SELECT '1','$$', '$hello$', "$" FROM "my'tab;le";
                CREATE function "hi'there" ("'" text default '$')
                returns void as $h$
                declare a int; b text;
                begin
                    b='hi';
                    return;
                end;
                $h$ language 'plpgsql';
                SELECT 5;
            `);
            assert.strictEqual(s.length, 4);
            assert.strictEqual(s[0], `CREATE table "my'tab;le" ("$" int)`);
            assert.strictEqual(s[1], `SELECT '1','$$', '$hello$', "$" FROM "my'tab;le"`);
            assert.strictEqual(s[2], `
                CREATE function "hi'there" ("'" text default '$')
                returns void as $h$
                declare a int; b text;
                begin
                    b='hi';
                    return;
                end;
                $h$ language 'plpgsql'
            `.trim());
            assert.strictEqual(s[3], `SELECT 5`);
        });

        it('Should work with quoted inserts', function () {
            const s = queryTables.getQueryStatements(`
                INSERT INTO "my''""t" values ('''','""'';;');
                SELECT $qu;oted$ hi $qu;oted$;
            `);
            assert.strictEqual(s.length, 2);
            assert.strictEqual(s[0], `INSERT INTO "my''""t" values ('''','""'';;')`);
            assert.strictEqual(s[1], `SELECT $qu;oted$ hi $qu;oted$`);
        });

        it('Should work with line breaks mid sentence', function () {
            const s = queryTables.getQueryStatements(`
                SELECT
                1 ; SELECT
                2
            `);
            assert.strictEqual(s.length, 2);
            assert.strictEqual(s[0], `
                SELECT
                1
            `.trim());
            assert.strictEqual(s[1], `
                SELECT
                2
            `.trim());
        });

        // This is an insane input, illegal sql
        // we are really only testing that it does not
        // take forever to process..
        // The actual result is not correct, so if the function
        // ever gets fixed check if it's better
        it('Should not crash with illegal sql', function () {
            const s = queryTables.getQueryStatements(`
                /a
                $b$
                $c$d
                ;
            `.trim());
            assert.ok(s);
        });

        it('Should work with quoted values', function () {
            const s = queryTables.getQueryStatements(`
                SELECT $quoted$ hi
                $quoted$;
            `);
            assert.strictEqual(s.length, 1);
            assert.strictEqual(s[0], `
                SELECT $quoted$ hi
                $quoted$
            `.trim());
        });
    });

    describe('.getQueryMetadataModel()', function () {
        let connection;
        let fdwConnection;

        const t1UpdateTime = 100000;
        // t2 doesn't use cdb_tablemetadata
        const t3UpdateTime = 101000;
        const tablenameUpdateTime = 104000;
        const remoteUpdateTime = 200000;

        before(function (done) {
            connection = createDBConnection();
            fdwConnection = createFDWDBConnection();

            const params = {};
            const readOnly = false;

            const configureRemoteDatabaseQueries = `
                CREATE SCHEMA IF NOT EXISTS remote_schema;
                CREATE TABLE IF NOT EXISTS remote_schema.remote_table ( a integer );
                CREATE TABLE IF NOT EXISTS remote_schema.cdb_tablemetadata
                        (tabname text, updated_at timestamp with time zone);
                INSERT INTO remote_schema.CDB_TableMetadata (tabname, updated_at)
                        SELECT 'remote_schema.remote_table', to_timestamp(${remoteUpdateTime / 1000});
            `;

            fdwConnection.query(configureRemoteDatabaseQueries, params, (err) => {
                assert.ifError(err);
                const { user, password, host, port, fdw_dbname: fdwDatabaseName} = databaseConfig;
                const configureLocalDatabaseQueries = `
                    CREATE TABLE t2(a integer);
                    CREATE TABLE t1(a integer);
                    CREATE TABLE t3(b text);
                    CREATE TABLE "t with space" (a integer);
                    CREATE TABLE "tablena\'me" (a integer);

                    CREATE SCHEMA IF NOT EXISTS local_fdw;
                    CREATE EXTENSION postgres_fdw;
                    CREATE SERVER remote_server
                        FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host '${host}', port '${port}', dbname '${fdwDatabaseName}');
                    CREATE USER MAPPING FOR ${user}
                        SERVER remote_server
                        OPTIONS (user '${user}' ${password ? `, password '${password}'` : ''});
                    IMPORT FOREIGN SCHEMA remote_schema
                    FROM SERVER remote_server INTO local_fdw;

                    CREATE SCHEMA IF NOT EXISTS cartodb;
                    CREATE TABLE IF NOT EXISTS cartodb.CDB_TableMetadata (
                        tabname regclass not null primary key,
                        updated_at timestamp with time zone not null default now()
                    );
                    INSERT INTO cartodb.CDB_TableMetadata (tabname, updated_at)
                        SELECT 'public.t1', to_timestamp(${t1UpdateTime / 1000}) UNION ALL
                        SELECT 'public.t3', to_timestamp(${t3UpdateTime / 1000}) UNION ALL
                        SELECT 'public.tablena''me', to_timestamp(${tablenameUpdateTime / 1000});
                `;
                connection.query(configureLocalDatabaseQueries, params, (err) => {
                    assert.ifError(err);
                    done();
                }, readOnly);
            }, readOnly);
        });

        after(function () {
            connection.end();
            fdwConnection.end();
        });

        const { dbname: databaseName, fdw_dbname: fdwDatabaseName} = databaseConfig;
        const defaultUpdateAt = -12345;
        const queries = [
            {
                sql: 'TABLE t1;',
                channel: `${databaseName}:public.t1`,
                updatedAt: t1UpdateTime
            },
            {
                sql: 'SELECT * FROM t2;',
                channel: `${databaseName}:public.t2`,
                updatedAt: defaultUpdateAt
            },
            {
                sql: 'SELECT * FROM t2',
                channel: `${databaseName}:public.t2`,
                updatedAt: defaultUpdateAt
            },
            {
                sql: 'SELECT * FROM t1 UNION ALL SELECT * from t2;',
                channel: `${databaseName}:public.t2,public.t1`,
                updatedAt: t1UpdateTime
            },
            {
                sql: 'SELECT * FROM t1 NATURAL JOIN "t with space";',
                channel: `${databaseName}:public.t1,public."t with space"`,
                updatedAt: t1UpdateTime
            },
            {
                sql: 'WITH s1 AS (SELECT * FROM t1) SELECT * FROM t2;',
                channel: `${databaseName}:public.t2`
            },
            {
                sql: 'SELECT 1;',
                channel: ''
            },
            {
                sql: 'TABLE t1; TABLE t2;',
                channel: `${databaseName}:public.t2,public.t1`,
                updatedAt: t1UpdateTime
            },
            {
                sql: "Select * from t3 where b = ';'; TABLE t2",
                channel: `${databaseName}:public.t2,public.t3`,
                updatedAt: t3UpdateTime
            },
            {
                sql: 'TABLE t1; TABLE t1;',
                channel: `${databaseName}:public.t1`,
                updatedAt: t1UpdateTime
            },
            {
                sql: 'SELECT * FROM "tablena\'me";',
                channel: `${databaseName}:public."tablena'me"`,
                updatedAt: tablenameUpdateTime
            },
            {
                sql: 'SELECT * FROM local_fdw.remote_table',
                channel: `${fdwDatabaseName}:local_fdw.remote_table`,
                updatedAt: remoteUpdateTime
            },
            {
                sql: 'SELECT * FROM local_fdw.remote_table NATURAL JOIN public.t1',
                channel: `${databaseName}:public.t1;;${fdwDatabaseName}:local_fdw.remote_table`,
                updatedAt: remoteUpdateTime
            },
            {
                sql: 'SELECT * FROM public.t1 NATURAL JOIN local_fdw.remote_table',
                channel: `${databaseName}:public.t1;;${fdwDatabaseName}:local_fdw.remote_table`,
                updatedAt: remoteUpdateTime
            }
        ];

        queries.forEach(q => {
            it('should return a DatabaseTables model (' + q.sql + ')', function (done) {
                queryTables.getQueryMetadataModel(connection, q.sql, function (err, result) {
                    assert.ifError(err);
                    assert.ok(result);
                    assert.strictEqual(result.getCacheChannel(), q.channel);
                    const expectedUpdatedAt = q.updatedAt ? q.updatedAt : defaultUpdateAt;
                    assert.strictEqual(result.getLastUpdatedAt(defaultUpdateAt), expectedUpdatedAt);
                    return done();
                });
            });
        });

        it('should not crash with syntax errors (DDL)', function (done) {
            queryTables.getQueryMetadataModel(connection, 'DROP TABLE t1;', function (err, result) {
                assert.ifError(err);
                assert.ok(result);
                return done();
            });
        });

        it('should work with unimported CDB_TableMetadata', function (done) {
            const dropQuery = `DROP FOREIGN TABLE local_fdw.CDB_TableMetadata`;
            const params = {};
            const readOnly = false;
            connection.query(dropQuery, params, (err) => {
                assert.ifError(err);
                const selectQuery = 'SELECT * FROM local_fdw.remote_table;';
                queryTables.getQueryMetadataModel(connection, selectQuery, function (err, result) {
                    assert.ifError(err);
                    assert.strictEqual(result.getCacheChannel(), "cartodb_query_tables_fdw:local_fdw.remote_table");
                    const fallbackValue = 123456789;
                    assert.strictEqual(result.getLastUpdatedAt(fallbackValue), fallbackValue);
                    return done();
                });
            }, readOnly);
        });

        it('should not crash with syntax errors (INTO)', function (done) {
            const query = 'SELECT generate_series(1,10) InTO t1';
            queryTables.getQueryMetadataModel(connection, query, function (err, result) {
                assert.ifError(err);
                assert.ok(result);
                return done();
            });
        });

        it('should error with an invalid query', function (done) {
            const query = 'SELECT * FROM table_that_does_not_exists';
            queryTables.getQueryMetadataModel(connection, query, function (err) {
                assert.ok(err);
                return done();
            });
        });

        it('should error with an invalid query at the end', function (done) {
            const queries = `
                SELECT * from t1;
                SELECT * FROM table_that_does_not_exists
            `;
            queryTables.getQueryMetadataModel(connection, queries, function (err) {
                assert.ok(err);
                return done();
            });
        });

        it('should not crash with multiple invalid queries', function (done) {
            const queries = `
                SELECT * from t1;
                SELECT * FROM table_that_does_not_exists;
                SELECT * FROM table_that_does_not_exists;
                SELECT * FROM table_that_does_not_exists;
                SELECT * FROM table_that_does_not_exists
            `;
            queryTables.getQueryMetadataModel(connection, queries, function (err) {
                assert.ok(err);
                return done();
            });
        });

        const tokens = ['pixel_width', 'pixel_height', 'scale_denominator'];
        tokens.forEach(token => {
            it('should not call Postgres with token: ' + token, function (done) {
                const query = 'Select 1 from t1 where 1 != ' + '!' + token + '!';
                queryTables.getQueryMetadataModel(connection, query, function (err, result) {
                    assert.ifError(err);
                    assert.ok(result);
                    assert.strictEqual(result.getCacheChannel(), `${databaseName}:public.t1`);
                    return done();
                });
            });
        });

        it('should not call Postgres with token: bbox', function (done) {
            const query = 'Select 1 from t1 where 1 != ST_Area(!bbox!)';
            queryTables.getQueryMetadataModel(connection, query, function (err, result) {
                assert.ifError(err);
                assert.ok(result);
                assert.strictEqual(result.getCacheChannel(), `${databaseName}:public.t1`);
                return done();
            });
        });

        it('should rethrow db errors', function (done) {
            const mockConnection = createMockConnection(new Error('foo-bar-error'));
            queryTables.getQueryMetadataModel(mockConnection, 'foo-bar-query', function (err) {
                assert.ok(err);
                assert.ok(err.message.match(/foo-bar-error/));
                return done();
            });
        });
    });
});
