var assert = require('assert');
var DatabaseTables = require('../../lib/models/database_tables');

describe('DatabaseTables', function() {
    describe('getCacheChannel', function() {
        it('should group cache-channel tables by database name', function() {
            var tables = new DatabaseTables([
                {dbname: "db1", schema_name: "public", table_name: "tableone"},
                {dbname: "db1", schema_name: "public", table_name: "tabletwo"}
            ]);

            assert.equal(tables.getCacheChannel(), "db1:public.tableone,public.tabletwo");
        });
        it('should support tables coming from different databases', function() {
            var tables = new DatabaseTables([
                {dbname: "db1", schema_name: "public", table_name: "tableone"},
                {dbname: "db1", schema_name: "public", table_name: "tabletwo"},
                {dbname: "db2", schema_name: "public", table_name: "tablethree"}
            ]);

            assert.equal(tables.getCacheChannel(), "db1:public.tableone,public.tabletwo;;db2:public.tablethree");
        });
    });

    describe('getLastUpdatedAt', function() {

        it('should return latest of the known dates', function() {
            var tables = new DatabaseTables([
                {dbname: "db1", schema_name: "public", table_name: "tableone", updated_at: new Date(12345678)},
                {dbname: "db1", schema_name: "public", table_name: "tabletwo", updated_at: new Date(1234567891)},
                {dbname: "db2", schema_name: "public", table_name: "tablethree", updated_at: null}
            ]);
            assert.equal(tables.getLastUpdatedAt(), 1234567891);
        });
        it('getSafeLastUpdatedAt should return fallback date if a table date is unknown', function() {
            var tables = new DatabaseTables([
                {dbname: "db2", schema_name: "public", table_name: "tablethree", updated_at: null}
            ]);
            assert.equal(tables.getLastUpdatedAt('FALLBACK'), 'FALLBACK');
        });
    });
});
