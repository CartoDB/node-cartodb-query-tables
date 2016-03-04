'use strict';

var assert = require('assert');
var DatabaseTables = require('../../lib/models/database_tables');

describe('DatabaseTables', function() {

    describe('getCacheChannel', function() {
        it('should group cache-channel tables by database name', function() {
            var tables = new DatabaseTables([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
            ]);

            assert.equal(tables.getCacheChannel(), 'db1:public.tableone,public.tabletwo');
        });

        it('should support tables coming from different databases', function() {
            var tables = new DatabaseTables([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                {dbname: 'db2', schema_name: 'public', table_name: 'tablethree'}
            ]);

            assert.equal(tables.getCacheChannel(), 'db1:public.tableone,public.tabletwo;;db2:public.tablethree');
        });
    });

    describe('getLastUpdatedAt', function() {

        it('should return latest of the known dates', function() {
            var tables = new DatabaseTables([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: new Date(12345678)},
                {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo', updated_at: new Date(1234567891)},
                {dbname: 'db2', schema_name: 'public', table_name: 'tablethree', updated_at: null}
            ]);
            assert.equal(tables.getLastUpdatedAt(), 1234567891);
        });

        it('getSafeLastUpdatedAt should return fallback date if a table date is unknown', function() {
            var tables = new DatabaseTables([
                {dbname: 'db2', schema_name: 'public', table_name: 'tablethree', updated_at: null}
            ]);
            assert.equal(tables.getLastUpdatedAt('FALLBACK'), 'FALLBACK');
        });

        it('getSafeLastUpdatedAt should return fallback date if no tables were found', function() {
            var tables = new DatabaseTables([]);
            assert.equal(tables.getLastUpdatedAt('FALLBACK'), 'FALLBACK');
        });
    });

    describe('key', function() {

        var KEY_LENGTH = 8;

        it('should get an array of keys for multiple tables', function() {
            var tables = new DatabaseTables([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
            ]);

            var keys = tables.key();
            assert.equal(keys.length, 2);
            assert.equal(keys[0].length, KEY_LENGTH);
            assert.equal(keys[1].length, KEY_LENGTH);
        });

        it('should return proper surrogate-key (db:schema.table)', function() {
            var tables = new DatabaseTables([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: new Date(12345678)},
            ]);
            assert.deepEqual(tables.key(), ['t:8ny9He']);
        });
        it('should keep escaped tables escaped (db:"sch-ema".table)', function() {
            var tables = new DatabaseTables([
                {dbname: 'db1', schema_name: '"sch-ema"', table_name: 'tableone', updated_at: new Date(12345678)},
            ]);
            assert.deepEqual(tables.key(), ['t:oVg75u']);
        });
    });
});
