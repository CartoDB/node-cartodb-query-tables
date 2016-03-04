'use strict';

var assert = require('assert');
var QueryTables = require('../../lib/querytables');

describe('QueryTables', function() {

    function createMockConnection(err, rows) {
        return {
            query: function(query, callback) {
                var result = err ? null : { rows: rows };
                return callback(err, result);
            }
        };
    }

    describe('getAffectedTablesFromQuery', function() {

        it('should return a DatabaseTables model', function(done) {
            var mockConnection = createMockConnection(null, [{
                dbname: 'dbd',
                schema_name: 'public',
                table_name: 't1',
                updated_at: new Date()
            }]);
            QueryTables.getAffectedTablesFromQuery(mockConnection, 'foo-bar-query', function (err, result) {
                assert.ok(!err, err);
                assert.ok(result);
                assert.equal(result.getCacheChannel(), 'dbd:public.t1');
                return done();
            });
        });

        it('should rethrow db errors', function(done) {
            var mockConnection = createMockConnection(new Error('foo-bar-error'));
            QueryTables.getAffectedTablesFromQuery(mockConnection, 'foo-bar-query', function (err) {
                assert.ok(err);
                assert.ok(err.message.match(/foo-bar-error/));
                return done();
            });
        });

    });

    describe('getAffectedTableNamesFromQuery', function() {

        it('should work for empty results', function(done) {
            var mockConnection = createMockConnection(null, []);
            QueryTables.getAffectedTableNamesFromQuery(mockConnection, 'foo-bar-query', function (err, result) {
                assert.ok(!err, err);

                assert.ok(result);

                return done();
            });
        });

    });

});
