'use strict';

const assert = require('assert');
const QueryTables = require('../../lib/querytables');
const SubstitutionTokens = require('../../lib/utils/substitution_tokens');

describe('QueryTables', function() {

    function createMockConnection(err, rows) {
        return {
            query: function(query, callback) {
                // Queries should never contain tokens
                assert.equal(SubstitutionTokens.hasTokens(query), false);

                const result = err ? null : { rows: rows };
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

        const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
        tokens.forEach(token => {
            it('should not call Postgres with token: ' + token, function(done) {
                var mockConnection = createMockConnection(null, [{
                    dbname: 'dbd',
                    schema_name: 'public',
                    table_name: 't1',
                    updated_at: new Date()
                }]);

                const query = 'Select 1 from t1 where 1 = ' + '!' + token + '!';
                QueryTables.getAffectedTablesFromQuery(mockConnection, query, function (err, result) {
                    assert.ok(!err, err);
                    assert.ok(result);
                    assert.equal(result.getCacheChannel(), 'dbd:public.t1');
                    return done();
                });
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
            const mockConnection = createMockConnection(null, []);
            QueryTables.getAffectedTableNamesFromQuery(mockConnection, 'foo-bar-query', function (err, result) {
                assert.ok(!err, err);

                assert.ok(result);

                return done();
            });
        });

        const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
        tokens.forEach(token => {
            it('should not call Postgres with token: ' + token, function(done) {
                const mockConnection = createMockConnection(null, []);

                const query = 'Select 1 from t1 where 1 = ' + '!' + token + '!';
                QueryTables.getAffectedTableNamesFromQuery(mockConnection, query, function (err, result) {
                    assert.ok(!err, err);
                    assert.ok(result);
                    return done();
                });
            });
        });

    });

});
