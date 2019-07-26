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

    describe('getQueryMetadataModel', function() {

        it('should return a DatabaseTables model', function(done) {
            const mockConnection = createMockConnection(null, [{
                dbname: 'dbd',
                schema_name: 'public',
                table_name: 't1',
                updated_at: new Date()
            }]);
            QueryTables.getQueryMetadataModel(mockConnection, 'foo-bar-query', function (err, result) {
                assert.ok(!err, err);
                assert.ok(result);
                assert.equal(result.getCacheChannel(), 'dbd:public.t1');
                return done();
            });
        });

        const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
        tokens.forEach(token => {
            it('should not call Postgres with token: ' + token, function(done) {
                const mockConnection = createMockConnection(null, [{
                    dbname: 'dbd',
                    schema_name: 'public',
                    table_name: 't1',
                    updated_at: new Date()
                }]);

                const query = 'Select 1 from t1 where 1 = ' + '!' + token + '!';
                QueryTables.getQueryMetadataModel(mockConnection, query, function (err, result) {
                    assert.ok(!err, err);
                    assert.ok(result);
                    assert.equal(result.getCacheChannel(), 'dbd:public.t1');
                    return done();
                });
            });
        });

        it('should rethrow db errors', function(done) {
            const mockConnection = createMockConnection(new Error('foo-bar-error'));
            QueryTables.getQueryMetadataModel(mockConnection, 'foo-bar-query', function (err) {
                assert.ok(err);
                assert.ok(err.message.match(/foo-bar-error/));
                return done();
            });
        });

    });

});
