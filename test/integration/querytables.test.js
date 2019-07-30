'use strict';

const assert = require('assert');
const QueryTables = require('../../lib/querytables');
const SubstitutionTokens = require('../../lib/utils/substitution_tokens');
const PSQL = require('cartodb-psql');

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

    function createDBConnection() {
        const dbParams = require('../test_config').postgres;
        const dbPoolParams = {};
        let connection = undefined;
        assert.doesNotThrow(() => { connection = new PSQL(dbParams, dbPoolParams);});
        assert.ok(connection);
        return connection;
    }

    describe('getQueryMetadataModel', function() {
        const connection = createDBConnection();
        const params = {};
        const readOnly = false;
        before((done) => {
            connection.query('CREATE TABLE t1(a integer)', params, (err, result) =>{
                assert.ok(!err, err);
                done();
            }, readOnly);
        });

        it('should return a DatabaseTables model', function(done) {
            
            QueryTables.getQueryMetadataModel(connection, 'Select * from t1', function (err, result) {
                assert.ok(!err, err);
                assert.ok(result);
                assert.equal(result.getCacheChannel(), `${require('../test_config').postgres.dbname}:public.t1`);
                return done();
            });
        });

        const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
        tokens.forEach(token => {
            xit('should not call Postgres with token: ' + token, function(done) {
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

        xit('should rethrow db errors', function(done) {
            const mockConnection = createMockConnection(new Error('foo-bar-error'));
            QueryTables.getQueryMetadataModel(mockConnection, 'foo-bar-query', function (err) {
                assert.ok(err);
                assert.ok(err.message.match(/foo-bar-error/));
                return done();
            });
        });

    });

});
