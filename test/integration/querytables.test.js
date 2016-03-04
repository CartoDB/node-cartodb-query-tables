'use strict';

var assert = require('assert');
var QueryTables = require('../../lib/querytables');

describe('QueryTables', function() {

    var mockConnection = {
        query: function(query, callback) {
            return callback(null, {rows: [{
                dbname: 'dbd',
                schema_name: 'public',
                table_name: 't1',
                updated_at: new Date()
            }]});
        }
    };

    it('should return a DatabaseTables model', function(done) {
        QueryTables.getAffectedTablesFromQuery(mockConnection, 'foo-bar-query', function (err, result) {
            assert.ok(!err, err);
            assert.ok(result);
            assert.equal(result.getCacheChannel(), 'dbd:public.t1');
            return done();
        });
    });
});
