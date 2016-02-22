var assert = require('assert');
var DatabaseTables = require('../../lib/models/database_tables');

describe('DatabaseTables', function() {

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
