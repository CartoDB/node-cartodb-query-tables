'use strict';

var DatabaseTables = require('./models/database_tables');

var affectedTableRegexCache = {
    bbox: /!bbox!/g,
    scale_denominator: /!scale_denominator!/g,
    pixel_width: /!pixel_width!/g,
    pixel_height: /!pixel_height!/g
};


module.exports.getAffectedTableNamesFromQuery = function (pg, sql, callback) {
    var query = 'SELECT CDB_QueryTablesText($windshaft$' + prepareSql(sql) + '$windshaft$)';

    pg.query(query, function handleAffectedTablesInQueryRows (err, result) {
        if (err){
            var msg = err.message ? err.message : err;
            return callback(new Error('could not fetch source tables: ' + msg));
        }

        result = result || {};
        var rows = result.rows || [];

        // This is an Array, so no need to split into parts
        var tableNames = (!!rows[0]) ? rows[0].cdb_querytablestext : [];
        return callback(null, tableNames);
    });
};

module.exports.getAffectedTablesFromQuery = function (pg, sql, callback) {
    var query =
            'SELECT * FROM CDB_QueryTables_Updated_At($windshaft$' + prepareSql(sql) + '$windshaft$)';

    pg.query(query, function handleAffectedTablesAndLastUpdatedTimeRows (err, result) {
        if (err) {
            var msg = err.message ? err.message : err;
            return callback(new Error('could not fetch affected tables or last updated time: ' + msg));
        }
        result = result || {};
        var rows = result.rows || [];

        callback(null, new DatabaseTables(rows));
    });
};

function prepareSql(sql) {
    return sql
        .replace(affectedTableRegexCache.bbox, 'ST_MakeEnvelope(0,0,0,0)')
        .replace(affectedTableRegexCache.scale_denominator, '0')
        .replace(affectedTableRegexCache.pixel_width, '1')
        .replace(affectedTableRegexCache.pixel_height, '1')
    ;
}

module.exports.DatabaseTablesEntry = DatabaseTables;
