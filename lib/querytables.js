'use strict';

var DatabaseTables = require('./models/database_tables');

var affectedTableRegexCache = {
    bbox: /!bbox!/g,
    scale_denominator: /!scale_denominator!/g,
    pixel_width: /!pixel_width!/g,
    pixel_height: /!pixel_height!/g,
    var_zoom: /@zoom/g,
    var_bbox: /@bbox/g,
    var_x: /@x/g,
    var_y: /@y/g,
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
    checkForeignTable(pg, sql, function (err, hasForeignTable) {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error('could not fetch affected tables or last updated time: ' + msg));
        }

        if (hasForeignTable) {
            return callback(null, null);
        }

        const query = 'SELECT * FROM CDB_QueryTables_Updated_At($windshaft$' + prepareSql(sql) + '$windshaft$)';

        pg.query(query, function handleAffectedTablesAndLastUpdatedTimeRows (err, result) {
            if (err) {
                var msg = err.message ? err.message : err;
                return callback(new Error('could not fetch affected tables or last updated time: ' + msg));
            }
            result = result || {};
            var rows = result.rows || [];

            callback(null, new DatabaseTables(rows));
        });
    });
};

function checkForeignTable (pg, sql, callback) {
    const query = `
        WITH
        query_tables AS (
            SELECT unnest(CDB_QueryTablesText($windshaft$${prepareSql(sql)}$windshaft$)) schema_table_name
        ),
        query_tables_oid AS (
            SELECT schema_table_name, schema_table_name::regclass::oid AS reloid
            FROM query_tables
        )
        SELECT
            current_database()::text AS dbname,
            quote_ident(n.nspname::text) schema_name,
            quote_ident(c.relname::text) table_name,
            c.relkind,
            query_tables_oid.reloid
        FROM query_tables_oid, pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = query_tables_oid.reloid
    `;

    pg.query(query, function (err, result) {
        if (err) {
            return callback(err);
        }

        result = result || {};
        const rows = result.rows || [];

        const hasForeignTables = rows.some(row => row.relkind === 'f');

        return callback(null, hasForeignTables);
    });
}

function prepareSql(sql) {
    return sql
        .replace(affectedTableRegexCache.bbox, 'ST_MakeEnvelope(0,0,0,0)')
        .replace(affectedTableRegexCache.scale_denominator, '0')
        .replace(affectedTableRegexCache.pixel_width, '1')
        .replace(affectedTableRegexCache.pixel_height, '1')
        .replace(affectedTableRegexCache.var_zoom, '0')
        .replace(affectedTableRegexCache.var_bbox, '[0,0,0,0]')
        .replace(affectedTableRegexCache.var_x, '0')
        .replace(affectedTableRegexCache.var_y, '0')
    ;
}

module.exports.DatabaseTablesEntry = DatabaseTables;
