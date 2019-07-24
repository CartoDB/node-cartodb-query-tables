'use strict';

const DatabaseTables = require('./models/database_tables');
const SubstitutionTokens = require('./utils/substitution_tokens');

function sql_t0 (sql) {
    return SubstitutionTokens.replaceXYZ(sql, {z : 0});
}

module.exports.getAffectedTableNamesFromQuery = function (pg, sql, callback) {
    const query = `SELECT CDB_QueryTablesText($cdb_query$' ${sql_t0(sql)} '$cdb_query$)`;

    pg.query(query, (err, result) => {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error('Could not fetch source tables: ' + msg));
        }

        result = result || {};
        const rows = result.rows || [];

        // This is an Array, so no need to split into parts
        const tableNames = (!!rows[0]) ? rows[0].cdb_querytablestext : [];
        return callback(null, tableNames);
    });
};

module.exports.getAffectedTablesFromQuery = function (pg, sql, callback) {
    const query = `SELECT * FROM CDB_QueryTables_Updated_At($cdb_query$' ${sql_t0(sql)} '$cdb_query$)`;

    pg.query(query, (err, result) => {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error('could not fetch affected tables or last updated time: ' + msg));
        }
        result = result || {};
        const rows = result.rows || [];

        callback(null, new DatabaseTables(rows));
    });
};


module.exports.DatabaseTablesEntry = DatabaseTables;
