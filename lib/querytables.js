'use strict';

const DatabaseTables = require('./models/database_tables');
const SubstitutionTokens = require('./utils/substitution_tokens');

function replaceTile0 (sql) {
    return SubstitutionTokens.replaceXYZ(sql, {z : 0});
}

/**
 * Returns a DatabaseTables Object that includes the information about the tables
 * affected by a query (as seen by the planner)
 * @param {Object} pg         - Database connection (PSQL)
 * @param {String} sql        - Database query
 * @param {Function} callback - Cb function ({Error}, {Object::DatabaseTablesEntry})
 */
module.exports.getQueryMetadataModel = function (pg, sql, callback) {
    const query = `SELECT * FROM CDB_QueryTables_Updated_At($cdb_query$ ${replaceTile0(sql)} $cdb_query$)`;

    pg.query(query, (err, result) => {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error('Could not fetch metadata about the affected tables: ' + msg));
        }
        result = result || {};
        const rows = result.rows || [];

        callback(null, new DatabaseTables(rows));
    });
};


module.exports.DatabaseTablesEntry = DatabaseTables;
