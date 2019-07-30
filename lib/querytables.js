'use strict';

const QueryMetadataModel = require('./models/query_metadata');
const SubstitutionTokens = require('./utils/substitution_tokens');

function replaceTile0 (sql) {
    return SubstitutionTokens.replaceXYZ(sql, {z : 0});
}

/**
 * Given a query, returns the list of tables affected by it (as seen by the planner)
 * @param {type} pg         - Database connection (PSQL)
 * @param {type} sql        - Database query
 * @param {type} callback   - Cb function ({Error}, {String Array})
 */
function getAffectedTables (pg, sql, callback) {
    const query = `EXPLAIN (FORMAT JSON, VERBOSE) ${sql}`;

    pg.query(query, (err, result) => {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error('Could not fetch metadata about the affected tables: ' + msg));
        }
        const rows = result.rows || [];
        rows.forEach(row => {
            // Parse plan, look for "Relation Name" and "Schema"
            // Check views, materialized views, etc
        });
        console.log(query);
        console.log(JSON.stringify(result, null, 2));
        callback(null, null);
    });
}


/**
 * Returns a DatabaseTables Object that includes the information about the tables
 * affected by a query (as seen by the planner)
 * @param {Object} pg         - Database connection (PSQL)
 * @param {String} sql        - Database query
 * @param {Function} callback - Cb function ({Error}, {Object::DatabaseTablesEntry})
 */
module.exports.getQueryMetadataModel = function (pg, sql, callback) {
    const iSQL = replaceTile0(sql);
    const query = `SELECT * FROM CDB_QueryTables_Updated_At($cdb_query$ ${iSQL} $cdb_query$)`;

    getAffectedTables(pg, iSQL, (err, result) => {
//        pg.query(query, (err, result) => {
            if (err) {
                const msg = err.message ? err.message : err;
                return callback(new Error('Could not fetch metadata about the affected tables: ' + msg));
            }
            result = result || {};
            const rows = result.rows || [];

            callback(null, new QueryMetadataModel(rows));
//        });
    });
};


module.exports.QueryMetadata = QueryMetadataModel;
