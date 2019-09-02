'use strict';

const QueryMetadataModel = require('./models/query_metadata');
const SubstitutionTokens = require('./utils/substitution_tokens');

function replaceTile0 (sql) {
    return SubstitutionTokens.replaceXYZ(sql, {z : 0});
}


function extractTablesFromPlan(plan) {
    let qualified_tables = [];
    if (plan.hasOwnProperty('Schema') && plan.hasOwnProperty('Relation Name')) {
        qualified_tables.push(plan.Schema + '.' + plan['Relation Name']);
    }

    if (plan.hasOwnProperty('Plans')) {
        plan.Plans.forEach(p => {
            qualified_tables = qualified_tables.concat(extractTablesFromPlan(p));
        });
    }

    return qualified_tables;
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
        let qualified_tables = [];
        rows.forEach(row => {
            row['QUERY PLAN'].forEach(p => {
                qualified_tables = qualified_tables.concat(extractTablesFromPlan(p.Plan));
            });
        });
        callback(null, [...new Set(qualified_tables)]);
    });
}


function trimSQL (sql) {
    const trimmedWhitepaces = sql.trim();
    let i;
    for (i = 0; i < sql.length && sql[i] === ';'; i++) {}
    return trimmedWhitepaces.substr(i);
}

/**
 * Given a string, returns an array of statements found in it
 * @param {type} sql (e.g: "Select * from t1; Select * from t2;")
 * @returns {Array} (e.g. ["Select * from t1", "Select * from t2"])
 */
function getQueryStatements (sql) {
    /* Ignore warning about 'DotAll RegExp flag', available since node 8.10.0 */
    /* jshint -W119 */
    const regex = /((?:[^'"$;]+|"[^"]*"|'[^']*'|(\$[^$]*\$).*?\2)+)/sug;
    /* jshint +W119 */

    let array = [];
    const match = regex.exec(sql);
    if (match !== null) {
        array.push(trimSQL(match[0]));
        array = array.concat(getQueryStatements(sql.substring(regex.lastIndex + 1)));
    } else {
        array.push(sql);
    }

    return array.filter(q => q !== '');
}
module.exports.getQueryStatements = getQueryStatements;

/**
 * Returns a DatabaseTables Object that includes the information about the tables
 * affected by a query (as seen by the planner)
 * @param {Object} pg         - Database connection (PSQL)
 * @param {String} sql        - Database query
 * @param {Function} callback - Cb function ({Error}, {Object::DatabaseTablesEntry})
 */
module.exports.getQueryMetadataModel = function (pg, sql, callback) {
    const iSQL = replaceTile0(sql);
    const statements = getQueryStatements(iSQL);

    let promiseCollection = [];
    let affectedTables = [];
    statements.forEach(query => {
        promiseCollection.push(new Promise((resolve, reject) => {
            getAffectedTables(pg, query, function (err, result) {
                if (err) {
                    const msg = err.message ? err.message : err;
                    reject(new Error('Could not fetch metadata about the affected tables: ' + msg));
                    return;
                }

                affectedTables = affectedTables.concat(result);
                resolve(null);
            });
        }));
    });

    Promise.all(promiseCollection)
        .then(() => {
            const rows = [];
            callback(null, new QueryMetadataModel(rows));
        })
        .catch(err => callback(err));

//    const query = `SELECT * FROM CDB_QueryTables_Updated_At($cdb_query$ ${iSQL} $cdb_query$)`;
//
//    getAffectedTables(pg, iSQL, (err, result) => {
////        pg.query(query, (err, result) => {
//            if (err) {
//                const msg = err.message ? err.message : err;
//                return callback(new Error('Could not fetch metadata about the affected tables: ' + msg));
//            }
//            result = result || {};
            const rows = [];
//
            callback(null, new QueryMetadataModel(rows));
////        });
//    });
};


module.exports.QueryMetadata = QueryMetadataModel;
