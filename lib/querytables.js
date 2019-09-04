'use strict';

const QueryMetadataModel = require('./models/query_metadata');
const SubstitutionTokens = require('./utils/substitution_tokens');

function replaceTile0 (sql) {
    return SubstitutionTokens.replaceXYZ(sql, {z : 0});
}

/**
 * Given a plan / subplan, extracts the tables affected by it
 * @param {Object} plan
 * @returns {Array of Objects [schema : string, name : string]}
 */
function extractTablesFromPlan(plan) {
    let qualified_tables = [];
    if (plan.hasOwnProperty('Schema') && plan.hasOwnProperty('Relation Name')) {
        qualified_tables.push({ schema_name : plan.Schema, table_name : plan['Relation Name']});
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
 * @param {type} callback   - Cb function ({Error}, {Object Array})
 */
function getAffectedTables (pg, sql, callback) {
    /* We use the `SELECT * FROM {sql}` form here to detect and error on multiqueries sooner */
    const query = `EXPLAIN (FORMAT JSON, VERBOSE) SELECT * FROM (${trimSQL(sql)}) __cdb_affected_tables_query`;
    pg.query(query, {}, (err, result) => {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error('Could not fetch metadata about the affected tables: ' + msg));
        }
        const rows = result.rows || [];
        let qualified_tables = [];
        rows.forEach(row => {
            if (row.hasOwnProperty('QUERY PLAN')) {
                row['QUERY PLAN'].forEach(p => {
                    qualified_tables = qualified_tables.concat(extractTablesFromPlan(p.Plan));
                });
            }
        });
        return callback(null, [...new Set(qualified_tables)]);
    }, true);
}


/**
 * Trims starting and ending whitespaces
 * Trims starting and ending whitespaces ';'
 * @param {String} sql
 * @returns {String}
 */
function trimSQL (sql) {
    let trimmed = sql.trim();
    let i;
    for (i = 0; i < sql.length && sql[i] === ';'; i++) {}
    trimmed = trimmed.substr(i);
    trimmed = trimmed.replace(/\s*;\s*$/, '');
    return trimmed;
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


function encodePGName (name) {
    return name.replace("'", "''");
}

function getTablesMetadata (pg, tableArray, callback) {
    /* Note: We propagate and order by id_name last to avoid order changes due to parallelism */

    let sql = `WITH cdb_table_names AS (
        SELECT format('%s.%s', quote_ident('${encodePGName(tableArray[0].schema_name)}'),
                               quote_ident('${encodePGName(tableArray[0].table_name)}')) as id_name
    `;

    for (let i = 1; i < tableArray.length; i++) {
        sql += `UNION ALL
        SELECT format('%s.%s', quote_ident('${encodePGName(tableArray[i].schema_name)}'),
                               quote_ident('${encodePGName(tableArray[i].table_name)}')) as id_name
        `;
    }

    sql += `
        ), cdb_table_oids AS (
            SELECT DISTINCT id_name, id_name::regclass::oid AS reloid FROM cdb_table_names
        ), cdb_table_metadata AS (
            SELECT
                quote_ident(n.nspname::text) schema_name,
                quote_ident(c.relname::text) table_name,
                c.relkind,
                cdb_table_oids.*
            FROM cdb_table_oids, pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = cdb_table_oids.reloid
        ),
        cdb_database_name AS (
            SELECT  reloid,
                    option_value AS dbname FROM cdb_table_metadata, pg_options_to_table((
                        SELECT fs.srvoptions
                        FROM pg_foreign_table ft
                        LEFT JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
                        WHERE ft.ftrelid = cdb_table_metadata.reloid
                    )) WHERE option_name='dbname'
        )
        SELECT  cdb_table_metadata.*,
                COALESCE(cdb_database_name.dbname, current_database()) AS dbname
        FROM cdb_table_metadata FULL JOIN cdb_database_name
            ON cdb_table_metadata.reloid = cdb_database_name.reloid
        ORDER BY id_name`;

    //  TODO: UpdatedAt
    pg.query(sql, {}, (err, result) => {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error('Could not fetch metadata about the affected tables: ' + msg));
        }
        const rows = result.rows || [];
        return callback(null, rows);
    }, true);
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
    
    const statements = getQueryStatements(iSQL);
    let promiseCollection = [];
    statements.forEach(query => {
        promiseCollection.push(new Promise((resolve, reject) => {
            getAffectedTables(pg, query, function (err, result) {
                if (err) {
                    return reject(err);
                }
                return resolve(result);
            });
        }));
    });

    Promise.all(promiseCollection)
        .then((values) => {
            const merged = [].concat.apply([], values);
            if (merged.length === 0) {
                return callback(null, new QueryMetadataModel([]));
            }

            getTablesMetadata(pg, merged, (err, metadata) => {
                if (err) {
                    return callback(err);
                }

                let rows = metadata || [];
                return callback(null, new QueryMetadataModel(rows));
            });
        })
        .catch(err => callback(err));

//    const query = `SELECT * FROM CDB_QueryTables_Updated_At($cdb_query$ ${iSQL} $cdb_query$)`;
//    pg.query(query, (err, result) => {
//        if (err) {
//            const msg = err.message ? err.message : err;
//            return callback(new Error('Could not fetch metadata about the affected tables: ' + msg));
//        }
//        result = result || {};
//        const rows = result.rows || [];
//
//        callback(null, new QueryMetadataModel(rows));
//    });
};


module.exports.QueryMetadata = QueryMetadataModel;
