'use strict';

const QueryMetadataModel = require('./models/query_metadata');
const SubstitutionTokens = require('./utils/substitution_tokens');
const { promisify } = require('util');

function replaceTile0 (sql) {
    return SubstitutionTokens.replaceXYZ(sql, {z : 0});
}

/** Flattens an array */
function flat (arr) {
    return [].concat(...arr);
}


/**
 * Given a plan / subplan, extracts the tables affected by it
 * @param {Object} plan
 * @returns {Array of Objects [schema : string, name : string]}
 */
function extractTablesFromPlan(plan = {}) {
    let qualifiedTables = [];
    if (plan.hasOwnProperty('Schema') && plan.hasOwnProperty('Relation Name')) {
        qualifiedTables.push({ schema_name : plan.Schema, table_name : plan['Relation Name']});
    }

    if (plan.hasOwnProperty('Plans')) {
        plan.Plans.forEach(p => {
            qualifiedTables = qualifiedTables.concat(extractTablesFromPlan(p));
        });
    }

    return qualifiedTables;
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
            /* We can get a syntax error if the user tries to EXPLAIN a DDL */
            if (err.hasOwnProperty('code') && err.code === '42601') {
                /* 42601 comes from Postgres' errcodes.txt:
                 *      42601    E    ERRCODE_SYNTAX_ERROR syntax_error */
                return callback(null, []);
            }
            const msg = err.message ? err.message : err;
            return callback(new Error(`Could not fetch metadata about the affected tables: ${msg}`));
        }
        const { rows = [] } = result;
        let qualifiedTables = [];
        rows.filter(row => row.hasOwnProperty('QUERY PLAN')).forEach(row => {
            row['QUERY PLAN'].forEach(p => {
                qualifiedTables = qualifiedTables.concat(extractTablesFromPlan(p.Plan));
            });
        });
        return callback(null, [...new Set(qualifiedTables)]);
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

/**
 * Replaces ' for '' to overcome issues when forming the qualified name using the data from the plan
 */
function encodePGName (name) {
    return name.replace("'", "''");
}

/**
 * This is based on the cartodb-postgresql function `CDB_Get_Foreign_Updated_At`
 * @param {Object}   pg         Database connection (PSQL)
 * @param {Object}   table      {id_name, reloid }
 * @param {Function} callback
 */
function getForeignTableUpdatedAt (pg, table, callback) {
    const remoteNameQuery = `
        WITH cdb_fdw_option_row AS
        (
            SELECT ftoptions FROM pg_foreign_table WHERE ftrelid='${encodePGName(table.id_name)}'::regclass LIMIT 1
        ),
        cdb_fdw_option_table AS
        (
            SELECT (pg_options_to_table(ftoptions)).* FROM cdb_fdw_option_row
        )
        SELECT
            FORMAT('%I.%I',
                (SELECT option_value FROM cdb_fdw_option_table WHERE option_name='schema_name'),
                (SELECT option_value FROM cdb_fdw_option_table WHERE option_name='table_name')
                ) AS cdb_fdw_qual_name
        `;

    const readOnly = true;
    pg.query(remoteNameQuery, {}, (err, result) => {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error(`Could not fetch metadata for relation '${table.id_name}': ${msg}`));
        }

        const { rows = [] } = result;
        const foreignQualifiedName = rows.length > 0 ? rows[0].cdb_fdw_qual_name : null;
        if (foreignQualifiedName === null) {
            return callback(new Error(`Could not fetch remote qualified name for relation '${table.id_name}'`));
        }

        /* We assume that the remote cdb_tablemetadata is called cdb_tablemetadata
         * and is on the same schema as the queried table. */
        const remoteMetadataTable = `${encodePGName(table.schema_name)}.cdb_tablemetadata`;
        const remoteUpdatedAtQuery = `
            SELECT updated_at
            FROM ${remoteMetadataTable}
            WHERE tabname='${encodePGName(foreignQualifiedName)}'
            ORDER BY updated_at DESC LIMIT 1
        `;

        pg.query(remoteUpdatedAtQuery, {}, (err, result) => {
            if (err) {
                /* The remote cdb_tablemetadata might not exists */
                if (err.hasOwnProperty('code') && err.code === '42P01') {
                    /* 42P01 comes from Postgres' errcodes.txt:
                     *      42P01    E    ERRCODE_UNDEFINED_TABLE undefined_table */
                    return callback(null, null);
                }
                const msg = err.message ? err.message : err;
                return callback(new Error(`Could not fetch update time for relation '${table.id_name}': ${msg}`));
            }

            const { rows = [] } = result;
            const updatedAt = rows.length > 0 ? rows[0].updated_at : null;
            return callback(null, updatedAt);
        }, readOnly);

    }, readOnly);
}

/* For foreign tables we need to do extra queries to extract the updated_at properly */
async function setUpdateAtToForeingTables (pg, rows) {
    const getForeignTableUpdatedAtPromise = promisify(getForeignTableUpdatedAt);
    for (const row of rows) {
        if (row.relkind === 'f') {
            row.updated_at = await getForeignTableUpdatedAtPromise(pg, row);
        }
    }
    return rows;
}

/* Extracts the metadata necessary from a list of tables
 * * id_name        - Fully qualified name ({schema}.{tablename})
 * * reloid         - Table OID
 * * schema_name    - LOCAL schema where the table is placed
 * * table_name     - LOCAL table name
 * * relkind        - Table type (https://www.postgresql.org/docs/current/catalog-pg-class.html)
 * * dbname         - Database name. For foreign tables this is the remote database
 * * updated_at     - Last update time according to the CDB_TableMetadata tables
 *
 * This query is based on the following cartodb-postgresql functions:
 * -` CDB_QueryTables_Updated_At`
 * - `_cdb_dbname_of_foreign_table`
 * - `CDB_Get_Foreign_Updated_At` via getForeignTableUpdatedAt
 */
function getTablesMetadata (pg, tableArray, callback) {
    /* Note: We order by **reloid** because that's the implicit behaviour of CDB_QueryTables_Updated_At
     * Eventhough `CDB_QueryTablesText` orders alphabetically (by our id_name), the unnest call breaks
     * that ordering and somehow (PG internals) the subsequent calls end up ordering the table names
     * by their `::regclass::oid` */

    let metadataQuery = `WITH cdb_table_names AS (
        SELECT format('%s.%s', quote_ident('${encodePGName(tableArray[0].schema_name)}'),
                               quote_ident('${encodePGName(tableArray[0].table_name)}')) as id_name
    `;

    for (let i = 1; i < tableArray.length; i++) {
        metadataQuery += `UNION ALL
        SELECT format('%s.%s', quote_ident('${encodePGName(tableArray[i].schema_name)}'),
                               quote_ident('${encodePGName(tableArray[i].table_name)}')) as id_name
        `;
    }

    metadataQuery += `
        ), cdb_table_oids AS (
            SELECT DISTINCT id_name, id_name::regclass::oid AS reloid FROM cdb_table_names
        ), cdb_table_metadata AS (
            SELECT
                quote_ident(n.nspname::text) schema_name,
                quote_ident(c.relname::text) table_name,
                c.relkind,
                cdb_table_oids.*,
                (SELECT md.updated_at FROM cartodb.CDB_TableMetadata md WHERE md.tabname = reloid) AS updated_at,
                (CASE   WHEN relkind != 'f' THEN current_database()
                        ELSE (
                            SELECT option_value AS dbname FROM cdb_table_oids, pg_options_to_table((
                                SELECT fs.srvoptions
                                FROM pg_foreign_table ft
                                LEFT JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
                                WHERE ft.ftrelid = cdb_table_oids.reloid
                            )) WHERE option_name='dbname')
                         END) AS dbname
            FROM cdb_table_oids, pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = cdb_table_oids.reloid
        )
        SELECT * FROM cdb_table_metadata ORDER BY reloid;`;

    const readOnly = true;
    pg.query(metadataQuery, {}, (err, result) => {
        if (err) {
            const msg = err.message ? err.message : err;
            return callback(new Error(`Could not fetch metadata about the affected tables: ${msg}`));
        }
        const { rows = [] } = result;

        setUpdateAtToForeingTables(pg, rows)
            .then(tablesMetadata => callback(null, tablesMetadata))
            .catch(err => callback(err));
    }, readOnly);
}


/**
 * Returns a QueryMetadata Model that includes the information about the tables
 * affected by a query (as seen by the planner)
 * @param {Object} pg         - Database connection (PSQL)
 * @param {String} sql        - Database query
 * @param {Function} callback - Cb function ({Error}, {Object::QueryMetadataModel})
 */
module.exports.getQueryMetadataModel = async function (pg, sql, callback) {
    const iSQL = replaceTile0(sql);

    const statements = getQueryStatements(iSQL);
    const getAffectedTablesPromise = promisify(getAffectedTables);

    const result = [];
    try {
        for (const query of statements) {
            result.push(...await getAffectedTablesPromise(pg, query));
        }
    } catch (err) {
        return callback(err);
    }

    const merged = flat(result);
    if (merged.length === 0) {
        return callback(null, new QueryMetadataModel([]));
    }

    const getTablesMetadataPromise = promisify(getTablesMetadata);
    return getTablesMetadataPromise(pg, merged)
        .then((metadata = []) => callback(null, new QueryMetadataModel(metadata)))
        .catch(err => callback(err));
};


module.exports.QueryMetadata = QueryMetadataModel;