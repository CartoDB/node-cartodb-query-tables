'use strict';

const QueryMetadataModel = require('./models/query_metadata');
const SubstitutionTokens = require('./utils/substitution_tokens');

module.exports.QueryMetadata = QueryMetadataModel;

function replaceTile0 (sql) {
    return SubstitutionTokens.replaceXYZ(sql, { z: 0 });
}

/** Flattens an array */
function flat (arr) {
    return [].concat(...arr);
}

function execQuery ({ pg, sql, params = {}, readOnly = true } = {}) {
    return new Promise((resolve, reject) => {
        pg.query(sql, params, (err, result) => err ? reject(err) : resolve(result.rows || []), readOnly);
    });
}

/**
 * Given a plan/subplan, extracts the tables affected by it
 *
 * @param {Object} plan
 * @returns {Array of Objects [{schema: string, name: string}]}
 */
function extractTablesFromPlan (plan = {}) {
    let qualifiedTables = [];
    if (Object.prototype.hasOwnProperty.call(plan, 'Schema') && Object.prototype.hasOwnProperty.call(plan, 'Relation Name')) {
        qualifiedTables.push({ schema_name: plan.Schema, table_name: plan['Relation Name'] });
    }

    if (Object.prototype.hasOwnProperty.call(plan, 'Plans')) {
        plan.Plans.forEach(p => {
            qualifiedTables = qualifiedTables.concat(extractTablesFromPlan(p));
        });
    }

    return qualifiedTables;
}

/**
 * Given a query, returns the list of tables affected by it (as seen by the planner)
 *
 * @param {Object} pg - Database connection (PSQL)
 * @param {String} sql - Database query
 * @throws {Error}
 * @returns {Promise<Array of Objects [schema_name : string, table_name : string]>}
 */
async function getAffectedTables (pg, sql) {
    try {
        /* We use the `SELECT * FROM {sql}` form here to detect and error on multiqueries sooner */
        const query = `EXPLAIN (FORMAT JSON, VERBOSE) SELECT * FROM (${trimSQL(sql)}) __cdb_affected_tables_query`;
        const rows = await execQuery({ pg, sql: query });

        let qualifiedTables = [];
        rows.filter(row => Object.prototype.hasOwnProperty.call(row, 'QUERY PLAN')).forEach(row => {
            row['QUERY PLAN'].forEach(p => {
                qualifiedTables = qualifiedTables.concat(extractTablesFromPlan(p.Plan));
            });
        });

        return [...new Set(qualifiedTables)];
    } catch (err) {
        /* We can get a syntax error if the user tries to EXPLAIN a DDL */
        if (Object.prototype.hasOwnProperty.call(err, 'code') && err.code === '42601') {
            /* 42601 comes from Postgres' errcodes.txt:
            *      42601    E    ERRCODE_SYNTAX_ERROR syntax_error */
            return [];
        }

        throw new Error(`Could not fetch metadata about the affected tables: ${err.message ? err.message : err}`);
    }
}

/**
 * Trims starting and ending whitespaces
 * Trims starting and ending whitespaces ';'
 *
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
 *
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
 *
 * @param {Object} pg database connection (PSQL)
 * @param {Object} table {id_name, reloid }
 * @throws {Error}
 * @returns {Promise<Date>}
 */
async function getForeignTableUpdatedAt (pg, table) {
    try {
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

        const remoteNameResult = await execQuery({ pg, sql: remoteNameQuery });
        const foreignQualifiedName = remoteNameResult.length > 0 ? remoteNameResult[0].cdb_fdw_qual_name : null;
        if (foreignQualifiedName === null) {
            throw new Error('Cannot find the names of the foreign tables');
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

        const remoteUpdatedAtResult = await execQuery({ pg, sql: remoteUpdatedAtQuery });
        const updatedAt = remoteUpdatedAtResult.length > 0 ? remoteUpdatedAtResult[0].updated_at : null;

        return updatedAt;
    } catch (err) {
        if (Object.prototype.hasOwnProperty.call(err, 'code') && err.code === '42P01') {
            /* 42P01 comes from Postgres' errcodes.txt:
            *      42P01    E    ERRCODE_UNDEFINED_TABLE undefined_table */
            return null;
        }

        throw new Error(`Could not fetch metadata for relation '${table.id_name}': ${err.message ? err.message : err}`);
    }
}

async function getViewUpdatedAt (pg, view) {
    const materializedViewDefinitionQuery = `select pg_get_viewdef(${view.reloid}, false) as definition`;

    const viewDefinition = await execQuery({ pg, sql: materializedViewDefinitionQuery });

    const query = viewDefinition[0].definition;
    const affectedTableView = await getAffectedTables(pg, query);

    const result = await getTablesMetadata(pg, affectedTableView);

    let latestTableMetadata = {};

    for (const tableMetadata of result) {
        if (!latestTableMetadata.updated_at) {
            latestTableMetadata = tableMetadata;
            continue;
        }

        const latestDate = new Date(latestTableMetadata.updated_at);
        const tableMetadataDate = new Date(tableMetadata.updated_at);

        if (tableMetadataDate.getTime() > latestDate.getTime()) {
            latestTableMetadata = tableMetadata;
        }
    }

    return latestTableMetadata.updated_at;
}

/* For foreign tables and materialized views to foreign tables
   we need to do extra queries to extract the updated_at properly */
async function setUpdateAtToNonRegularTables (pg, rows) {
    for (const row of rows) {
        switch (row.relkind) {
        case 'f': // foreign table
            row.updated_at = await getForeignTableUpdatedAt(pg, row);
            break;
        case 'm': // materialized view
        case 'v': // view
            if (!row.updated_at) {
                row.updated_at = await getViewUpdatedAt(pg, row);
            }
            break;
        default:
            continue;
        }
    }

    return rows;
}

/**
 * Extracts the metadata necessary from a list of tables
 *
 * This query is based on the following cartodb-postgresql functions:
 *      `CDB_QueryTables_Updated_At`
 *      `_cdb_dbname_of_foreign_table`
 *      `CDB_Get_Foreign_Updated_At` via getForeignTableUpdatedAt
 *
 * @param {Object} pg - Database connection (PSQL)
 * @param {Array of Objects [{schema_name: string, table_name: string}]} tableArray
 * @throws {Error}
 * @returns {Array of Objects [{TableMetadata}] where TableMetadata:
 *   id_name - Fully qualified name ({schema}.{tablename})
 *   reloid - Table OID
 *   schema_name - LOCAL schema where the table is placed
 *   table_name - LOCAL table name
 *   relkind - Table type (https://www.postgresql.org/docs/current/catalog-pg-class.html)
 *   dbname - Database name. For foreign tables this is the remote database
 *   updated_at - Last update time according to the CDB_TableMetadata tables
 */
async function getTablesMetadata (pg, tableArray) {
    try {
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
            SELECT * FROM cdb_table_metadata ORDER BY reloid;
        `;

        const metadata = await execQuery({ pg, sql: metadataQuery });
        const tablesMetadata = await setUpdateAtToNonRegularTables(pg, metadata);

        return tablesMetadata;
    } catch (err) {
        throw new Error(`Could not fetch metadata about the affected tables: ${err.message ? err.message : err}`);
    }
}

/**
 * Returns a QueryMetadata Model that includes the information about the tables
 * affected by a query (as seen by the planner)
 *
 * @param {Object} pg - Database connection (PSQL)
 * @param {String} sql - Database query
 * @throws {Error}
 * @returns {Object<QueryMetadataModel>}
 */
async function getQueryMetadataModel (pg, sql) {
    const iSQL = replaceTile0(sql);

    const statements = getQueryStatements(iSQL);

    const result = [];
    for (const query of statements) {
        result.push(...await getAffectedTables(pg, query));
    }

    const merged = flat(result);
    if (merged.length === 0) {
        return new QueryMetadataModel([]);
    }

    const metadata = await getTablesMetadata(pg, merged) || [];

    return new QueryMetadataModel(metadata);
};

module.exports.getQueryMetadataModel = getQueryMetadataModel;
