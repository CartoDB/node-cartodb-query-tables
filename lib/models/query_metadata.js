'use strict';

const crypto = require('crypto');

class QueryMetadata {
    constructor(tables) {
        this.key_namespace = 't';
        this.tables = tables;
    }

    key(skipNotUpdatedAtTables) {
        return this.getTables(skipNotUpdatedAtTables).map(table => {
            return  this.key_namespace + ':' +
                    shortHashKey(table.dbname + ':' + table.schema_name + '.' + table.table_name);
        }).sort();
    }

    /**
     * Returns the calculated X-Cache-Channel for all of the tables.
     * @returns {String}
     */
    getCacheChannel(skipNotUpdatedAtTables) {
        const groupedTables = this.getTables(skipNotUpdatedAtTables).reduce((grouped, table) => {
            if (!grouped.hasOwnProperty(table.dbname)) {
                grouped[table.dbname] = [];
            }
            grouped[table.dbname].push(table);
            return grouped;
        }, {});

        return Object.keys(groupedTables).map(function(dbname) {
            return dbname + ':' + (groupedTables[dbname].map(function(table) {
                return table.schema_name + '.' + table.table_name;
            }));
        }).join(';;');
    }

    getLastUpdatedAt(fallbackValue = 0) {
        const updatedTimes = this.tables.map(function(table) { return table.updated_at; });
        return (this.tables.length === 0 ? fallbackValue : Math.max.apply(null, updatedTimes)) || fallbackValue;
    }

    getTables(skipNotUpdatedAtTables = false, skipAnalysisCachedTables = false) {
        let tables = skipNotUpdatedAtTables ? this.tables.filter(filterTablesWithUpdatedAt) : this.tables;
        tables = skipAnalysisCachedTables ? tables.filter(filterAnalysisTables) : tables;

        return tables;
    }

}


function shortHashKey(target) {
    return crypto.createHash('sha256').update(target).digest('base64').substring(0,6);
}

function filterTablesWithUpdatedAt(table) {
    return table.hasOwnProperty('updated_at') && table.updated_at !== undefined && table.updated_at !== null;
}

const analysisTableRegex = /^analysis_[a-f0-9]{10}_[a-f0-9]{40}$/;

function filterAnalysisTables (table) {
    return table.hasOwnProperty('table_name') && !analysisTableRegex.test(table.table_name);
}

module.exports = QueryMetadata;