var _ = require('underscore');
var crypto = require('crypto');

function DatabaseTables(tables) {
    this.key_namespace = 't';
    this.tables = tables;
}

module.exports = DatabaseTables;

DatabaseTables.prototype.key = function() {
    return this.tables.map(function(table) {
        return this.key_namespace + ':' + shortHashKey(table.dbname + ':' + table.table_name + '.' + table.schema_name);
    }.bind(this)).sort();
};

/**
 * Returns the calculated X-Cache-Channel for all of the tables.
 * @returns {String} 
 */
DatabaseTables.prototype.getCacheChannel = function() {
    var groupedTables = _(this.tables).groupBy('dbname');
    return _.map(groupedTables, function(tables, dbname) {
        return dbname + ':' + (tables.map(function(table) {
            return table.schema_name + "." + table.table_name;
        }));
    }).join(";;");
};


/**
 * Gets last updated_at date from all the tables.
 * @returns {Date}
 */
DatabaseTables.prototype.getLastUpdatedAt = function(fallbackValue) {
    if(fallbackValue === undefined) fallbackValue = 0;
    updatedTimes = _.map(this.tables, 'updated_at');
    return (this.tables.length === 0 ? fallbackValue : Math.max.apply(null, updatedTimes)) || fallbackValue;
};

function shortHashKey(target) {
    return crypto.createHash('sha256').update(target).digest('base64').substring(0,6);
}
