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

DatabaseTables.prototype.getCacheChannel = function() {
    var key = this.tables.map(function(table) {
        return table.dbname + ':' + table.schema_name + "." + table.table_name;
    }).sort().join(";;");
    return key;
};


DatabaseTables.prototype.getLastUpdatedAt = function() {
    updatedTimes = this.tables.map(function getUpdateDate(table) {
        return table.updated_at;
    });
    return (this.tables.length === 0 ? 0 : Math.max.apply(null, updatedTimes)) || 0;
};

function shortHashKey(target) {
    return crypto.createHash('sha256').update(target).digest('base64').substring(0,6);
}
