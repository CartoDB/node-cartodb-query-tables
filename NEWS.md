# Version 0.6.3

Released 2019-09-19
  * getQueryMetadataModel: Serialize SQL requests using async / await

# Version 0.6.2

Released 2019-09-18
  * getQueryMetadataModel: Serialize SQL requests

# Version 0.6.1

Released 2019-09-16
  * querytables.js: Make having a remote metadata table optional

# Version 0.6.0

Released 2019-09-13
  * package.json: Only support officially node 10+
  * Add webmercator_helper with utilities to calculate tile extent and resolution
  * Add substitution_tokens with utilities to replace tokens in SQL queries
  * Breaking: Switched from database_tables.js to query_metadata.js
    * Uses ES6 class.
  * querytables.js: Stop using cartodb-postgresql PG extension

# Version 0.5.0

Released 2019-07-09
 * `.getTables()`: Add parameter to skip analysis tables from results


# Version 0.4.0

Released 2018-11-21

 * Support Node.js 8 and 10
 * Add package-lock.json


# Version 0.3.0

Released 2017-09-25

 * Allow zoom, x, y, bbox query variables.


# Version 0.2.0

Released 2016-07-11

 * Adds getTables public method.
 * Adds option to retrieve keys and cache channels for tables with updated_at.


# Version 0.1.0

Released 2016-03-08

 * First release
