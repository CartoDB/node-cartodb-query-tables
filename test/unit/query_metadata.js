'use strict';

const assert = require('assert');
const QueryMetadata = require('../../lib/models/query_metadata');

describe('QueryMetadata', function() {

    describe('getCacheChannel', function() {
        it('should group cache-channel tables by database name', function() {
            var tables = new QueryMetadata([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
            ]);

            assert.equal(tables.getCacheChannel(), 'db1:public.tableone,public.tabletwo');
        });

        it('should support tables coming from different databases', function() {
            var tables = new QueryMetadata([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                {dbname: 'db2', schema_name: 'public', table_name: 'tablethree'}
            ]);

            assert.equal(tables.getCacheChannel(), 'db1:public.tableone,public.tabletwo;;db2:public.tablethree');
        });

        describe('skipNotUpdatedAtTables', function() {
            var scenarios = [
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
                    ],
                    expectedCacheChannel: ''
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: null},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
                    ],
                    expectedCacheChannel: ''
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: undefined},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
                    ],
                    expectedCacheChannel: ''
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
                    ],
                    expectedCacheChannel: 'db1:public.tableone'
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo', updated_at: Date.now()}
                    ],
                    expectedCacheChannel: 'db1:public.tableone,public.tabletwo'
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                        {dbname: 'db2', schema_name: 'public', table_name: 'tablethree', updated_at: Date.now()}
                    ],
                    expectedCacheChannel: 'db1:public.tableone;;db2:public.tablethree'
                }
            ];
            scenarios.forEach(function(scenario) {
                it('should get an cache channel skipping tables with no updated_at', function() {
                    var tables = new QueryMetadata(scenario.tables);

                    var cacheChannel = tables.getCacheChannel(true);
                    assert.equal(cacheChannel, scenario.expectedCacheChannel);
                });
            });
        });
    });

    describe('getLastUpdatedAt', function() {

        it('should return latest of the known dates', function() {
            var tables = new QueryMetadata([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: new Date(12345678)},
                {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo', updated_at: new Date(1234567891)},
                {dbname: 'db2', schema_name: 'public', table_name: 'tablethree', updated_at: null}
            ]);
            assert.equal(tables.getLastUpdatedAt(), 1234567891);
        });

        it('getSafeLastUpdatedAt should return fallback date if a table date is unknown', function() {
            var tables = new QueryMetadata([
                {dbname: 'db2', schema_name: 'public', table_name: 'tablethree', updated_at: null}
            ]);
            assert.equal(tables.getLastUpdatedAt('FALLBACK'), 'FALLBACK');
        });

        it('getSafeLastUpdatedAt should return fallback date if no tables were found', function() {
            var tables = new QueryMetadata([]);
            assert.equal(tables.getLastUpdatedAt('FALLBACK'), 'FALLBACK');
        });
    });

    describe('key', function() {

        var KEY_LENGTH = 8;

        it('should get an array of keys for multiple tables', function() {
            var tables = new QueryMetadata([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
            ]);

            var keys = tables.key();
            assert.equal(keys.length, 2);
            assert.equal(keys[0].length, KEY_LENGTH);
            assert.equal(keys[1].length, KEY_LENGTH);
        });

        it('should return proper surrogate-key (db:schema.table)', function() {
            var tables = new QueryMetadata([
                {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: new Date(12345678)},
            ]);
            assert.deepEqual(tables.key(), ['t:8ny9He']);
        });
        it('should keep escaped tables escaped (db:"sch-ema".table)', function() {
            var tables = new QueryMetadata([
                {dbname: 'db1', schema_name: '"sch-ema"', table_name: 'tableone', updated_at: new Date(12345678)},
            ]);
            assert.deepEqual(tables.key(), ['t:oVg75u']);
        });

        describe('skipNotUpdatedAtTables', function() {
            var scenarios = [
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
                    ],
                    expectedLength: 0
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: null},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
                    ],
                    expectedLength: 0
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: undefined},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
                    ],
                    expectedLength: 0
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'}
                    ],
                    expectedLength: 1
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo', updated_at: Date.now()}
                    ],
                    expectedLength: 2
                },
                {
                    tables: [
                        {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                        {dbname: 'db1', schema_name: 'public', table_name: 'tablethree', updated_at: Date.now()}
                    ],
                    expectedLength: 2
                }
            ];
            scenarios.forEach(function(scenario) {
                it('should get an array for multiple tables skipping the ones with no updated_at', function() {
                    var tables = new QueryMetadata(scenario.tables);

                    var keys = tables.key(true);
                    assert.equal(keys.length, scenario.expectedLength);
                    keys.forEach(function(key) {
                        assert.equal(key.length, KEY_LENGTH);
                    });
                });
            });
        });
    });

    describe('getTables', function () {
        const scenarios = [
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: false,
                expectedLength: 0
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: null},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: false,
                expectedLength: 0
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: undefined},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: false,
                expectedLength: 0
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: false,
                expectedLength: 2
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo', updated_at: Date.now()},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: false,
                expectedLength: 3
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tablethree', updated_at: Date.now()},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: false,
                expectedLength: 3
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: false,
                skipAnalysisCachedTables: true,
                expectedLength: 2
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: null},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: false,
                skipAnalysisCachedTables: true,
                expectedLength: 2
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: undefined},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: false,
                skipAnalysisCachedTables: true,
                expectedLength: 2
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: false,
                skipAnalysisCachedTables: true,
                expectedLength: 2
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo', updated_at: Date.now()},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: false,
                skipAnalysisCachedTables: true,
                expectedLength: 2
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tablethree', updated_at: Date.now()},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: false,
                skipAnalysisCachedTables: true,
                expectedLength: 3
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone'},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: true,
                expectedLength: 0
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: null},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: true,
                expectedLength: 0
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: undefined},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34'
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: true,
                expectedLength: 0
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: true,
                expectedLength: 1
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo', updated_at: Date.now()},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: true,
                expectedLength: 2
            },
            {
                result: [
                    {dbname: 'db1', schema_name: 'public', table_name: 'tableone', updated_at: Date.now()},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tabletwo'},
                    {dbname: 'db1', schema_name: 'public', table_name: 'tablethree', updated_at: Date.now()},
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'analysis_b194a8f896_81cc00c1cfbd5c04d3375fc0e0343a34ae979f34',
                        updated_at: Date.now()
                    }
                ],
                skipNotUpdatedAtTables: true,
                skipAnalysisCachedTables: true,
                expectedLength: 2
            }
        ];

        scenarios.forEach(function ({ result, skipNotUpdatedAtTables, skipAnalysisCachedTables, expectedLength }) {
            const filterUpdatedAt = skipNotUpdatedAtTables ? 'in' : 'out';
            const filterAnalysisTables = skipAnalysisCachedTables ? 'in' : 'out';
            const arrayLengthCond = `an array of ${expectedLength} items`;
            const updatedAtCond = `filtering ${filterUpdatedAt} updated_at`;
            const analysisTablesCond = `filtering ${filterAnalysisTables} analysis tables`;

            it(`should get ${arrayLengthCond} by ${updatedAtCond} and ${analysisTablesCond}`, function () {
                const queryMetadata = new QueryMetadata(result);
                const tables = queryMetadata.getTables(skipNotUpdatedAtTables, skipAnalysisCachedTables);

                assert.equal(tables.length, expectedLength);
            });
        });
    });
});
