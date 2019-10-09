'use strict';

const assert = require('assert');
const QueryMetadata = require('../../lib/models/query_metadata');

describe('QueryMetadata', function () {
    describe('.getCacheChannel()', function () {
        it('should group cache-channel tables by database name', function () {
            const tables = new QueryMetadata([
                {
                    dbname: 'db1',
                    schema_name: 'public',
                    table_name: 'tableone'
                },
                {
                    dbname: 'db1',
                    schema_name: 'public',
                    table_name: 'tabletwo'
                }
            ]);

            assert.strictEqual(tables.getCacheChannel(), 'db1:public.tableone,public.tabletwo');
        });

        it('should support tables coming from different databases', function () {
            const tables = new QueryMetadata([
                {
                    dbname: 'db1',
                    schema_name: 'public',
                    table_name: 'tableone'
                },
                {
                    dbname: 'db1',
                    schema_name: 'public',
                    table_name: 'tabletwo'
                },
                {
                    dbname: 'db2',
                    schema_name: 'public',
                    table_name: 'tablethree'
                }
            ]);

            assert.strictEqual(tables.getCacheChannel(), 'db1:public.tableone,public.tabletwo;;db2:public.tablethree');
        });

        describe('with skipNotUpdatedAtTables enabled', function () {
            const skipNotUpdatedAtTables = true;
            const scenarios = [
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone'
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        }
                    ],
                    expectedCacheChannel: ''
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: null
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        }
                    ],
                    expectedCacheChannel: ''
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: undefined
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        }
                    ],
                    expectedCacheChannel: ''
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: Date.now()
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        }
                    ],
                    expectedCacheChannel: 'db1:public.tableone'
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: Date.now()
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo',
                            updated_at: Date.now()
                        }
                    ],
                    expectedCacheChannel: 'db1:public.tableone,public.tabletwo'
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: Date.now()
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        },
                        {
                            dbname: 'db2',
                            schema_name: 'public',
                            table_name: 'tablethree',
                            updated_at: Date.now()
                        }
                    ],
                    expectedCacheChannel: 'db1:public.tableone;;db2:public.tablethree'
                }
            ];
            scenarios.forEach(function (scenario) {
                it('should get an cache channel skipping tables with no updated_at', function () {
                    const tables = new QueryMetadata(scenario.tables);
                    const cacheChannel = tables.getCacheChannel(skipNotUpdatedAtTables);
                    assert.strictEqual(cacheChannel, scenario.expectedCacheChannel);
                });
            });
        });
    });

    describe('.getLastUpdatedAt()', function () {
        it('should return latest of the known dates', function () {
            const tables = new QueryMetadata([
                {
                    dbname: 'db1',
                    schema_name: 'public',
                    table_name: 'tableone',
                    updated_at: new Date(12345678)
                },
                {
                    dbname: 'db1',
                    schema_name: 'public',
                    table_name: 'tabletwo',
                    updated_at: new Date(1234567891)
                },
                {
                    dbname: 'db2',
                    schema_name: 'public',
                    table_name: 'tablethree',
                    updated_at: null
                }
            ]);
            assert.strictEqual(tables.getLastUpdatedAt(), 1234567891);
        });

        it('getSafeLastUpdatedAt should return fallback date if a table date is unknown', function () {
            const tables = new QueryMetadata([
                {
                    dbname: 'db2',
                    schema_name: 'public',
                    table_name: 'tablethree',
                    updated_at: null
                }
            ]);
            assert.strictEqual(tables.getLastUpdatedAt('FALLBACK'), 'FALLBACK');
        });

        it('getSafeLastUpdatedAt should return fallback date if no tables were found', function () {
            const tables = new QueryMetadata([]);
            assert.strictEqual(tables.getLastUpdatedAt('FALLBACK'), 'FALLBACK');
        });
    });

    describe('.key()', function () {
        const KEY_LENGTH = 8;

        it('should get an array of keys for multiple tables', function () {
            const tables = new QueryMetadata([
                {
                    dbname: 'db1',
                    schema_name: 'public',
                    table_name: 'tableone'
                },
                {
                    dbname: 'db1',
                    schema_name: 'public',
                    table_name: 'tabletwo'
                }
            ]);
            const keys = tables.key();

            assert.strictEqual(keys.length, 2);
            assert.strictEqual(keys[0].length, KEY_LENGTH);
            assert.strictEqual(keys[1].length, KEY_LENGTH);
        });

        it('should return proper surrogate-key (db:schema.table)', function () {
            const tables = new QueryMetadata([
                {
                    dbname: 'db1',
                    schema_name:'public',
                    table_name: 'tableone',
                    updated_at: new Date(12345678)
                },
            ]);
            assert.deepStrictEqual(tables.key(), ['t:8ny9He']);
        });

        it('should keep escaped tables escaped (db:"sch-ema".table)', function () {
            const tables = new QueryMetadata([
                {
                    dbname: 'db1',
                    schema_name: '"sch-ema"',
                    table_name: 'tableone',
                    updated_at: new Date(12345678)
                },
            ]);
            assert.deepStrictEqual(tables.key(), ['t:oVg75u']);
        });

        describe('with skipNotUpdatedAtTables enabled', function () {
            const skipNotUpdatedAtTables = true;
            const scenarios = [
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone'
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        }
                    ],
                    expectedLength: 0
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: null
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        }
                    ],
                    expectedLength: 0
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: undefined
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        }
                    ],
                    expectedLength: 0
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: Date.now()
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        }
                    ],
                    expectedLength: 1
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: Date.now()
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo',
                            updated_at: Date.now()
                        }
                    ],
                    expectedLength: 2
                },
                {
                    tables: [
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tableone',
                            updated_at: Date.now()
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tabletwo'
                        },
                        {
                            dbname: 'db1',
                            schema_name: 'public',
                            table_name: 'tablethree',
                            updated_at: Date.now()
                        }
                    ],
                    expectedLength: 2
                }
            ];
            scenarios.forEach(({ tables, expectedLength }) => {
                it('should get an array for multiple tables skipping the ones with no updated_at', function () {
                    const queryMetadata = new QueryMetadata(tables);
                    const keys = queryMetadata.key(skipNotUpdatedAtTables);
                    assert.strictEqual(keys.length, expectedLength);
                    keys.forEach(key => assert.strictEqual(key.length, KEY_LENGTH));
                });
            });
        });
    });

    describe('.getTables()', function () {
        const scenarios = [
            {
                result: [
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone'
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: null
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: undefined
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo',
                        updated_at: Date.now()
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tablethree',
                        updated_at: Date.now()
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone'
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: null
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: undefined
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo',
                        updated_at: Date.now()
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tablethree',
                        updated_at: Date.now()
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone'
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: null
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: undefined
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo',
                        updated_at: Date.now()
                    },
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
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tableone',
                        updated_at: Date.now()
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tabletwo'
                    },
                    {
                        dbname: 'db1',
                        schema_name: 'public',
                        table_name: 'tablethree',
                        updated_at: Date.now()
                    },
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

        scenarios.forEach(({ result, skipNotUpdatedAtTables, skipAnalysisCachedTables, expectedLength }) => {
            const filterUpdatedAt = skipNotUpdatedAtTables ? 'in' : 'out';
            const filterAnalysisTables = skipAnalysisCachedTables ? 'in' : 'out';
            const arrayLengthCond = `an array of ${expectedLength} items`;
            const updatedAtCond = `filtering ${filterUpdatedAt} updated_at`;
            const analysisTablesCond = `filtering ${filterAnalysisTables} analysis tables`;

            it(`should get ${arrayLengthCond} by ${updatedAtCond} and ${analysisTablesCond}`, function () {
                const queryMetadata = new QueryMetadata(result);
                const tables = queryMetadata.getTables(skipNotUpdatedAtTables, skipAnalysisCachedTables);
                assert.strictEqual(tables.length, expectedLength);
            });
        });
    });
});
