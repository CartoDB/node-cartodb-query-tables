'use strict';

const assert = require('assert');
const SubstitutionTokens = require('../../lib/utils/substitution_tokens');

describe('SubstitutionTokens', function () {
    describe('.hasTokens()', function () {
        const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
        tokens.forEach(token => {
            it(`Works with Mapnik tokens: ${token}`, function () {
                assert.ok(SubstitutionTokens.hasTokens(`!${token}!`));
            });
        });

        it('Returns false when no tokens are found', function () {
            assert.strictEqual(SubstitutionTokens.hasTokens('wadus wadus wadus'), false);
        });
    });

    describe('.replace()', function () {
        const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
        tokens.forEach(token => {
            it(`Replaces Mapnik token: ${token}`, function () {
                const replaceValues = {};
                replaceValues[token] = 'wadus';
                assert.strictEqual(SubstitutionTokens.replace(`!${token}!`, replaceValues), replaceValues[token]);
            });
        });

        it('Throws on unsupported tokens', function () {
            const replaceValues = { unsupported: 'wadus' };
            assert.throws(() => SubstitutionTokens.replace('!unsupported!', replaceValues), '!unsupported!');
        });

        it('The defaults are used when a value is not passed for a token', function () {
            const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table where the_geom && !bbox!';
            const values = {
                scale_denominator : '10'
            };
            const replaced = SubstitutionTokens.replace(sql, values);
            assert.ok(replaced.includes('10'));
            assert.ok(!replaced.includes('!bbox!'));
        });
    });

    describe('.replaceXYZ()', function () {
        const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];

        tokens.forEach(token => {
            it(`Replaces Mapnik token: ${token}`, function () {
                const replaced = SubstitutionTokens.replaceXYZ(`!${token}!`, { z: 1, x : 1, y : 0 });
                assert.ok(!SubstitutionTokens.hasTokens(replaced));
            });
        });

        it('Throws on unsupported invalid tile', function () {
            const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table where the_geom && !bbox!';
            assert.throws(() => SubstitutionTokens.replaceXYZ(sql, { z: 0.4, x : 4 }));
        });

        it('Works with just the zoom', function () {
            const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table';
            assert.ok(!SubstitutionTokens.hasTokens(SubstitutionTokens.replaceXYZ(sql, { z : 1 } )));
        });

        it('Works without arguments', function () {
            const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table where the_geom && !bbox!';
            assert.ok(!SubstitutionTokens.hasTokens(SubstitutionTokens.replaceXYZ(sql)));
        });

        it('Accepts bbox argument', function () {
            const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table where the_geom && !bbox!';
            assert.ok(SubstitutionTokens.replaceXYZ(sql, { bbox : 'DUMMY' }).includes('DUMMY'));
        });
    });
});
