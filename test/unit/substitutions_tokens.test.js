'use strict';

const assert = require('assert');
const SubstitutionTokens = require('../../lib/utils/substitution_tokens');


describe('Substitution tokens: hasTokens', () => {

    const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
    tokens.forEach(token => {
        it('Works with Mapnik tokens: ' + token, () => {
            assert.ok(SubstitutionTokens.hasTokens('!' + token + '!'));
        });
    });

    it('Returns false when no tokens are found', () => {
        assert.equal(SubstitutionTokens.hasTokens('wadus wadus wadus'), false);
    });
});

describe('Substitution tokens: Replace', () => {

    const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
    tokens.forEach(token => {
        it('Replaces Mapnik token: ' + token, () => {
            const replaceValues = {};
            replaceValues[token] = 'wadus';
            assert.equal(SubstitutionTokens.replace('!' + token + '!', replaceValues), replaceValues[token]);
        });
    });

    it('Throws on unsupported tokens', () => {
        const replaceValues = { unsupported: 'wadus' };
        assert.throws(() => SubstitutionTokens.replace('!unsupported!', replaceValues), '!unsupported!');
    });

    it('The defaults are used when a value is not passed for a token', () => {
        const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table where the_geom && !bbox!';
        const values = {
            scale_denominator : '10'
        };
        const replaced = SubstitutionTokens.replace(sql, values);
        assert.ok(replaced.includes('10'));
        assert.ok(!replaced.includes('!bbox!'));
    });

});

describe('Substitution tokens: replaceXYZ', () => {

    const tokens = ['bbox', 'pixel_width', 'pixel_height', 'scale_denominator'];
    tokens.forEach(token => {
        it('Replaces Mapnik token: ' + token, () => {
            const replaced = SubstitutionTokens.replaceXYZ('!' + token + '!', { z: 1, x : 1, y : 0 });
            assert.ok(!SubstitutionTokens.hasTokens(replaced));
        });
    });

    it('Throws on unsupported invalid tile', () => {
        const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table where the_geom && !bbox!';
        assert.throws(() => SubstitutionTokens.replaceXYZ(sql, { z: 0.4, x : 4 }));
    });

    it('Works with just the zoom', () => {
        const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table';
        assert.ok(!SubstitutionTokens.hasTokens(SubstitutionTokens.replaceXYZ(sql, { z : 1 } )));
    });

    it('Works without arguments', () => {
        const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table where the_geom && !bbox!';
        assert.ok(!SubstitutionTokens.hasTokens(SubstitutionTokens.replaceXYZ(sql)));
    });

    it('Accepts bbox argument', () => {
        const sql = 'Select !scale_denominator! * ST_Area(geom) from my_table where the_geom && !bbox!';
        assert.ok(SubstitutionTokens.replaceXYZ(sql, { bbox : 'DUMMY' }).includes('DUMMY'));
    });
});
