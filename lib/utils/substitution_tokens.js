'use strict';

const WebMercatorHelper = require('../utils/webmercator_helper');
const webMercator = new WebMercatorHelper();

const SUBSTITUTION_TOKENS = {
    // Declared and used by Mapnik
    bbox: /!bbox!/g,
    scale_denominator: /!scale_denominator!/g,
    pixel_width: /!pixel_width!/g,
    pixel_height: /!pixel_height!/g,

    // Backward compatibility with camshaft
    var_zoom: /@zoom/g,
    var_bbox: /@bbox/g,
    var_x: /@x/g,
    var_y: /@y/g,

    // Used by pg-mvt to distinguish between the tile bbox and the one with the extra buffer
    tile_bbox: /!tile_bbox!/g
};


const DEFAULT_VALUES = {
    // For Mapnik tokens we use the values for tile 0/0/0 as it will contain all geometries
    bbox: 'ST_MakeEnvelope(-20037508.342789245, -20037508.342789245, 20037508.342789245, 20037508.342789245, 3857)',
    scale_denominator: '559082264.02871777343',
    pixel_width: '156543.03392804097656',
    pixel_height: '156543.03392804097656',

    // These tokens are not meant to be used directly, so we leave them as they are unless a value is passed
    var_zoom: '@zoom',
    var_bbox: '@bbox',
    var_x: '@x',
    var_y: '@y',
    tile_bbox: '!tile_bbox!'
};

class SubstitutionTokens {

    static tokens(sql) {
        return Object.keys(SUBSTITUTION_TOKENS).filter(tokenName => !!sql.match(SUBSTITUTION_TOKENS[tokenName]));
    }

    static hasTokens(sql) {
        return this.tokens(sql).length > 0;
    }

    /**
     * Replaces tokens in a query with the passing values. If a kwown token is found without
     * the corresponding replaceValue, the default value will be set.
     * @param {String} sql - SQL query
     * @param {Object} replaceValues - Values to be set to replace the tokens
     * @returns the modified string
     */
    static replace(sql, replaceValues) {
        if (!sql) {
            return undefined;
        }
        const allValues = Object.assign({}, DEFAULT_VALUES, replaceValues);
        Object.keys(allValues).forEach(token => {
            if (SUBSTITUTION_TOKENS[token]) {
                sql = sql.replace(SUBSTITUTION_TOKENS[token], allValues[token]);
            }
            else {
                throw Error(`Invalid token passed: '${token}'. Expected: [${Object.keys(SUBSTITUTION_TOKENS)}]`);
            }
        });
        return sql;
    }

    /**
     * Replaces Mapnik tokens with the values to match the tile defined in the parameters
     * @param {String} sql - SQL query
     * @param {integer} z, {integer} x, {integer} y - Defines the tile used
     * @param {String} bbox - Bbox for the tile (overwrites x and y)
     * @returns the modified string
     * This will throw if an invalid tile is passed
     */
    static replaceXYZ(sql, { z = 0, x = 0, y = 0, bbox } = {}) {
        const resolution = webMercator.getResolution({ z : z });

        if (!bbox) {
            const extent = webMercator.getExtent({ x : x, y : y, z : z });
            bbox = `ST_MakeEnvelope(${extent.xmin}, ${extent.ymin}, ${extent.xmax}, ${extent.ymax}, 3857)`;
        }

        // See https://github.com/mapnik/mapnik/wiki/ScaleAndPpi#scale-denominator
        const scale_denominator = `${resolution.dividedBy(0.00028)}`;

        const replaceValues = {
            bbox: bbox,
            scale_denominator: scale_denominator,
            pixel_width: `${resolution}`,
            pixel_height: `${resolution}`
        };

        return this.replace(sql, replaceValues);
    }
}

module.exports = SubstitutionTokens;
