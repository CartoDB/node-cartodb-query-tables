'use strict';

// TODO: Check if this can be removed
const Decimal = require('decimal.js');

class WebMercatorHelper {
    constructor(tileSize, maxGeosize) {
        this.tileSize = tileSize || 256;
        this.tileMaxGeosize = new Decimal(maxGeosize || 6378137.0 * Math.PI * 2);
    }

    getResolution({ z }) {
        if (!Number.isInteger(z) || z < 0) {
            throw new Error('Input must be a positive integer');
        }
        const fullResolution = this.tileMaxGeosize.dividedBy(this.tileSize);
        return fullResolution.dividedBy(Decimal.pow(2, z));
    }

    getExtent({ x, y, z }) {
        /* jshint maxcomplexity:10 */

        if (!Number.isInteger(x) || x < 0 ||
            !Number.isInteger(y) || y < 0 ||
            !Number.isInteger(z) || z < 0) {
            throw new Error('Inputs must be positive integers');
        }

        const maxCoordinate = Decimal.pow(2, z);
        if (x >= maxCoordinate || y >= maxCoordinate) {
            throw new Error('Invalid tile XYZ (' + x +',' + y + z + ')');
        }

        const originShift  = this.tileMaxGeosize.dividedBy(2);
        const tileGeoSize = this.tileMaxGeosize.dividedBy(maxCoordinate);

        const xmin = tileGeoSize.times(x).minus(originShift);
        const xmax = xmin.plus(tileGeoSize);

        const ymax = originShift.minus(tileGeoSize.times(y));
        const ymin = ymax.minus(tileGeoSize);

        return {
            xmin: xmin,
            ymin: ymin,
            xmax: xmax,
            ymax: ymax
        };
    }
}

module.exports = WebMercatorHelper;