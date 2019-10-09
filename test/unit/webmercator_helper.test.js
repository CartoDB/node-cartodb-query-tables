'use strict';

const assert = require('assert');
const WebMercatorHelper = require('../../lib/utils/webmercator_helper');

describe('WebMercatorHelper', function () {
    describe('.getResolution()', function () {
        it('works with integer', function () {
            const wmh = new WebMercatorHelper();

            assert.strictEqual(wmh.getResolution({ z : 0 }).toString(), '156543.03392804097656');
            assert.strictEqual(wmh.getResolution({ z : 1 }).toString(), '78271.51696402048828');
            assert.strictEqual(wmh.getResolution({ z : 4 }).toString(), '9783.939620502561035');
            assert.strictEqual(wmh.getResolution({ z : 6 }).toString(), '2445.9849051256402588');
            assert.strictEqual(wmh.getResolution({ z : 18 }).toString(), '0.5971642834779395163');
            assert.strictEqual(wmh.getResolution({ z : 24 }).toString(), '0.0093306919293428049421');
            assert.strictEqual(wmh.getResolution({ z : 30 }).toString(), '0.00014579206139598132722');
            assert.strictEqual(wmh.getResolution({ z : 32 }).toString(), '0.000036448015348995331805');
        });

        it('throws on invalid values', function () {
            const wmh = new WebMercatorHelper();

            assert.throws(() => wmh.getResolution(1));
            assert.throws(() => wmh.getResolution({ z : 1.0001 }));
            assert.throws(() => wmh.getResolution({ z : -3 }));
            assert.throws(() => wmh.getResolution({ z : -2.99 }));
            assert.throws(() => wmh.getResolution({ z : 'dasd' }));
        });
    });

    describe('.getExtent()', function () {
        it('works with integer', function () {
            const wmh = new WebMercatorHelper();

            let extent = wmh.getExtent({ x : 0, y : 0, z : 0 });
            assert.strictEqual(extent.xmin.toString(), '-20037508.342789245');
            assert.strictEqual(extent.ymin.toString(), '-20037508.342789245');
            assert.strictEqual(extent.xmax.toString(), '20037508.342789245');
            assert.strictEqual(extent.ymax.toString(), '20037508.342789245');

            extent = wmh.getExtent({ x : 0, y : 0, z : 18 });
            assert.strictEqual(extent.xmin.toString(), '-20037508.342789245');
            assert.strictEqual(extent.ymin.toString(), '20037355.468732674647');
            assert.strictEqual(extent.xmax.toString(), '-20037355.468732674647');
            assert.strictEqual(extent.ymax.toString(), '20037508.342789245');

            extent = wmh.getExtent({ x : 0, y : 0, z : 20 });
            assert.strictEqual(extent.xmin.toString(), '-20037508.342789245');
            assert.strictEqual(extent.ymin.toString(), '20037470.124275102412');
            assert.strictEqual(extent.xmax.toString(), '-20037470.124275102412');
            assert.strictEqual(extent.ymax.toString(), '20037508.342789245');


            extent = wmh.getExtent({ x : 1208, y : 1539, z : 12 });
            assert.strictEqual(extent.xmin.toString(), '-8218509.281222151269');
            assert.strictEqual(extent.ymin.toString(), '4970241.327215301006');
            assert.strictEqual(extent.xmax.toString(), '-8208725.341601648708');
            assert.strictEqual(extent.ymax.toString(), '4980025.266835803567');

            extent = wmh.getExtent({ x : 603, y : 670, z : 11 });
            assert.strictEqual(extent.xmin.toString(), '-8238077.160463156392');
            assert.strictEqual(extent.ymin.toString(), '6907461.3720748080909');
            assert.strictEqual(extent.xmax.toString(), '-8218509.2812221512699');
            assert.strictEqual(extent.ymax.toString(), '6927029.251315813213');
        });

        it('boundaries around 0,0 (zoom 1)', function () {
            const wmh = new WebMercatorHelper();

            let extent = wmh.getExtent({ x : 0, y : 0, z : 1 });
            assert.strictEqual(extent.xmin.toString(), '-20037508.342789245');
            assert.strictEqual(extent.ymin.toString(), '0');
            assert.strictEqual(extent.xmax.toString(), '0');
            assert.strictEqual(extent.ymax.toString(), '20037508.342789245');

            extent = wmh.getExtent({ x : 0, y : 1, z : 1 });
            assert.strictEqual(extent.xmin.toString(), '-20037508.342789245');
            assert.strictEqual(extent.ymin.toString(), '-20037508.342789245');
            assert.strictEqual(extent.xmax.toString(), '0');
            assert.strictEqual(extent.ymax.toString(), '0');

            extent = wmh.getExtent({ x : 1, y : 0, z : 1 });
            assert.strictEqual(extent.xmin.toString(), '0');
            assert.strictEqual(extent.ymin.toString(), '0');
            assert.strictEqual(extent.xmax.toString(), '20037508.342789245');
            assert.strictEqual(extent.ymax.toString(), '20037508.342789245');

            extent = wmh.getExtent({ x : 1, y : 1, z : 1 });
            assert.strictEqual(extent.xmin.toString(), '0');
            assert.strictEqual(extent.ymin.toString(), '-20037508.342789245');
            assert.strictEqual(extent.xmax.toString(), '20037508.342789245');
            assert.strictEqual(extent.ymax.toString(), '0');
        });

        it('boundaries around 0,0 (zoom 3)', function () {
            const wmh = new WebMercatorHelper();

            let extent = wmh.getExtent({ x : 3, y : 3, z : 3 });
            assert.strictEqual(extent.xmin.toString(), '-5009377.08569731125');
            assert.strictEqual(extent.ymin.toString(), '0');
            assert.strictEqual(extent.xmax.toString(), '0');
            assert.strictEqual(extent.ymax.toString(), '5009377.08569731125');

            extent = wmh.getExtent({ x : 3, y : 4, z : 3 });
            assert.strictEqual(extent.xmin.toString(), '-5009377.08569731125');
            assert.strictEqual(extent.ymin.toString(), '-5009377.08569731125');
            assert.strictEqual(extent.xmax.toString(), '0');
            assert.strictEqual(extent.ymax.toString(), '0');

            extent = wmh.getExtent({ x : 4, y : 3, z : 3 });
            assert.strictEqual(extent.xmin.toString(), '0');
            assert.strictEqual(extent.ymin.toString(), '0');
            assert.strictEqual(extent.xmax.toString(), '5009377.08569731125');
            assert.strictEqual(extent.ymax.toString(), '5009377.08569731125');

            extent = wmh.getExtent({ x : 4, y : 4, z : 3 });
            assert.strictEqual(extent.xmin.toString(), '0');
            assert.strictEqual(extent.ymin.toString(), '-5009377.08569731125');
            assert.strictEqual(extent.xmax.toString(), '5009377.08569731125');
            assert.strictEqual(extent.ymax.toString(), '0');
        });

        it('throws with invalid tiles', function () {
            const wmh = new WebMercatorHelper();

            assert.throws(() => wmh.getExtent({ x : 0, y : 2, z : 0 }));
            assert.throws(() => wmh.getExtent({ x : 2, y : 0, z : 0 }));
        });

        it('throws on invalid values', function () {
            const wmh = new WebMercatorHelper();

            assert.throws(() => wmh.getExtent());
            assert.throws(() => wmh.getExtent(0, 0, 0));
            assert.throws(() => wmh.getExtent({ x : -1, y : 0, z : 0 }));
            assert.throws(() => wmh.getExtent(-2.99));
            assert.throws(() => wmh.getExtent('dasd'));
        });
    });
});
