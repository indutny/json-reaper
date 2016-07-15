'use strict';

const assert = require('assert');

const Reaper = require('../');
const utils = Reaper.utils;

describe('JSON Reaper / Utils', () => {
  describe('index', () => {
    it('should work at depth=0', () => {
      const root = { a: 1, b: { c: 2 } };
      assert.deepEqual(utils.index(root, 0), {
        chunks: [ JSON.stringify(root) ],
        refs: [ { key: [], start: 0, end: 1 } ]
      });
    });

    it('should work at depth=1', () => {
      const root = { a: 1, b: { c: 2 } };
      assert.deepEqual(utils.index(root, 1), {
        chunks: [ '{', '"a":', '1', ',"b":', '{"c":2}', '}' ],
        refs: [
          { key: [], start: 0, end: 6 },
          { key: [ 'a' ], start: 2, end: 3 },
          { key: [ 'b' ], start: 4, end: 5 }
        ]
      });
    });

    it('should work at depth=2', () => {
      const root = { a: 1, b: { c: 2 } };
      assert.deepEqual(utils.index(root, 2), {
        chunks: [ '{', '"a":', '1', ',"b":', '{', '"c":', '2', '}', '}' ],
        refs: [
          { key: [], start: 0, end: 9 },
          { key: [ 'a' ], start: 2, end: 3 },
          { key: [ 'b' ], start: 4, end: 8 },
          { key: [ 'b', 'c' ], start: 6, end: 7 }
        ]
      });
    });

    it('should work at depth=3', () => {
      const root = { a: 1, b: { c: 2 } };
      assert.deepEqual(utils.index(root, 2), {
        chunks: [ '{', '"a":', '1', ',"b":', '{', '"c":', '2', '}', '}' ],
        refs: [
          { key: [], start: 0, end: 9 },
          { key: [ 'a' ], start: 2, end: 3 },
          { key: [ 'b' ], start: 4, end: 8 },
          { key: [ 'b', 'c' ], start: 6, end: 7 }
        ]
      });
    });

    it('should work with arrays', () => {
      const root = [ 1, 2, 3 ];
      assert.deepEqual(utils.index(root, 1), {
        chunks: [ '[', '1', ',', '2', ',', '3', ']' ],
        refs: [
          { key: [], start: 0, end: 7 },
          { key: [ 0 ], start: 1, end: 2 },
          { key: [ 1 ], start: 3, end: 4 },
          { key: [ 2 ], start: 5, end: 6 }
        ]
      });
    });
  });
});
