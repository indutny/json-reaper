'use strict';

const assert = require('assert');
const async = require('async');
const path = require('path');
const fs = require('fs');

const Reaper = require('../');

const db = path.join(__dirname, 'tmp', 'db.json.gz');

describe('JSON Reaper', () => {
  beforeEach(() => {
    try {
      fs.unlinkSync(db);
    } catch (e) {
    }
  });

  afterEach(() => {
    try {
      fs.unlinkSync(db);
    } catch (e) {
    }
  });

  it('should index/query JSON', (cb) => {
    const r = new Reaper(db, {
      depth: 1
    });

    async.waterfall([
      (callback) => {
        r.replace({
          a: 1,
          b: 2,
          c: [ { f: 4 }, { f: 5 } ],
          d: 4
        }, callback);
      },
      (callback) => {
        r.query([ 'c', 1 ], callback);
      },
      (value, callback) => {
        assert.deepEqual(value, { f: 5 });
        callback(null);
      },

      // Not found
      (callback) => {
        r.query([ 'e', 1 ], (err) => {
          assert(/not found/i.test(err.message));
          callback(null);
        });
      }
    ], cb);
  });
});
