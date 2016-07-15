'use strict';

const assert = require('assert');
const async = require('async');
const path = require('path');
const fs = require('fs');

const Reaper = require('../');

const db = path.join(__dirname, 'tmp', 'db.json.gz');

describe('JSON Reaper', () => {
  return;
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
          c: 3,
          d: 4
        }, callback);
      }
    ], cb);
  });
});
