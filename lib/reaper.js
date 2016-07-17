'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');

const async = require('async');
const backend = require('json-reaper-backend');
const binarySearch = require('binary-search');

const JSONDepthStream = require('json-depth-stream');

const utils = require('./reaper/utils');

function Reaper(db, options) {
  this.options = options || {};
  this.depth = this.options.depth === undefined ? 1 : this.options.depth;
  this.db = db;
}
module.exports = Reaper;

Reaper.utils = utils;

Reaper.prototype._finish = function _finish(c, tmp, body, index, callback) {
  async.waterfall([
    (callback) => {
      c.finish(JSON.stringify(index), (err, header) => {
        callback(err, tmp, body, header);
      });
    },
    (tmp, body, header, callback) => {
      const full = path.join(tmp, 'full');
      const to = fs.createWriteStream(full);
      const from = fs.createReadStream(body);

      to.write(header);
      from.pipe(to);

      const onError = (err) => {
        to.destroy();
        from.destroy();
        callback(err);
      };
      to.on('error', onError);
      from.on('error', onError);

      to.on('finish', () => {
        callback(null, full);
      });
    },
    (full, callback) => {
      fs.exists(this.db, exists => callback(null, full, exists));
    },
    (full, exists, callback) => {
      if (!exists)
        return callback(null, full, exists);

      fs.rename(this.db, this.db + '.copy', err => callback(err, full, exists));
    },
    (full, exists, callback) => {
      fs.rename(full, this.db, err => callback(err, exists));
    },
    (exists, callback) => {
      if (!exists)
        return callback(null);

      fs.unlink(this.db + '.copy', callback);
    }
  ], callback);
};

Reaper.prototype.replace = function replace(data, callback) {
  const indexed = utils.index(data, this.depth);

  async.waterfall([
    (callback) => {
      fs.mkdtemp(path.join(os.tmpdir(), 'json-reaper-'), callback);
    },
    (tmp, callback) => {
      const body = path.join(tmp, 'body');
      const stream = fs.createWriteStream(body);

      const c = new backend.Compressor();
      c.pipe(stream);

      async.map(indexed.chunks, (chunk, callback) => {
        c.append(chunk, (err, start, length) => {
          callback(err, { start: start, length: length });
        });
      }, (err, offsets) => {
        callback(err, tmp, body, c, offsets);
      });
    },
    (tmp, body, c, offsets, callback) => {
      const index = indexed.refs.map((ref) => {
        return {
          key: ref.key,
          start: offsets[ref.start].start,
          length: offsets[ref.end - 1].start + offsets[ref.end - 1].length -
                  offsets[ref.start].start
        };
      });

      this._finish(c, tmp, body, index, callback);
    }
  ], callback);
};

Reaper.prototype.replaceWithFD = function replaceWithFD(fd, callback) {
  async.waterfall([
    (callback) => {
      const stream = fs.createReadStream('/dev/null', {
        fd: fd,
        autoClose: false
      });
      const parser = new JSONDepthStream(this.depth);

      stream.pipe(parser);

      const visits = [];
      let total = 0;
      stream.on('data', chunk => total += chunk.length);

      parser.on('visit', (key, start, end) => {
        visits.push({ key: key, start: start, end: end });
      });

      parser.on('finish', () => {
        visits.push({ key: [], start: 0, end: total });
        callback(null, visits);
      });
    },
    (visits, callback) => {
      const index = utils.processVisits(visits);

      fs.mkdtemp(path.join(os.tmpdir(), 'json-reaper-'), (err, tmp) => {
        callback(err, index, tmp);
      });
    },
    (index, tmp, callback) => {
      const body = path.join(tmp, 'body');
      const stream = fs.createWriteStream(body);

      const c = new backend.Compressor();
      c.pipe(stream);

      async.mapSeries(index.chunks, (chunk, callback) => {
        const read = fs.createReadStream('/dev/null', {
          fd: fd,
          autoClose: false,
          start: chunk.start,
          end: chunk.end - 1
        });

        c.append(read, (err, start, length) => {
          callback(err, { start: start, length: length });
        });
      }, (err, offsets) => {
        callback(err, tmp, body, c, index, offsets);
      });
    },
    (tmp, body, c, indexed, offsets, callback) => {
      const index = indexed.refs.map((ref) => {
        return {
          key: ref.key,
          start: offsets[ref.start].start,
          length: offsets[ref.end - 1].start + offsets[ref.end - 1].length -
                  offsets[ref.start].start
        };
      });

      this._finish(c, tmp, body, index, callback);
    }
  ], callback);
};

Reaper.prototype.query = function query(key, callback) {
  // TODO(indutny): cache these
  const file = new backend.File(this.db);
  const d = new backend.Decompressor(file);

  async.waterfall([
    callback => file.open(callback),
    (callback) => {
      d.getIndex(callback);
    },
    (index, callback) => {
      try {
        index = JSON.parse(index.toString());
      } catch (e) {
        return callback(e);
      }

      const needle = { key: key };
      let i = binarySearch(index, needle, utils.refCompare);

      // No exact matches
      if (i < 0) {
        i = -1 - i;
        // Find first parent key
        for (i = Math.max(0, i - 1); i >= 0; i--)
          if (utils.refCompare(index[i], needle) <= 0)
            break;
      }

      const closest = index[i];

      // Check that subkey can be found at all
      for (let i = 0; i < closest.key.length; i++)
        if (closest.key[i] !== key[i])
          return callback(new Error('Not found'));

      // Additional keys to dive in
      const sub = key.slice(closest.key.length);
      const stream = d.fetch(closest.start, closest.length);

      utils.parseStream(stream, (err, json) => callback(err, sub, json));
    },
    (sub, json, callback) => {
      for (let i = 0; i < sub.length; i++) {
        if (json === null || typeof json !== 'object')
          return callback(new Error('Not found'));

        json = json[sub[i]];
      }

      callback(null, json);
    }
  ], (err, result) => {
    file.close();
    callback(err, result);
  });
};
