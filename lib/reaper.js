'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const PassThrough = require('stream').PassThrough;

const async = require('async');
const backend = require('json-reaper-backend');
const binarySearch = require('binary-search');
const debug = require('debug')('json-reaper');

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
      debug('copying %s => %s', body, full);

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

      debug('renaming %s => %s', this.db, this.db + '.copy');
      fs.rename(this.db, this.db + '.copy', err => callback(err, full, exists));
    },
    (full, exists, callback) => {
      debug('renaming %s => %s', full, this.db);
      fs.rename(full, this.db, err => callback(err, exists));
    },
    (exists, callback) => {
      if (!exists)
        return callback(null);

      debug('removing %s', this.db + '.copy');
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
      debug('temp dir %s', tmp);
      const body = path.join(tmp, 'body');
      const stream = fs.createWriteStream(body);

      const c = new backend.Compressor();
      c.pipe(stream);

      debug('writing');
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

      debug('finalizing');
      this._finish(c, tmp, body, index, callback);
    }
  ], callback);
};

Reaper.prototype._chunkify = function _chunkify(read, c, callback) {
  const chunks = [];
  const refs = [];

  let waiting = 0;
  let once = false;
  const done = (err) => {
    if (!err && --waiting !== 0)
      return;

    if (once)
      return;
    once = true;

    // Whole JSON
    refs.push({ key: [], start: 0, end: chunks.length });

    callback(null, refs.map((ref) => {
      return {
        key: ref.key,
        start: chunks[ref.start].start,
        length: chunks[ref.end - 1].start + chunks[ref.end - 1].length -
                chunks[ref.start].start
      };
    }).sort(utils.refCompare));
  };

  const parser = new JSONDepthStream(this.depth);

  let current = null;
  let last = 0;

  waiting++;
  read.on('data', (chunk) => {
    // Initial chunk
    if (chunks.length === 0)
      createChunk();

    last = 0;
    current = chunk;
    parser.update(chunk);

    if (last !== chunk.length)
      writeChunk(chunk.length, false);
    current = null;
  });
  read.once('end', () => {
    chunks[chunks.length - 1].stream.end();
    done(null)
  });

  const keyStack = [];
  const chunkStack = [];

  let paused = 0;

  function createChunk() {
    const stream = new PassThrough();
    const chunk = { start: 0, length: 0, stream: stream };
    chunks.push(chunk);

    waiting++;
    c.append(stream, (err, start, length) => {
      if (--paused === 0)
        read.resume();

      if (err)
        return done(err);

      chunk.start = start;
      chunk.length = length;
      chunk.stream = null;
      done(null);
    });
  }

  function writeChunk(index, end) {
    const stream = chunks[chunks.length - 1].stream;

    stream.write(current.slice(last, index));
    if (end) {
      if (++paused === 1)
        read.pause();
      stream.end();
    }
    last = index;
  }

  parser.on('split', (key, index) => {
    writeChunk(index, true);

    if (key.length > keyStack.length) {
      // Enter
      keyStack.push(key[key.length - 1]);
      chunkStack.push(chunks.length);
    } else {
      const start = chunkStack.pop() || 0;
      const end = chunks.length;

      refs.push({ key: keyStack.slice(), start: start, end: end });

      // Leave
      keyStack.pop();
    }

    createChunk(index);
  });
};

Reaper.prototype.replaceWithStream = function replaceWithStream(input,
                                                                callback) {
  async.waterfall([
    (callback) => {
      fs.mkdtemp(path.join(os.tmpdir(), 'json-reaper-'), callback);
    },
    (tmp, callback) => {
      debug('temp dir %s', tmp);

      // Writing
      const body = path.join(tmp, 'body');
      const write = fs.createWriteStream(body);

      const c = new backend.Compressor();
      c.pipe(write);
      debug('writing data to %s', body);

      // Reading
      debug('parse start');
      this._chunkify(input, c, (err, index) => {
        callback(err, tmp, c, body, index);
      });
    },
    (tmp, c, body, index, callback) => {
      debug('finalizing %s', body);
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
