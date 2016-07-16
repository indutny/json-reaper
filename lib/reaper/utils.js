'use strict';

function recursiveIndex(data, depth, key, out, refs) {
  if (depth === 0 || data === null || typeof data !== 'object') {
    refs.push({ key: key, start: out.length, end: out.length + 1 });
    out.push(JSON.stringify(data));
    return;
  }

  const ref = { key: key, start: out.length, end: 0 };
  refs.push(ref);

  if (Array.isArray(data)) {
    out.push('[');
    for (let i = 0; i < data.length; i++) {
      if (i !== 0)
        out.push(',');

      const nextKey = key.concat(i);
      recursiveIndex(data[i], depth - 1, nextKey, out, refs);
    }
    out.push(']');
  } else {
    out.push('{');
    const keys = Object.keys(data);
    for (let i = 0; i < keys.length; i++) {
      const subkey = keys[i];

      const strkey = JSON.stringify(subkey) + ':';
      if (i !== 0)
        out.push(',' + strkey);
      else
        out.push(strkey);

      const nextKey = key.concat(subkey);
      recursiveIndex(data[subkey], depth - 1, nextKey, out, refs);
    }
    out.push('}');
  }

  ref.end = out.length;
}

function refCompare(a, b) {
  const min = Math.min(a.key.length, b.key.length);
  for (let i = 0; i < min; i++) {
    const aKey = a.key[i];
    const bKey = b.key[i];
    if (aKey > bKey)
      return 1;
    else if (aKey < bKey)
      return -1;
  }

  if (a.key.length > b.key.length)
    return 1;
  else if (b.key.length > a.key.length)
    return -1;
  else
    return 0;
}
exports.refCompare = refCompare;

function index(data, depth) {
  depth = Math.max(depth, 0);

  const chunks = [];
  const refs = [];
  recursiveIndex(data, depth, [], chunks, refs);
  refs.sort(refCompare);
  return { chunks: chunks, refs: refs };
}
exports.index = index;

function parseStream(stream, callback) {
  let chunks = '';
  stream.on('data', chunk => chunks += chunk);
  stream.on('error', err => callback(err));
  stream.on('end', () => {
    let data;
    try {
      data = JSON.parse(chunks);
    } catch (e) {
      return callback(e);
    }
    callback(null, data);
  });
}
exports.parseStream = parseStream;
