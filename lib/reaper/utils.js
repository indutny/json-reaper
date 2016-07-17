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

function posCompare(a, b) {
  return a.offset - b.offset;
}

function processVisits(visits, total) {
  const chunks = [];
  const refs = [];

  const positions = [];
  for (let i = 0; i < visits.length; i++) {
    const v = visits[i];
    const ref = { key: v.key, start: 0, end: 0 };

    positions.push({ offset: v.start, ref: ref, type: 'start' });
    positions.push({ offset: v.end, ref: ref, type: 'end' });

    refs.push(ref);
  }

  positions.sort(posCompare);
  for (let i = 0; i < positions.length - 1; i++) {
    const current = positions[i];
    const next = positions[i + 1];

    if (current.type === 'end')
      current.ref.end = chunks.length;
    else
      current.ref.start = chunks.length;

    chunks.push({ start: current.offset, end: next.offset });

    if (next.type === 'end')
      next.ref.end = chunks.length;
    else
      next.ref.start = chunks.length;
  }

  refs.sort(refCompare);

  return {
    refs: refs,
    chunks: chunks
  };
}
exports.processVisits = processVisits;
