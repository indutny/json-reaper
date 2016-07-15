'use strict';

const backend = require('json-reaper-backend');

const utils = require('./reaper/utils');

function Reaper(db, options) {
  this.options = options || {};
  this.depth = this.options.depth === undefined ? 1 : this.options.depth;
  this.db = db;
}
module.exports = Reaper;

Reaper.utils = utils;

Reaper.prototype.replace = function replace(data, callback) {
  const indexed = utils.index(data, this.depth);
};
