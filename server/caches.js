'use strict';
/** Shared InnerTube caches — Vandal. */
const { TTLCache } = require('./cache');

const CACHE_MIN = 60 * 1000;
const caches = {
  api: new TTLCache({ max: 800, ttl: 10 * CACHE_MIN }),
  visitor: new TTLCache({ max: 4, ttl: 25 * CACHE_MIN }),
  streams: new TTLCache({ max: 600, ttl: 5 * 60 * CACHE_MIN }), // googlevideo URLs expire ~6h
};

module.exports = { caches, CACHE_MIN };
