'use strict';
/** Upstream/content error with HTTP status + code — Vandal. */
class YTError extends Error {
  constructor(message, status = 502, code = 'UPSTREAM') {
    super(message);
    this.status = status;
    this.code = code;
  }
}

module.exports = { YTError };
