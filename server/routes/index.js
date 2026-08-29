'use strict';
/** HTTP ルートの組み立て — Vandal. */
const express = require('express');

const router = express.Router();
router.use(express.json({ limit: '64kb' }));

router.use(require('./assets').router);
router.use(require('./health').router);   // /health /healthz /api/health
router.use(require('./media').router);
router.use(require('./api').router);
router.use(require('./ask').router);

module.exports = { router };
