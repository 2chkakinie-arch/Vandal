'use strict';
/** 分散ソースのバンドル配信（/app.js, /styles.css）— Vandal. */
const express = require('express');
const bundle = require('../client-bundle');

const router = express.Router();
router.get('/', bundle.index);
// Go エッジは起動時に /index.html を直接 pull するため、こちらもテンプレート配信にする
router.get('/index.html', bundle.index);
router.get('/app.js', bundle.app);
router.get('/styles.css', bundle.styles);

module.exports = { router };
