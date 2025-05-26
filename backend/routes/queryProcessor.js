// === routes/queryProcessor.js ===
const express = require('express');
const router = express.Router();
const { processQuery } = require('../controllers/queryController');

router.get('/', processQuery);

module.exports = router;

