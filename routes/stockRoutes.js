// routes/stockRoutes.js
const express = require('express');
const multer = require('multer');
const stockController = require('../controllers/stockController');
const router = express.Router();
const upload = multer({ dest: 'uploads/' });

router.post('/upload', upload.single('file'), stockController.uploadCSV);
router.get('/highest_volume', stockController.getHighestVolume);
router.get('/average_close', stockController.getAverageClose);
router.get('/average_vwap', stockController.getAverageVWAP);

module.exports = router;
