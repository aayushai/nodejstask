// controllers/stockController.js
const csv = require('csv-parser');
const fs = require('fs');
const Stock = require('../models/stock');

// Upload CSV and validate data
exports.uploadCSV = async (req, res) => {
  if (!req.file || req.file.mimetype !== 'text/csv') {
    return res.status(400).json({ error: 'Please upload a CSV file.' });
  }

  const results = [];
  const requiredColumns = [
    'Date', 'Symbol', 'Series', 'Prev Close', 'Open', 'High', 'Low', 'Last', 'Close',
    'VWAP', 'Volume', 'Turnover', 'Trades', 'Deliverable Volume', '%Deliverable'
  ];
  
  let successful = 0;
  let failed = 0;
  const errors = [];

  fs.createReadStream(req.file.path)
    .pipe(csv())
    .on('headers', (headers) => {
      const missingColumns = requiredColumns.filter(col => !headers.includes(col));
      if (missingColumns.length > 0) {
        res.status(400).json({ error: `Missing required columns: ${missingColumns.join(', ')}` });
      }
    })
    .on('data', (row) => {
      const isValid = validateRow(row);
      if (isValid) {
        results.push(formatRow(row));
        successful++;
      } else {
        failed++;
        errors.push({ row, reason: 'Validation failed' });
      }
    })
    .on('end', async () => {
      try {
        await Stock.insertMany(results);
        fs.unlinkSync(req.file.path);
        res.json({
          total_records: results.length + failed,
          successful_records: successful,
          failed_records: failed,
          errors
        });
      } catch (err) {
        res.status(500).json({ error: "Database insertion error", details: err.message });
      }
    });
};

// Row validation
const validateRow = (row) => {
  return !isNaN(Date.parse(row['Date'])) && 
         !isNaN(parseFloat(row['Prev Close'])) && 
         !isNaN(parseFloat(row['Open'])) && 
         !isNaN(parseFloat(row['High'])) && 
         !isNaN(parseFloat(row['Low'])) && 
         !isNaN(parseFloat(row['Last'])) && 
         !isNaN(parseFloat(row['Close'])) && 
         !isNaN(parseFloat(row['VWAP'])) && 
         !isNaN(parseInt(row['Volume'])) && 
         !isNaN(parseFloat(row['Turnover'])) && 
         !isNaN(parseInt(row['Trades'])) && 
         (row['Deliverable Volume'] ? !isNaN(parseInt(row['Deliverable Volume'])) : true) && 
         (row['%Deliverable'] ? !isNaN(parseFloat(row['%Deliverable'])) : true);
};

const formatRow = (row) => ({
  date: new Date(row['Date']),
  symbol: row['Symbol'],
  series: row['Series'],
  prev_close: parseFloat(row['Prev Close']),
  open: parseFloat(row['Open']),
  high: parseFloat(row['High']),
  low: parseFloat(row['Low']),
  last: parseFloat(row['Last']),
  close: parseFloat(row['Close']),
  vwap: parseFloat(row['VWAP']),
  volume: parseInt(row['Volume']),
  turnover: parseFloat(row['Turnover']),
  trades: isNaN(parseInt(row['Trades'])) ? 0 : parseInt(row['Trades']),
  deliverable: row['Deliverable Volume'] ? parseInt(row['Deliverable Volume']) : null,
  percent_deliverable: row['%Deliverable'] ? parseFloat(row['%Deliverable']) : null
});

// APIs

// Get highest volume
exports.getHighestVolume = async (req, res) => { 
  const { start_date, end_date, symbol } = req.query;
  const filter = {};
  if (symbol) filter.symbol = symbol;
  if (start_date && end_date) filter.date = { $gte: new Date(start_date), $lte: new Date(end_date) };

  const result = await Stock.findOne(filter).sort('-volume');
  res.json({ highest_volume: result });
};

// Get average close price
exports.getAverageClose = async (req, res) => {
  const { start_date, end_date, symbol } = req.query;
  const match = { symbol, date: { $gte: new Date(start_date), $lte: new Date(end_date) } };

  const [result] = await Stock.aggregate([
    { $match: match },
    { $group: { _id: null, average_close: { $avg: '$close' } } }
  ]);
  res.json({ average_close: result ? result.average_close : null });
};

// Get average VWAP
exports.getAverageVWAP = async (req, res) => {
  const { start_date, end_date, symbol } = req.query;
  const match = { date: { $gte: new Date(start_date), $lte: new Date(end_date) } };
  if (symbol) match.symbol = symbol;

  const [result] = await Stock.aggregate([
    { $match: match },
    { $group: { _id: null, average_vwap: { $avg: '$vwap' } } }
  ]);
  res.json({ average_vwap: result ? result.average_vwap : null });
};



// exports.uploadCSV = async (req, res) => {
//     const results = [];
//     const failedRecords = [];
//     let successCount = 0;
//     let failCount = 0;

//     // Open and parse CSV file
//     fs.createReadStream(req.file.path)
//         .pipe(csvParser())
//         .on('data', (row) => {
//             try {
//                 // Validate each field here
//                 if (isValidRow(row)) {
//                     const parsedRow = {
//                         date: new Date(row['Date']),
//                         symbol: row['Symbol'],
//                         series: row['Series'],
//                         prev_close: parseFloat(row['Prev Close']),
//                         open: parseFloat(row['Open']),
//                         high: parseFloat(row['High']),
//                         low: parseFloat(row['Low']),
//                         last: parseFloat(row['Last']),
//                         close: parseFloat(row['Close']),
//                         vwap: parseFloat(row['VWAP']),
//                         volume: parseInt(row['Volume']),
//                         turnover: parseFloat(row['Turnover']),
//                         trades: parseInt(row['Trades']),
//                         deliverable: parseInt(row['Deliverable Volume']),
//                         percent_deliverable: parseFloat(row['%Deliverable']),
//                     };
//                     results.push(parsedRow);
//                     successCount++;
//                 } else {
//                     failedRecords.push({ row, reason: 'Validation failed' });
//                     failCount++;
//                 }
//             } catch (error) {
//                 console.error('Row parsing error:', error);
//                 failedRecords.push({ row, reason: error.message });
//                 failCount++;
//             }
//         })
//         .on('end', async () => {
//             try {
//                 // Insert validated records into MongoDB
//                 await Stock.insertMany(results);
//                 res.json({
//                     totalRecords: results.length + failedRecords.length,
//                     successfulRecords: successCount,
//                     failedRecords: failCount,
//                     details: failedRecords, // Optional: To see specific failures
//                 });
//             } catch (error) {
//                 console.error('Database insertion error:', error);
//                 res.status(500).json({
//                     error: 'Database insertion error',
//                     details: error.message,
//                 });
//             }
//         });
// };

// // Validate row helper function
// function isValidRow(row) {
//     return row['Date'] && !isNaN(Date.parse(row['Date'])) &&
//            !isNaN(row['Prev Close']) &&
//            !isNaN(row['Open']) &&
//            !isNaN(row['High']) &&
//            !isNaN(row['Low']) &&
//            !isNaN(row['Last']) &&
//            !isNaN(row['Close']) &&
//            !isNaN(row['VWAP']) &&
//            !isNaN(row['Volume']) &&
//            !isNaN(row['Turnover']) &&
//            !isNaN(row['Trades']) &&
//            !isNaN(row['Deliverable Volume']) &&
//            !isNaN(row['%Deliverable']);
// }
