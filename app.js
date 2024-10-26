// app.js
const express = require('express');
const dotenv = require('dotenv');
const connectDB = require('./config/db');
const stockRoutes = require('./routes/stockRoutes');

dotenv.config();
connectDB();

const app = express();
app.use(express.json());
app.use('/api', stockRoutes);

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));