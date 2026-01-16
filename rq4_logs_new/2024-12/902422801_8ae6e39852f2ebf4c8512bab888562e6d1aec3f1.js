// server/server.js
const express = require('express');
const dotenv = require('dotenv');
const db = require('./config/db');
const authRoutes = require('./api/auth');
const { authenticateToken, authorizeRole } = require('./middleware/roleMiddleware');
const menuRoutes = require('./api/menu');
const orderRoutes = require('./api/orders');
const cors = require('cors');
dotenv.config();

const app = express();
app.use(express.json());
app.use(express.static('public'));
app.use(cors());

// Routes
app.use('/api/auth', authRoutes);
app.use('/api/menu', menuRoutes);
app.use('/api/order', orderRoutes);
// Test route
app.get('/', (req, res) => {
    res.sendFile('index.html', { root: './public' });
});

// Start the server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});