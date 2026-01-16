const express = require('express');
require('dotenv').config();
const { PORT } = require('./config/server');
const { connectDB } = require('./config/database');
const animalRoutes = require('./routes/animal.routes');

const app = express();
app.use(express.json());

app.get('/api/health', (req, res) => {
    res.send('Hello World');
});

app.use('/api/animals', animalRoutes);

// connect to database
connectDB();

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});