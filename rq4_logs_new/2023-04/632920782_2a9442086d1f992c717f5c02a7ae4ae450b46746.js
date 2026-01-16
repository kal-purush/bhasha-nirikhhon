const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  connectionString: 'postgresql://postgres:PolarisTHREE2023!@localhost:5432/WHERE'
});

// API endpoint to insert detected objects
app.post('/api/detected-objects', async (req, res) => {
  const { object_name, timestamp, location } = req.body;

  try {
    const result = await pool.query(
      'INSERT INTO detected_objects (object_name, timestamp, location) VALUES ($1, $2, $3) RETURNING *',
      [object_name, timestamp, location]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Something went wrong. Please try again later.' });
  }
});

// API endpoint to retrieve detected objects grouped by the specified time intervals
app.get('/api/detected-objects/:interval', async (req, res) => {
  const { interval } = req.params;

  try {
    const result = await pool.query(
      `SELECT object_name, timestamp, location
      FROM detected_objects
      WHERE timestamp > NOW() - INTERVAL '1 ${interval}'
      ORDER BY timestamp DESC`
    );

    res.status(200).json(result.rows);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Something went wrong. Please try again later.' });
  }
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});