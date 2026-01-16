const express = require('express');
const router = express.Router();
const mysql = require('mysql');

// Configuración de la base de datos
const db = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
});

// Ruta para obtener los datos del PT100
router.get('/pt100', (req, res) => {
  const query = "SELECT DATE_FORMAT(fecha, '%Y-%m-%d %H:%i:%s') as Fecha, Serie as Temperatura FROM datos ORDER BY fecha DESC LIMIT 10";

  db.query(query, (err, results) => {
    if (err) {
      console.error('Error en la consulta:', err);
      return res.status(500).json({ error: 'Error en la consulta' });
    }
    res.json(results);
  });
});

module.exports = router;