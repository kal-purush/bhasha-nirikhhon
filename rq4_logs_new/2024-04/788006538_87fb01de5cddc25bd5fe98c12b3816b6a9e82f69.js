import express from "express";
import sqlite3 from "sqlite3";

const app = express();
const PORT = process.env.PORT || 3000;

// Conexión a la base de datos SQLite
const db = new sqlite3.Database('fintechs.db');

// Ruta para obtener todas las fintechs
app.get('/fintechs', (req, res) => {
    db.all('SELECT * FROM fintechs', (err, rows) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        res.json(rows);
    });
});

// Iniciar el servidor
app.listen(PORT, () => {
    console.log(`Servidor iniciado en http://localhost:${PORT}/fintechs/`);
});