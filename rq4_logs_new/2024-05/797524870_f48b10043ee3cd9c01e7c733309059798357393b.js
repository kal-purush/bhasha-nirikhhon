const express = require("express");
const mysql = require('mysql2');
const cors = require('cors');

const dotenv = require('dotenv');
dotenv.config();

const app = express();

app.use(express.json());
app.use(cors({
    origin:"https://mahuliwebsite.netlify.app",
}));


const connection = mysql.createConnection({

    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD,
    database:process.env.DATABASE
});



connection.connect((err) => {
    if (err) {
        console.error('Error connecting to MySQL: ' + err.stack);
        return;
    }
    console.log('Connected to MySQL database as id ' + connection.threadId);
});


// MySQL Schema
const createUserTable = `CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255),
    subject VARCHAR(255),
    message TEXT,
    contact VARCHAR(255)
)`;

connection.query(createUserTable, (err, results) => {
    if (err) throw err;
    console.log("Users table created successfully");
});

// Endpoint to save user data
app.post('/save', (req, res) => {
    const { username, subject,  message, contact, } = req.body;
    const insertUserQuery = `INSERT INTO users (username, subject, message, contact) VALUES (?, ?, ?, ?)`;
    connection.query(insertUserQuery, [username, subject,  message, contact,], (err, results) => {
        if (err) {
            console.error('Error saving user data: ' + err);
            res.status(500).json({ error: 'Error saving user data' });
            return;
        }
        res.json({ message: 'User data saved successfully' });
    });
});

// Start the server
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});