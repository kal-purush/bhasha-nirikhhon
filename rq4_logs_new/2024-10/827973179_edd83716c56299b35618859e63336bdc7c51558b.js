const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(bodyParser.json());

// Serve static files from the 'public' directory
app.use(express.static(path.join(__dirname, 'public')));

// Store generated codes in memory
const generatedCodes = new Set();

// Serve the index.html file
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Endpoint to generate a 64-digit code
app.get('/generate-code', (req, res) => {
    const code = crypto.randomBytes(32).toString('hex'); // 64 hex digits = 32 bytes
    generatedCodes.add(code); // Store the generated code
    res.json({ code }); // Return the generated code as JSON
});

// Endpoint to validate a generated code
app.post('/validate-code', (req, res) => {
    const { code } = req.body;
    if (generatedCodes.has(code)) {
        res.json({ valid: true, message: 'Code is valid.' });
    } else {
        res.json({ valid: false, message: 'Code is invalid.' });
    }
});

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});