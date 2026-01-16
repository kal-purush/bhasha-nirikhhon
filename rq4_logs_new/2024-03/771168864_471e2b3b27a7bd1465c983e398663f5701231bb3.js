// Create web server

// Import modules
const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const path = require('path');

// Create web server
const app = express();
app.use(bodyParser.json());
app.use(express.static('public'));
    
// Read comments from file
const commentsPath = path.join(__dirname, 'comments.json');
let comments = [];
if (fs.existsSync(commentsPath)) {
    comments = JSON.parse(fs.readFileSync(commentsPath, 'utf8'));
}

// Get comments
app.get('/comments', (req, res) => {
    res.json(comments);
});

// Add comment
app.post('/comments', (req, res) => {
    const comment = req.body;
    comments.push(comment);
    fs.writeFileSync(commentsPath, JSON.stringify(comments, null, 4));
    res.json(comment);
});

// Start web server
const port = 3000;
app.listen(port, () => {
    console.log(`Web server is running at http://localhost:${port}`);
});

// Path: public/index.html
// Create web page


