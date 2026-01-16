// Create web server
// Access: http://localhost:3000/comments
// Access: http://localhost:3000/comments/1
// Access: http://localhost:3000/comments/2

// Import express module
const express = require('express');

// Create express app
const app = express();

// Create route for GET /comments
app.get('/comments', function (req, res) {
  res.send('GET /comments');
});

// Create route for GET /comments/1
app.get('/comments/1', function (req, res) {
  res.send('GET /comments/1');
});

// Create route for GET /comments/2
app.get('/comments/2', function (req, res) {
  res.send('GET /comments/2');
});

// Start server
app.listen(3000, () => console.log('Server is listening on port 3000'));