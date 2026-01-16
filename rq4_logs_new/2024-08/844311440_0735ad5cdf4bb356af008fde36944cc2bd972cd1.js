const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = 3000;

app.use(bodyParser.json());
app.use(express.static('public'));

let comments = [];

app.post('/add-comment', (req, res) => {
    const { author, commentText } = req.body;
    comments.push({ author, commentText });
    res.json({ success: true });
});

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});