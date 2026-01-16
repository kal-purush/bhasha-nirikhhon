import express from 'express'

const port = 5000;

const app = express();

app.get('/', (req, res) => {
    res.send('Successful PassrAPI!')
});

app.listen(port, () => {
    console.log('Running Passr API.');
});