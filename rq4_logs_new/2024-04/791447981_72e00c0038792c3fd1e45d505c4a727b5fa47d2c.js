const express = require('express');

const app = express();

app.get('/', (request, response) => {
    response.send('app is working...')
});

app.listen(5000);