const express = require('express');
const app = express();
const PORT = process.env.PORT || 8080;

app.get('/', (req, res) => res.send('Microservice 2 says Hello!'));

app.listen(PORT, () => console.log(`Microservice 2 running on port ${PORT}`));