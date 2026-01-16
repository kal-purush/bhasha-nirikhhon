const express = require('express');
const cors = require('cors');

const employeeData = require('./data.json');
const app = express();

app.use(cors());
app.options('*', cors());
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

app.get('/', (req, res) => {
    res.send('hello');
});
app.get('/employees', (req, res, next) => {
    res.status(200).json(employeeData);
});

const port = process.env.PORT || 3000;

app.listen(port, () => console.info('server is up on port ' + port));