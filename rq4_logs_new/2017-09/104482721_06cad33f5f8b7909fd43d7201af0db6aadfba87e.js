var express = require('express');
var bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());

app.get('/api', function api(req, res) {
    res.json({
        description: 'My API. Please authenticate!'
    });
});

app.listen(3000, function () {
    console.log('App listening on port 3000!');
});