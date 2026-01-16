var express = require('express');

var app = express();

app.use('/', express.static('public'));

// since Step 1 use port 3000 & Step 2 use port 3001, we use port 4000 here
app.listen(4000);