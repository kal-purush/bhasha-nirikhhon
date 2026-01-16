const express = require('express');
const path = require('path');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const port = process.env.NODE_PORT || 3000
var server = app.listen(port, '0.0.0.0', function (err) {
    if (err) {
        console.error(err)
        return
    }
    console.log('Example app listening at http://%s:%s',
        server.address().address,
        server.address().port
    );
});