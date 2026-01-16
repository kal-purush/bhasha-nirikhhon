let express = require('express');
const path = require('path');
let app = express();

app.use('/css', express.static(path.join(__dirname, 'css')));
app.use('/img', express.static(path.join(__dirname, ('img'))));
app.use('/vendor', express.static(path.join(__dirname, 'vendor')));

app.get('/', function (req, res) {
    res.sendfile(path.join(__dirname, 'index.html'));
});

const port = process.env.PORT || 8080;

app.listen(port, function(){
    console.log('Express server listening on port', port);
});