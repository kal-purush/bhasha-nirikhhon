const express = require('express'),
    app = express();

app.use(express.static('./src'));
app.use('/', (req, res) => {
    res.sendfile('./src/index.html');
});

app.listen(8080);
console.log("App listening on port 8080");