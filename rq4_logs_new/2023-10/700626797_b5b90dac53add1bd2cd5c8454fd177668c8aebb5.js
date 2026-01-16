const express = require("express");
const  path  = require("path");
const routes  = require("./routes/routes");

const app = express();


// set static files
app.use(express.static(path.join(__dirname, '../public')));

// set template engines
app.set('view engine','ejs');
app.set('views',path.join(__dirname,'views'))

app.use(routes)

module.exports = app