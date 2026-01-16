//import modules
const express = require('express');
const app = express();
const bodyParser = require('body-parser')

//use bodyparser
app.use(bodyParser.urlencoded({ extended: true }));

//use static files
app.use(express.static('public'));
app.use(express.static('node_modules/bulma/css'));

//set view enige
app.set("view engine", "ejs");

//posting a message
app.get('/postmessage', (req, res) => {
    res.render('postmessage')
})

app.listen(3000, () => {
    console.log('Server listening on port 3000!')
})

