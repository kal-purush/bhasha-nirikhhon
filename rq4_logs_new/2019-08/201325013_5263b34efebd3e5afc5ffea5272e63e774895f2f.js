const express = require('express')
const cors = require('cors')
const bodyParser = require('body-parser');
const app = express()
const port = 3000

require('dotenv').config()

// Models
var Paste = require('./models/pastes'); 

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

// Routes
app.get('/', function(req, res) {
    var number = 
    res.send(number)
})

app.post('/api/paste', function(req, res) {
    var paste = req.body.paste
    var language = req.body.language
    var page_id = Math.random().toString(36).replace('0.', '')

    return Paste.create ({
        page_id: page_id,
        paste: paste,
        language: language
    })
    .then(function(Paste) {
        console.log(`New Paste ${Paste}`)
        res.json( {"data": {"page_id": page_id, "paste": paste, "language": language }});
    })
})

app.get('/api/pastes/:page_id', cors(), function(req, res) {
    Paste.findAll({ where: {page_id: req.params.page_id} }).then(pastes => res.json(pastes))
})

// Listen
app.listen(port, () => console.log(`Example app listening on port ${port}!`))