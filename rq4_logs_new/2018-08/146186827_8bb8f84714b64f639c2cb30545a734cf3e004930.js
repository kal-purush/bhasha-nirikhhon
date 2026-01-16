const express = require('express');
const  bodyParser = require('body-parser');
const  cors = require('cors');
const  mongoose = require('mongoose');
const regRoutes = require('./exproutes/routes');
const auth = require('./exproutes/docControl');

mongoose.Promise = global.Promise;

mongoose.connect('mongodb://127.0.0.1:27017/FIR',{
    useNewUrlParser: true
});
mongoose.Promise = require('bluebird');
mongoose.set('useFindAndModify', false);
mongoose.set('useCreateIndex', true);
    
const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(cors());
app.use('/docs', regRoutes);
app.use('/api/auth', auth);
app.get('/', function(req, res) {
    res.send('Use Api End Points');
    res.end();
});
app.listen(4000, function() {
    console.log("App is Running at port 4000");
});