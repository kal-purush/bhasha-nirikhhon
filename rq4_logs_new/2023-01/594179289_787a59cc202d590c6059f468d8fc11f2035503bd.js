//app aplicação principal

//fazendo a importação do Express
const express = require('express');
const router = require('./routes/index');
const mustache = require('mustache-express');

//configuraçõa basica do aplicativo
const app = express();
app.use('/', router); //foi passado 1 rota pois criamos apenas 1 rota

app.use(express.json());

app.engine('mst', mustache( __dirname + '/views/partials', '.mst'));
app.set('view engine', 'mst'); 

app.set('views', __dirname + '/views/');

module.exports = app; //exportamos o app, pois vamos importa-lo no servidor
