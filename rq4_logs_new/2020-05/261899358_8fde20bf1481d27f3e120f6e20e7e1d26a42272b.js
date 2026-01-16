// import do express
var express = require('express');
// cria um objeto do express
var app = express();

//ponto de acesso (endpoit)
//request = requisição
//response = resposta do servidor
app.get('/', function (request, response) {
  response.send('Tulio Metal!');
});

//A porta que o node irá expor
app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});