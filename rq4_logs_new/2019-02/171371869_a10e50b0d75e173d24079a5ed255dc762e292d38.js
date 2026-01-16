var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var port = process.env.PORT || 3000;
//Cria um app EXPRESS que usa uma conexão
//http como servidor deste aplicativo
//que realiza um conexão a de socket deste app

//socket.io= bilbioteca que permite conexão
//entre o servidor e o navegador

app.get('/', function(req, res){
  res.sendFile(__dirname + '/index.html');
});
//a aplicação sendo iniciada com o host padrão,
//ou seja, "localhost" ou "www.google.com"
//ela verá a "/" e carregará o index.html

io.on('connection', function(socket){
  socket.on('chat message', function(msg){
    io.emit('chat message', msg);
    console.log("Msg: " + msg);
  });
});
//o objeto io usara a connection para carregar uma
//função de socket (comunicação) que usará
//o "chat message" como parametro de entrada do
// io.emit (que 'emite' uma msg  na tela);

http.listen(port, function(){
  console.log('listening on *:' + port);
});