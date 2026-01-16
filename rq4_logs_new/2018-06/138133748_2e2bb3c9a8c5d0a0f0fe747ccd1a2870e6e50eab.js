// initialisation du server
const express = require('express');
const app = express();
var port = 3013;


app.use(express.static('public'));

// afficher l'index

app.get('/', function(req,res){
    res.sendFile(__dirname+'/index.html')
});



// port d'écoute du serveur

app.listen(port, function(){
    console.log('the port is on')
})