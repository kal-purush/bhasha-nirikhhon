var pizzaria = require('./pizzaria')

const getUsuarios = function(){
    let usuarios = pizzaria.usuarios
    
    usuarios.forEach(function(produto, indice){
      console.log(indice + ' - Nome do Produto: ' + produto);
})

}