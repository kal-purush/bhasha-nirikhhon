const fs = require('fs');
const colors = require('colors');

const crearArchivo = async ( base = 5,listar=false,hasta) =>{
    try {
        let salida = '';
        for(let i=1;i<hasta+1;i++){
            salida += 'resultado: ' + i*base + '\n';
        }
        if(listar){
            console.log('salida');
            console.log(salida);
        }
        fs.writeFileSync(`./salida/tabla-${base}.txt`,salida);  
        return `tabla-${base}.txt`
        
    } catch (error) {
        throw error;
    }
        
}

module.exports = {
    crearArchivo:crearArchivo
}