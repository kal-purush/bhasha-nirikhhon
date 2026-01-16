const fs = require('fs')

const FSPromisas = fs.promises
const datos = FSPromisas.readFile("data.json", 'utf-8')

function leerDatos(){
    datos
    //respuesta positiva, promesa resuelta
    .then((data)=>{ console.log("[Respuesta positiva:]", data)})
    //respuesta negativa, promesa no resuelta
    .catch((err)=>{console.log("[Respuesta Negativa:]", err)})
    //siempre va a responder
    .finally(()=>{console.log("Archivo leido")})
}
function escribirArchivo(valor, nombre){
    fs.promises
    .writeFile(nombre + ".json", valor)
    .then((info)=>console.log("Archivo escrito"))
    .catch((error)=>{console.log("Archivo no escrito")})
    .finally(()=>{console.log("Terminado!!!")});


    
}
//usando async await
 async function leerDatosAwait(){
    try {
      //.then
        const data = await FSPromisas.readFile("data.json", 'utf-8')
        console.log("[Respuesta positiva:]", data);
    }catch(error){
        //catch
        console.log("[Respuesta Negativa:]", error);
    }
}

//escribirArchivo("Hola Mundo", "CoderHouse")