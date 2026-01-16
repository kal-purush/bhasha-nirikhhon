
// Se recomienda que el archivo principal "appa" este lo mas limpio posible

const { crearArchivo } = require('./helpers/multiplicar');
const argv = require('./config/yargs');
const colors = require('colors'); // https://www.npmjs.com/package/colors


console.clear();

console.log( argv );         // Vine de yargs
// node app -b 2 = cual tabla va a generar
// argv.l = lista la aplicacion
crearArchivo( argv.b, argv.l, argv.h )
    .then( nombreArchivo => console.log(nombreArchivo.rainbow, 'creado' ) )
    .catch( err => console.log(err) );



/** TAREA 1 - COMENTADA - El ejemplo comentado se puede ejecutar mejor con yargs
// ********************** 
// Recibir informacion desde linea de comando
// Manera de ejecutarlo en consola: node app --base=9
// **********************
console.group(process.argv); // Nativo de node
const [ , , arg3 = 'base=5'] = process.argv;
const [ , base = 5 ] = arg3.split('=');
console.log( base ); 
// Crear archivo
crearArchivo( base )
    .then( nombreArchivo => console.log(nombreArchivo, 'creado' ) )
    .catch( err => console.log(err) );
END TAREA 1 COMENTADA */