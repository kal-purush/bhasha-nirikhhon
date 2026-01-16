/* Codigo Javascript */

/*
Variable : var, let y const
- Var: Global
- Let: Local
- Const: Constante
*/

var edad = 10;
let edad2 = 105;
const edad3 = 205;

console.log(edad);
// alert(edad2);

// VARIABLES BOOLEANAS
const alto = false;
const nombre = "Carlos";
// edad = nombre;
// console.log(edad);

// Saber tipo de una variable
console.log(typeof edad);
console.log(typeof nombre);
console.log(typeof alto);

// CONVERTIR STRING A NUMERO -----------------------------------

// Number
const edadStr = "10";
const edadNumber = Number(edadStr);
console.log(edadNumber);

// +
const edadmas = +edadStr;
console.log(edadmas);

// parseInt
const edadParseInt = parseInt(edadStr);
console.log(edadParseInt);

// Diferencias entre Number y parseInt. ParseInt pierde los decimales y Number no

// MODIFICAR TEXTO -----------------------------------
const apellido = "lopez";
console.log(apellido.toUpperCase());
console.log(apellido.toLowerCase());
console.log(apellido.length);
console.log("hola".repeat(5));
console.log(apellido.replace("op", "oopp"));

// Si no defines el tipo, se trata como global por defecto
nombreVariable = 10;

// CREACION DE OBJETOS -----------------------------------
const persona = {
    dni: "12345",
    nombre: "Carlos",
    edad: 36
}

console.log(typeof (persona));
console.log(persona);

// Cambiar sus atributos siendo constante el objeto
persona.dni = "54321";
console.log(persona);

// UNDEFINED -----------------------------------
let vx; // que valor tiene? Ninguno. Es Undefined
console.log(vx);
console.log("Tipo undefined - " + typeof (vx));

// NULL -----------------------------------
const nulo = null;
console.log(nulo);
console.log("Tipo null - " + typeof (nulo));

// OPERADORES ARITMETICOS (+ - / *) -----------------------------------
console.log(1 + 1);
console.log(1 - 1);
console.log(1 * 1);
console.log(1 / 1);
console.log(1 / 0);

// OPERADORES RELACIONAL (< > >= <=) -----------------------------------
console.log("1 < 1", 1 < 1);
console.log("1 === 1", 1 === 1);
console.log("1 <= 1", 1 <= 1);
console.log("2 != 3", 2 != 3);

// === ???
console.log("2" == 2);  // true
console.log("2" === 2); // false // Compara valor y tipo

// OPERADORES LOGICOS (?? && || ! xor > boolean | true o false) -----------------------------------
const username = 'user000';
const password = '1234';

const valido = (username === 'user000' && password === '1234');
console.log("valido", valido);
console.log("!valido", !valido);

const ovalido = (username === 'user000' || password === '0000');
console.log("ovalido", ovalido)
console.log("!ovalido", !ovalido)

// FUNCIONES ----------------------------------

// Definición función
function saludar() {
    alert("Hola, dentro de saludar");
}

// Invocar función
// saludar();

// Función con devolución
function saludarConReturn() {
    return "Hola, dentro de saludar con return";
}

const retornoDeLaFuncion = saludarConReturn();
console.log(retornoDeLaFuncion);

// FUNCIONES CON PARÁMETROS ----------------------------------
// -------- IF 
function calcular(param1, param2, op) {
    let res;

    // Si op == + -> Sumar
    if (op === '+') {
        res = Number(param1) + parseInt(param2);
    }

    // Si op == - -> Restar
    if (op === '-') {
        res = Number(param1) - parseInt(param2);
    }

    // Si op == / -> Dividir
    if (op === '/') {
        res = Number(param1) / parseInt(param2);
    }

    // Si op == * -> Multiplicar
    if (op === '*') {
        res = Number(param1) * parseInt(param2);
    }

    return res || `${op} - Operación Inválida;` //alt+96
    // return res || 0; -> Si res es undefined, devolver 0
}

const resultado = calcular(100, 2, '/');
console.log(resultado);
const resultado2 = calcular(100, 2, '88');
console.log(resultado2);

// -------- IF ELSE

function calcularv2(param1, param2, op) {
    let res;
    if (op === '+') {
        res = Number(param1) + parseInt(param2);
    } else {
        if (op === '-') {
            res = Number(param1) - parseInt(param2);
        } else {
            if (op === '/') {
                res = Number(param1) / parseInt(param2);
            } else {
                if (op === '*') {
                    res = Number(param1) * parseInt(param2);
                }
            }
        }
    }
    return res || `${op} - Operación Inválida;`
}

// -------- ELSE IF

function calcularv3(param1, param2, op) {
    let res;
    if (op === '+') {
        res = Number(param1) + parseInt(param2);
    } else if (op === '-') {
        res = Number(param1) - parseInt(param2);
    } else if (op === '/') {
        res = Number(param1) / parseInt(param2);
    } else if (op === '*') {
        res = Number(param1) * parseInt(param2);
    }
    return res || `${op} - Operación Inválida;`
}
// -------- SWITCH

function calcularv4(param1, param2, op) {
    let res;
    switch (op) {
        case '+':
            res = Number(param1) + parseInt(param2);
            break;
        case '-':
            res = Number(param1) - parseInt(param2);
            break;
        case '/':
            res = Number(param1) / parseInt(param2);
            break;
        case '*':
            res = Number(param1) * parseInt(param2);
            break;
        default:
            res = `${op} operación inválida`;
    }
    return res;
}


// FUNCIONES ANIDADAS ----------------------------------

function padre(url, var1) {
    function hija() {
        return `${url}/${var1}`;
    }
    return hija();
}

// api/client/6
let fHija = padre('http://api/client', 1); // http://api/client/1
fHija = padre('http://otraapi/client', 1); // http://otraapi/client/1


// FUNCIONES LAMBA - PASAR FUNCIONES COMO PARAMETROS ----------------------------------
function fx() {

}

fx = function () { }

// nueva!!
fx = (param1, param2) => {
    return param1 + param2;
}

// Invocar
const algo = fx('a', 'b');
console.log(algo);

// Funcion que recibe funcion
const x = (fy, px) => { //callback
    console.log('ejecutando x(fy,px)')
    return fy(px);
}

// Invocar x
const f = (px) => {
    console.log("No hace nada");
}
x(f, 'curso angular loyal');

// Se pueden pasar funciones como parametros al invocar funciones!!!


// ARRAY, ARREGLOS, VECTORES ----------------------------------
const edades = [10, 20, 30]; // Tamaño es tres. Las posiciones son 0, 1 y 2

// Agregar elementos
console.log(edades[3]);
edades.push(40);
console.log(edades[3]);

// RECORRER ARRAYS ----------------------------------
// While - No conoces el tamaño, pero si la condicion de corte. 
// Do While - Evalua antes de iniciar el ciclo y evalua después del ciclo. Garantiza al menos una vez

// For - Conoces el tamaño
for (let i = 0; i < edades.length; i++) {
    const aux = edades[i];
    console.log(`${i}`, aux);
}

// For each - No conoces el tamaño
edades.forEach(aux => console.log(aux));


// FILTRAR ARRAYS - Filter/map/recuce/every/some---------------------------------
const apellidos = [
    {
        id: 1,
        apellido: 'CAMPS',
        direccion: {
            calle: 'calle 13',
            altura: 1234
        }

    },
    {
        id: 2,
        apellido: 'REYES',
        direccion: {
            calle: 'calle 13',
            altura: 1234
        }
    },
    {
        id: 3,
        apellido: 'GOMEZ',
        direccion: {
            calle: 'calle 13',
            altura: 1234
        }
    },
    {
        id: 4,
        apellido: 'ZAMORA',
        direccion: {
            calle: 'calle 13',
            altura: 1234
        }
    }
];

apellidos.push(
    {
        id: 5,
        apellido: 'ZAMORA',
    }
);

console.log('apellidos', apellidos);

// Forma clásica: Recorrer array y comparar cada valor hasta encontrar los deseados

// FILTER
const listaGomez = apellidos.filter(aux => aux.apellido === 'GOMEZ');
console.log(listaGomez);

// MAP - convertir o mappear algo en otra cosa
const ids = apellidos.map(x => {
    return {
        id: x.id,
        apellido: x.apellido
    }
}
);
console.log(ids);

// EVERY 

// Son todos los apellidos Zamora??
const todosZamora = apellidos.every(x => x.apellido === 'ZAMORA'); // Devuelve boolean
console.log(todosZamora); //false

// Son todos los id mayor a 0??
const todos0 = apellidos.every(x => x.id > 0); // Devuelve boolean
console.log('x.id > 0 -> ¿Todos mayor que 0?', todos0);

// SOME
const existeZamora = apellidos.some(x => x.apellido === 'ZAMORA'); // Devuelve boolean
console.log('Existe Zamora?', existeZamora); //true

















