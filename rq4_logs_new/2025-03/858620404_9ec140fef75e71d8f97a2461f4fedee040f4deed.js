import Contenedor from './Logica'
import AltaDensidad from './Logica'

const todosContenedores = [];
const muyDensos=[];

function loadContendor(){
    const guardado = JSON.parse(localStorage.getItem('contenedores'))||[]
    guardado.array.forEach(c =>todosContenedores.push(new Contenedor(c.referencia,c.peso,c.volumen)) );
    listaContenedores();
}

function guardaContenedor(){
    localStorage.setItem('contenedor',JSON.stringify(todosContenedores))
}

function annadirContenedor(referencia,peso,volumen){
    if(todosContenedores.some(c=>c.referencia===referencia)){
        alert('Referencia duplicada')
        return
    }
    todosContenedores.push(new Contenedor(referencia,peso,volumen))
    guardaContenedor();
    listaContenedores();
    
}

function modificarContenedor(referencia,peso,volumen){
    const contenedor =  todosContenedores.find(c=>c.referencia===referencia)
    if(!contenedor){
        alert('No se ha encontrado el contenedor')
        return
    }
    contenedor.referencia= nuevaReferencia;
    contenedor.peso=nuevoPeso;
    contenedor.volumen=nuevoVolumen;
    guardaContenedor();
    listaContenedores();
}
function listaContenedores(){
    let tbody=document.querySelector('#tablacontenedores tbody')
    tbody.innerHTML="";
    let tr = document.createElement("tr")
    tr.innerHTML= `<td>${contenedor.referencia}</td><td>${contenedor.peso}</td><td>${contenedor.volumen}</td>`
    tbody.appendChild(tr)
}

function calcularAltaDensidad(){
    muyDensos.length=0;
    let tbodyDenso = document.querySelector('#muydensos tbody')
    tbodyDenso.innerHTML="";
    todosContenedores.forEach(contenedor=>{
        const densidad = (contenedor.peso/contenedor.volumen)
        if(densidad>2.5){
            muyDensos.push(new AltaDensidad(contenedor.referencia,contenedor.densidad.toFixed(1)));
            let trmuyDensos = document.createElement('tr');
            trmuyDensos.innerHTML=`<td>${contenedor.referencia}</td><td>${contenedor.densidad}</td>`
        }
    })
}

window.addEventListener('load',loadContendor)
document.getElementById('formAnnadir').addEventListener( 'submit',(evento)=>{
    evento.preventDefault();
    const referencia = document.getElementById('referencia').value.trim();
    const peso = parseInt(document.getElementById('peso').value);
    const volumen = parseFloat(document.getElementById('volumen').value)
    annadirContenedor(referencia,peso,volumen)
    evento.target.reset();
})

document.getElementById('formModificar').addEventListener( 'submit',(evento)=>{
    evento.preventDefault();
    const referencia = document.getElementById('modReferencia').value.trim();
    const pesoNuevo = parseInt(document.getElementById('modPeso').value);
    const volumenNuevo = parseFloat(document.getElementById('modVolumen').value)
    annadirContenedor(referencia,pesoNuevo,volumenNuevo)
    evento.target.reset();
})

document.getElementById('calcular').addEventListener('click',calcularAltaDensidad)