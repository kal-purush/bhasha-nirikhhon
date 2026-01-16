const db = firebase.firestore();

let cerrar = document.querySelectorAll('.cierre')[0];
let abrir = document.querySelectorAll('.modal1')[0];
let editar = document.querySelectorAll('.btnsedi')[0];
let modal = document.querySelectorAll('.modal')[0];
let modalC = document.querySelectorAll('.contenedor-modal')[0];
let modal2 = document.querySelectorAll('.modal')[0];
let modalC2 = document.querySelectorAll('.contenedor-modal')[0];
let buscar = document.querySelectorAll('.btnbuscar')[0];

abrir.addEventListener("click", function(e){
    e.preventDefault();
    modalC.style.opacity = "1";
    modalC.style.visibility = "visible";
    modal.classList.toggle("cierre-modal");
});
cerrar.addEventListener("click", function(){
    modal.classList.toggle("cierre-modal");
    setTimeout(function(){
        modalC.style.opacity = "0";
        modalC.style.visibility = "hidden"
    },700)
});
//--------------------------------------------------------------------------------------------------------------------
const buscando = db.collection('clientedb');
const user = document.getElementById('cliente').value;
buscar.addEventListener('click', function(){
    var clienteBuscado = buscando.where('nombre', '==', user);
    const usuario = clienteBuscado.data();
    console.log(usuario)
})
//---------------------------------------------------------------------------------------------------------------------
const regcliente = document.getElementById('regcliente');
const cuerpo = document.getElementById('cuerpo');

let editEstado = false;
let id = '';

const guardarCliente = (nombre, apellido1, apellido2, edad) =>
    db.collection('clientedb').doc().set({
        nombre: nombre,
        apellido1: apellido1,
        apellido2: apellido2,
        edad: edad
    });

const obtenerClientes = () => db.collection('clientedb').get();
const onClientes = (llamar) => db.collection('clientedb').onSnapshot(llamar);
const eliminarCliente = id =>db.collection('clientedb').doc(id).delete();
const obtenerDCliente = (id) => db.collection('clientedb').doc(id).get();
const actualizarCliente = (id, updatedTask) => db.collection('clientedb').doc(id).update(updatedTask);

window.addEventListener('DOMContentLoaded', async (e) => {

    onClientes((querySnapshot) => {
        cuerpo.innerHTML = '';
        querySnapshot.forEach(doc => {
           // console.log(doc.data())
    
            const tdatos = doc.data();
            tdatos.id = doc.id;
            cuerpo.innerHTML += 
            `
            <td class="tdcampos">${tdatos.nombre}</td>
            <td class="tdcampos">${tdatos.apellido1}</td>
            <td class="tdcampos">${tdatos.apellido2}</td>
            <td class="tdcampos">${tdatos.edad}</td>
            <td class="tdbotones">
                <button class="btn btnEdit" data-id="${tdatos.id}">Editar</button>
                <button class="btn btnDelete" data-id="${tdatos.id}">Elimar</button>
            </td>
            `;

            const btnsDelete = document.querySelectorAll('.btnDelete');
            btnsDelete.forEach(btn => {
                btn.addEventListener('click', async (e) => {
                    
                    await eliminarCliente(e.target.dataset.id);
                })
            })

            const btnsEdit = document.querySelectorAll('.btnEdit');
            btnsEdit.forEach((btn) => {
                btn.addEventListener('click', async (e) => {
                    const doc = await obtenerDCliente(e.target.dataset.id)
                    //console.log(doc.data())
                        e.preventDefault();
                        modalC2.style.opacity = "1";
                        modalC2.style.visibility = "visible";
                        modal2.classList.toggle("cierre-modal");
                    
                        const valor = doc.data();
                        editEstado = true;
                        id = doc.id;

                        regcliente['nombre'].value = valor.nombre;
                    regcliente['apellido1'].value = valor.apellido1;
                    regcliente['apellido2'].value = valor.apellido2;
                    regcliente['edad'].value = valor.edad;
                    regcliente['btn-enviar'].innerText = 'Actualizar';
                })
            })
        })
    })
})

regcliente.addEventListener('submit', async (e) => {
    e.preventDefault();
    const nombre = regcliente['nombre'];
    const apellido1 = regcliente['apellido1'];
    const apellido2 = regcliente['apellido2'];
    const edad = regcliente['edad'];

    if(!editEstado){
        await guardarCliente(nombre.value, apellido1.value, apellido2.value, edad.value);
    }else{
        await actualizarCliente(id, {
            nombre: nombre.value,
            apellido1: apellido1.value,
            apellido2: apellido2.value,
            edad: edad.value
        })
        editEstado = false;
        id = '';
        regcliente['btn-enviar'].innerText = 'Guardar';
    }
    regcliente.reset();

})
//---------------------------------------------------------------------------------------------------------------------

/*
class Busqueda{
    constructor(){
        this.personas = [
            {nombre:"Juan", edad:34},
            {nombre:"Pedro", edad:23},
            {nombre:"Marian", edad:22}
        ];
        this.personasBk = this.personas;
        this.onInit();
        console.log(this.personas)
    }
    onInit(){
        let cuerpo = document.getElementById('cuerpo');
        while(cuerpo.rows.length > 0){
            cuerpo.deleteRow(0);
        }
        this.personas.forEach(persona => {
            let fila = cuerpo.insertRow(cuerpo.rows.length);
            fila.insertCell(0).innerHTML = persona.nombre;
            fila.insertCell(1).innerHTML = persona.edad;
        });
    }
    buscar(id){
        let busqueda = document.getElementById(id).value;
        this.personas = this.personasBk;
        this.personas = this.personas.filter(persona => {
            return persona.nombre.toLowerCase().indexOf(busqueda) > -1;
        });
        this.onInit();
    }
}
let busqueda = new Busqueda();
let form = document.getElementById('buscarCliente');
form.addEventListener('submit', () => {
    busqueda.buscar('cliente');
})
*/