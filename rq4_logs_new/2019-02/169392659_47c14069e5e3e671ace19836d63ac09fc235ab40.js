import Apiquery from './modules/apiquery.js';
import Reserva from './modules/reserva.js';

const formCrear = document.querySelector('#crear');
const mcrear = document.querySelector('#mcrear');
const formLeer = document.querySelector('#leer');
const mleer = document.querySelector('#mleer');
const formLeerPag = document.querySelector('#leerPag');
const mleerPag = document.querySelector('#mleerPag');
const pleerPag = document.querySelector('#pleerPag');
const formLeerUno = document.querySelector('#leerUno');
const mleerUno = document.querySelector('#mleerUno');
const formActualizar = document.querySelector('#actualizar');
const mactualizar = document.querySelector('#mactualizar');
const formEliminar = document.querySelector('#eliminar');
const meliminar = document.querySelector('#meliminar');
const inputBuscar = document.querySelector('#busqueda');
const mbuscar = document.querySelector('#mbuscar');
const inputBuscarPag = document.querySelector('#busquedaPag');
const mbuscarPag = document.querySelector('#mbuscarPag');
const lbuscarPag = document.querySelector('#limitBuscar');
let query = new Apiquery();

(function(){

  formCrear.addEventListener('submit', e => {
    e.preventDefault();
    //crear array con los valores del formulario a partir del Iterador obtenido
    //recorrer con la función filter los valores del array y limpiar valores vacíos 
    const values =  Array.from( new FormData(formCrear).values() ).filter( element => element );
    //obtener el nombre del servicio seleccionado (con FormData sólo obtenemos el valor)
    const select = document.querySelector('#servicio');
    const servicio = select.options[select.selectedIndex].text;
    //añadir el servicio al array data 
    values.splice(3,0, servicio);
      //si cumple el número de valores mínimos crear reserva o indicar que faltan datos
      if( values.length < 5 ){
        //indicar al usuario que faltan datos
        mcrear.setAttribute('class','alert alert-danger text-center mt-3');
        mcrear.innerHTML = 'Faltan Datos';
      }else{
        //variables
        query.url = 'https://zionedesign.000webhostapp.com/v1/reservas';
        query.token = 'token';
        query.method = 'POST';
        query.sec = 'crear';
        //crear data (instancia objeto Reserva)
        query.data = new Reserva(values);
        //llamar al metodo crear
        query.fetchCall();
      }  
  });

  formLeer.addEventListener('submit', e => {
    e.preventDefault();
    //variables
    query.url = 'https://zionedesign.000webhostapp.com/v1/reservas';
    query.token = 'token';
    query.method = 'GET';
    query.sec = 'leer';
    //llamar al metodo fetch
    query.fetchCall();    
  });

  formLeerPag.addEventListener('submit', e => {
    e.preventDefault();
    //variables
    //obtener el numero de registros a mostrar por página
    const select = document.querySelector('#limitLeer');
    const limit = select.options[select.selectedIndex].value;
    query.url = `https://zionedesign.000webhostapp.com/v1/reservas/?limit=${limit}`;
    query.token = 'token';
    query.method = 'GET';
    query.sec = 'leerPag';
    //llamar al metodo fetch
    query.fetchCall();    
  });

  formLeerUno.addEventListener('submit', e => {
    e.preventDefault();
    //variables
    //obtener id
    let ID = parseInt(document.querySelector('#idreserva').value); 
    query.url = `https://zionedesign.000webhostapp.com/v1/reservas/${ID}`;
    query.token = 'token';
    query.method = 'GET';
    query.sec = 'leerUno';
    //llamar al metodo fetch
    query.fetchCall();    
  });
  
  formActualizar.addEventListener('submit', e => {
    e.preventDefault();
    //crear array con los valores del formulario a partir del Iterador obtenido
    //recorrer con la función filter los valores del array y limpiar valores vacíos 
    const values =  Array.from( new FormData(formActualizar).values() ).filter( element => element );
    //obtener el nombre del servicio seleccionado (con FormData sólo obtenemos el valor)
    const select = document.querySelector('#servicioAct');
    const servicio = select.options[select.selectedIndex].text;
    //añadir el servicio al array data 
    values.splice(3,0, servicio);
      //si cumple el número de valores mínimos actualizar reserva o indicar que faltan datos
      if( values.length < 8 ){
        //indicar al usuario que faltan datos
        mactualizar.setAttribute('class','alert alert-danger text-center mt-3');
        mactualizar.innerHTML = 'Faltan Datos';
      }else{
        //variables
        //obtener id
        let ID = values.pop();
        query.url = `https://zionedesign.000webhostapp.com/v1/reservas/${ID}`;
        query.token = 'token';
        query.method = 'PUT';
        query.sec = 'actualizar';
        //crear data (instancia objeto Reserva)
        query.data = new Reserva(values);
        //llamar al metodo fetch
        query.fetchCall();
      }  
  });

  formEliminar.addEventListener('submit', e => {
    e.preventDefault();
    //variables
    //obtener id
    let ID = parseInt(document.querySelector('#idElm').value);
    query.url = `https://zionedesign.000webhostapp.com/v1/reservas/${ID}`;
    query.token = 'token';
    query.method = 'DELETE';
    query.sec = 'eliminar';
    //llamar al metodo fetch
    query.fetchCall();    
  });

  inputBuscar.addEventListener('keyup', e => {
    e.preventDefault();
    //variables
    query.token = 'token';
    query.method = 'GET';
    query.sec = 'buscar';
      //si no hay valores que buscar vaciar contenedor
      if(e.target.value == ''){
        mbuscar.innerHTML = '';
        mbuscar.removeAttribute('class');
      }
      // si hay valores
      else{
        //obtener busqueda, asignar url al objeto query y llamar al metodo fetch
        let srch = e.target.value;
        query.url = `https://zionedesign.000webhostapp.com/v1/reservas/${srch}`;
        query.fetchCall();
      }
  });

  inputBuscarPag.addEventListener('keyup', e => {
    e.preventDefault();
    //variables
    query.token = 'token';
    query.method = 'GET';
    query.sec = 'buscarPag';
      //si no hay valores que buscar vaciar contenedor
      if(e.target.value == ''){
        mbuscarPag.innerHTML = '';
        pbuscarPag.innerHTML = '';
        mbuscarPag.removeAttribute('class');
      }
      // si hay valores
      else{
        //obtener busqueda y numero de regitros, asignar url al objeto query y llamar al metodo fetch
        const srch = e.target.value;
        const limit = lbuscarPag.options[lbuscarPag.selectedIndex].value;
        query.url = `https://zionedesign.000webhostapp.com/v1/reservas/${srch}/?limit=${limit}`;
        query.fetchCall();
      }
  });

}());
 