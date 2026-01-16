/**
* Funciones Y Ajax de clientes
**/

document.getElementById('nuevoCliente').addEventListener('click',function(){
	agregarCliente();
},true);


agregarCliente = function () {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function () {
        if (this.readyState == 4) if (this.status == 200) {
				modal.innerHTML = this.responseText;
        }
    };
    xhttp.open("GET","modulos/cli/agregar.php",true);
    xhttp.send();
};



var clienteDetalle = function(id){
	clienteDetalleAj(id);
};
clienteDetalleAj = function (str) {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function () {
        if (this.readyState == 4) if (this.status == 200) {
				modal.innerHTML = this.responseText;
        }
    };
    xhttp.open("GET","modulos/cli/modalCliDetalle.php?id="+str,true);
    xhttp.send();
};


var clienteBorrar = function(id){
	clienteBorrarAj(id);
};
clienteBorrarAj = function (str) {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function () {
        if (this.readyState == 4) if (this.status == 200) {
				modal.innerHTML = this.responseText;
        }
    };
    xhttp.open("GET","modulos/cli/modalCliBorrar.php?id="+str,true);
    xhttp.send();
};