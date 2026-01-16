    // Datos simulados de pagos
    var pagos = [
        { idNota: 1, idCliente: 1, nombreCliente: 'Cliente 1', monto: 100, evidencia: 'pdf1.pdf', fecha: '2022-01-01', estado: 'validado' },
        { idNota: 2, idCliente: 2, nombreCliente: 'Cliente 2', monto: 200, evidencia: 'pdf2.pdf', fecha: '2022-01-02', estado: 'pendiente' },
        { idNota: 3, idCliente: 3, nombreCliente: 'Cliente 3', monto: 300, evidencia: 'pdf3.pdf', fecha: '2022-01-03', estado: 'no-pagado' }
      ];
  
      // Función para generar la tabla de pagos
      function generarTablaPagos() {
        var tabla = document.getElementById('tabla-pagos');
        tabla.innerHTML = '';
  
        pagos.forEach(function(pago) {
          var fila = document.createElement('tr');
  
          fila.innerHTML = `
            <td>${pago.idNota}</td>
            <td>${pago.idCliente}</td>
            <td>${pago.nombreCliente}</td>
            <td>${pago.monto}</td>
            <td>${pago.evidencia}</td>
            <td>${pago.fecha}</td>
            <td class="${pago.estado}">${pago.estado}</td>
            <td>
              <button onclick="validarPago(${pago.idNota})">Validar</button>
            </td>
          `;
  
          tabla.appendChild(fila);
        });
      }
  
      // Función para validar un pago
      function validarPago(idNota) {
        // Aquí puedes implementar la lógica para cambiar el estado del pago en la base de datos o en el arreglo pagos
        console.log('Validando pago con ID de Nota:', idNota);
      }
  
      // Generar la tabla de pagos al cargar la página
      generarTablaPagos();


      function logout() {
        var confirmLogout = confirm("¿Estás seguro de que deseas cerrar sesión?");
        if (confirmLogout == true) {
            window.location.href = "index.php?";
        }
    }

// Función para ocultar todas las secciones
function ocultarSecciones() {
    var secciones = document.querySelectorAll('main > section');
    for (var i = 0; i < secciones.length; i++) {
        secciones[i].style.display = 'none';
    }
}

// Función para mostrar una sección específica
function mostrarSeccion(id) {
    ocultarSecciones();
    document.getElementById(id).style.display = 'block';
}

// Asignar los eventos de click a los enlaces del menú
var enlaces = document.querySelectorAll('nav ul li a');
for (var i = 0; i < enlaces.length; i++) {
    enlaces[i].addEventListener('click', function(e) {
        e.preventDefault();
        mostrarSeccion(this.getAttribute('href').substring(1));
    });
}

window.onload = function() {
  // Ocultar todas las secciones al cargar la página
  document.getElementById('pagos').style.display = 'none';
  document.getElementById('Usuarios').style.display = 'none';
  document.getElementById('formulario-crear-usuario').style.display = 'none';

  // Agregar evento de clic a los enlaces de la barra de navegación
  document.querySelectorAll('nav ul li a').forEach(function(enlace) {
    enlace.addEventListener('click', function(event) {
      event.preventDefault(); // Evita el comportamiento predeterminado del enlace

      // Ocultar todas las secciones
      document.getElementById('pagos').style.display = 'none';
      document.getElementById('Usuarios').style.display = 'none';
      document.getElementById('formulario-crear-usuario').style.display = 'none';

      // Mostrar la sección correspondiente
      var seccion = document.getElementById(this.getAttribute('href').substring(1));
      if (seccion) {
        seccion.style.display = 'block';
      }
    });
  });
};

function logout() {
    console.log('Cerrar sesión');
}