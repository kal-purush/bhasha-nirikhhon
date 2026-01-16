/* Funcion para que no se envien los campos vacios */
function validarFormulario(event) {
    event.preventDefault();
    //Definicion de variables que capturan los campos del formulario
    const nombre = document.getElementById('nombres').value;
    const identificacion = document.getElementById('nit').value;
    const edad = document.getElementById('years').value;
    const email = document.getElementById('correo').value;
    const password = document.getElementById('password').value;
    const checkCondiciones = document.getElementById('checkCondiciones').checked;
  
    //Condicional que valida si los campos del formulario estan vacios
    //Si estan vacios y se envia el formulario se muestra una alerta
    //Si no se cumplen las condiciones, se puede enviar el formulario {a || b || c}
    if (
      nombre.trim() === '' ||
      identificacion.trim() === '' ||
      edad.trim() === '' ||
      email.trim() === '' ||
      password.trim() === ''
    ) {
      //Alerta de campos vacios
      Swal.fire({
        icon: 'error',
        title: 'Falta información',
        text: 'Por favor, completa todos los campos antes de enviar el formulario.',
        confirmButtonColor: '#3085d6',
        confirmButtonText: 'Aceptar'
      });
    } else if (!checkCondiciones) {
      //Alerta terminos y condiciones
      Swal.fire({
        icon: 'warning',
        title: 'Términos y condiciones',
        text: 'Debes aceptar los términos y condiciones antes de enviar el formulario.',
        confirmButtonColor: '#3085d6',
        confirmButtonText: 'Aceptar'
      });
    } else {
      Swal.fire({
        //Alerta formulario enviado correctamente
        icon: 'success',
        title: 'Formulario enviado',
        text: 'Tu formulario ha sido enviado exitosamente.',
        confirmButtonColor: '#3085d6',
        confirmButtonText: 'Aceptar'
      });
    }
  }
  
  /*
    Escucha los clic que se le dan al boton y llama la funcion validarFormulario
  */
  const btnEnviar = document.getElementById('enviar');
  btnEnviar.addEventListener('click', function (event) {
    validarFormulario(event);
  });
  
  
  
  /* Funcion para subir archivos desde el pc a la web */
  const subirArchivos = document.querySelector('.subirArchivos');
  
  subirArchivos.addEventListener('dragover', (e) => {
    e.preventDefault();
    subirArchivos.classList.add('hover');
  });
  
  subirArchivos.addEventListener('dragleave', () => {
    subirArchivos.classList.remove('hover');
  });
  
  subirArchivos.addEventListener('drop', (e) => {
    e.preventDefault();
    subirArchivos.classList.remove('hover');
    const files = e.dataTransfer.files;
    handleFiles(files);
  });
  
  function handleFiles(files) {
    for (let i = 0; i < files.length; i++) {
      // Aquí se agrega el codigo para subir el archivo al servidor
    }
  }
  
  /* limita los archivos que se permiten subir a solo imagenes */
  function validarImagen(input) {
    var extensionesPermitidas = /(\.jpg|\.jpeg|\.png)$/i;
    if (!extensionesPermitidas.exec(input.value)) {
      Swal.fire({
        //Alerta archivos no permitidos, si se sube un archivo que no contenga las extenciones permitidas
        icon: 'warning',
        title: 'Archivo no permitido',
        text: 'Solo se permiten imágenes en formato JPG, JPEG o PNG.',
        confirmButtonColor: '#3085d6',
        confirmButtonText: 'Aceptar'
      });
      input.value = '';
      return false;
    }
  }
  
  
  
  function limitarCaracteres() {
    var input = document.getElementById("conVendedor");
    input.addEventListener("input", function() {
      if (this.value.length > 10) {
        this.value = this.value.slice(0,10);
      }
      if (this.value.length === 1 && this.value !== '3') {
        this.value = '';
      }
    });
    input.addEventListener("keydown", function(event) {
      var key = event.key;
      var regex = /^[0-9]$/; // expresión regular que permite solo caracteres numéricos
      if (!regex.test(key)) {
        event.preventDefault(); // previene la entrada del carácter no válido
      }
    });
  }