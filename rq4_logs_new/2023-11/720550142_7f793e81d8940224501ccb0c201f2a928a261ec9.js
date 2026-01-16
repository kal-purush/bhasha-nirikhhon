let medico = [
    { 
        nombre:'Juan Perez',
        apellido:'Mendez',
        dni:'12431235',
        email:'pepe@olis',
        password:'12343asd',
        matricula:'12412',
        isAdmin:false,
        especialidad:'Pediatra'
    },
    {
        nombre:'Jose',
        apellido:'Pierini',
        dni:'12431235',
        email:'pepe@olis',
        password:'12343asd',
        matricula:'12412',
        isAdmin:false,
        especialidad:'Oncologo'
    },
    {
        nombre:'Jose',
        apellido:'Pierini',
        dni:'12431235',
        email:'pepe@olis',
        password:'12343asd',
        matricula:'12412',
        isAdmin:false,
        especialidad:'Oncologo'
    }
    
  ];
  let medicoPendiente =[
    {
        nombre:'pepito augusto',
        apellido:'Jodar',
        email:'peptio@23123.com',
        password:'34124aasd',
        dni:12341234
    },
    {
        nombre:'pepito 2',
        apellido:'yeins',
        email:'peptssio@23123.com',
        password:'34124a12asd',
        dni:12341234
    },
    {
        nombre:'pepito ',
        apellido:'Martinez',
        email:'pept22io@23123.com',
        password:'34124aasd',
        dni:12341234
    },
    {
        nombre:'Pepsi',
        apellido:'Jodar',
        email:'peptd22io@23123.com',
        password:'34124aasd',
        dni:12341234
    },
    {
        nombre:'augusto',
        apellido:'Jodar',
        email:'peptio@23123.com',
        password:'34124aasd',
        dni:12341234
    }
  ]
  let paciente = [
    { 
        nombre:'Juan Perez',
        apellido:'Mendez',
        dni:'12431235',
        email:'pepe@olis',
        password:'12343asd',
        matricula:'12412',
        loged:false,
        especialidad:'Pediatra'
    },
    {
        nombre:'Jose',
        apellido:'Pierini',
        dni:'12431235',
        email:'pepe@olis',
        password:'12343asd',
        matricula:'12412',
        isAdmin:false,
        especialidad:'Oncologo'
    },
    {
        nombre:'Jose',
        apellido:'Pierini',
        dni:'12431235',
        email:'pepe@olis',
        password:'12343asd',
        matricula:'12412',
        isAdmin:false,
        especialidad:'Oncologo'
    }
    
  ];
  let pacientePendiente =[
    {
        nombre:'pepito augusto',
        apellido:'Jodar',
        email:'peptio@23123.com',
        password:'34124aasd',
        dni:12341234,
        loged:false
    },
    {
        nombre:'pepito 2',
        apellido:'yeins',
        email:'peptssio@23123.com',
        password:'34124a12asd',
        dni:12341234,
        loged:false
    },
    {
        nombre:'pepito ',
        apellido:'Martinez',
        email:'pept22io@23123.com',
        password:'34124aasd',
        dni:12341234,
        loged:false
    },
    {
        nombre:'Pepsi',
        apellido:'Jodar',
        email:'peptd22io@23123.com',
        password:'34124aasd',
        dni:12341234,
        loged:false
    },
    {
        nombre:'augusto',
        apellido:'Jodar',
        email:'peptio@23123.com',
        password:'34124aasd',
        dni:12341234,
        loged:false
    }
  ]
function cargarObjetos() {
    localStorage.setItem('medico', JSON.stringify(medico));
    localStorage.setItem('medicoPendiente', JSON.stringify(medicoPendiente));
    localStorage.setItem('paciente', JSON.stringify(paciente));
    localStorage.setItem('pacientePendiente', JSON.stringify(pacientePendiente));
    localStorage.setItem('turno', JSON.stringify(turno));
}