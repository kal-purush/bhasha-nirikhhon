let nombreYApellido;
let pregunta__bienvenida;
let consulta;
let mensaje = 'ERROR';

let consul__clinico = 0;
let consul__trauma = 0;
let consul__psico = 0;
let consul__derma = 0;
let consul__gine = 0;
let consul = 0;

function tomar__turno(nombreYApellido) {

    let profesional_medico;
    let medico_clinico = '1';
    let traumatologo = '2';
    let psicologo = '3';
    let dermatologo = '4';
    let ginecologo = '5';
    let salir = 'S';



    do {


        profesional_medico = prompt(`Bienvenidos a consultorios Piculi ${nombreYApellido}` + '\n' +
            '¿Con que especialista desea un turno?' + '\n' +
            '1) Medico Clinico' + '\n' +
            '2) Traumatologo' + '\n' +
            '3) Psicologo' + '\n' +
            '4) Dermatologo' + '\n' +
            '5) Ginecologo' + '\n' +
            '\n' +
            '¿Desea Salir? Presione S');

        if (profesional_medico !== medico_clinico && profesional_medico !== traumatologo && profesional_medico !== psicologo && profesional_medico !== dermatologo && profesional_medico !== ginecologo && profesional_medico !== salir) {

            alert('Ingreso una opcion incorrecta!');


        } else {

            if (profesional_medico !== 'S') {
                consulta = profesional_medico;
                consul++;

                switch (consulta) {


                    case medico_clinico:
                        if (consul__clinico == 0) {
                            alert(`Perfecto ${nombreYApellido} eres el primero`);
                        } else {
                            alert(`Perfecto ${nombreYApellido} tienes ${consul__clinico} delante de ti`);
                        }

                        consul__clinico++;

                        return tomar__turno (nombreYApellido);

                    case traumatologo:
                        if (consul__trauma == 0) {
                            alert(`Perfecto ${nombreYApellido} eres el primero`);
                        } else {
                            alert(`Perfecto ${nombreYApellido} tienes ${consul__trauma} delante de ti`);
                        }

                        consul__trauma++;

                        return tomar__turno (nombreYApellido);

                    case psicologo:
                        if (consul__psico == 0) {
                            alert(`Perfecto ${nombreYApellido} eres el primero`);
                        } else {
                            alert(`Perfecto ${nombreYApellido} tienes ${consul__psico} delante de ti`);
                        }

                        consul__psico++;

                        return tomar__turno (nombreYApellido);

                    case dermatologo:
                        if (consul__derma == 0) {
                            alert(`Perfecto ${nombreYApellido} eres el primero`);
                        } else {
                            alert(`Perfecto ${nombreYApellido} tienes ${consul__derma} delante de ti`);
                        }

                        consul__derma++;

                        return tomar__turno (nombreYApellido);

                    case ginecologo:
                        if (consul__gine == 0) {
                            alert(`Perfecto ${nombreYApellido} eres el primero`);
                        } else {
                            alert(`Perfecto ${nombreYApellido} tienes ${consul__gine} delante de ti`);
                        }

                        consul__gine++;

                        return tomar__turno (nombreYApellido);


                    default:
                        alert('Usted a ingresado una opcion incorrecta');

                        return tomar__turno (nombreYApellido);



                }



            }



        }
    
    

    } while (profesional_medico !== 'S');



    alert(`Perfecto ${nombreYApellido} tenes ${consul} turnos medicos reservados ` + '\n' + 'Gracias por atenderte aqui!');

}




do {
    pregunta__bienvenida = prompt('Buenas tardes, ¿Desea reservar un turno? S/N');


    
    if (pregunta__bienvenida !== 'N') {

        nombreYApellido = prompt('Ingrese nombre y apellido por favor = ');

        if (typeof nombreYApellido !== 'string') {
            alert('No ingreso un nombre valido!!')
            nombreYApellido = prompt('Ingrese nombre y apellido por favor = ');
        } else {
            tomar__turno(nombreYApellido);
            
        }

    }else {
        alert ('Hasta luego!');
    }

} while (pregunta__bienvenida !== 'N');

alert('Muchas gracias por su visita')

