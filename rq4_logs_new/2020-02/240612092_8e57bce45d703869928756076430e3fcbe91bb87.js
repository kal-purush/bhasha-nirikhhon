import Cita from "./cita.js";
import Doctor from "./doctor.js";
export default class Hospital {
    /**
     * 
     * @param {string} nombre Nombre del doctor
     * @param {string} direccion 
     * @param {string} doctores Listado de doctores
     * @param {string} citas Listado de citas
     */
    constructor(nombre, direccion) {
        this.nombre = nombre;
        this.direccion = direccion;
        this.doctores = new Array();
        this.citas = new Array();
    }
    registrarDoctor(doctor) {
        this.doctores.push(doctor);
    }
    listarDoctores() {
        this.doctores.forEach((doctor, i) => {
            console.log(`${i} ${doctor.getPerfil()}`);
        });
    }
    registrarCita(cita) {
        this.citas.push(cita);
    }
    listarCitas() {
        this.citas.forEach((cita, i) => {
            console.log(`${i} ${cita.getCita()}`);
        });
    }
}