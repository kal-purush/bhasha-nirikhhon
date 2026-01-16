import { buscarEmpleadoAsync } from "./promesas.js";

const empleadosNombres=['informatica','recursosHumanos','ventas'];

export const obtenerEmpleadosArr=async()=>{

    return await Promise.all(empleadosNombres.map(buscarEmpleadoAsync));
};