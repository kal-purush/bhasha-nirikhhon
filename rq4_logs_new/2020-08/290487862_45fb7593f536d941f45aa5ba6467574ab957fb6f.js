const { argv } = require('./config/yargs');
const lugar = require('./lugar/lugar');
const clima = require('./clima/clima');
const { getLugarLatLng } = require('./lugar/lugar');


let getInfo = async(direccion) => {
    try {
        let coors = await lugar.getLugarLatLng(direccion);
        let temperatura = await clima.getClima(coors.lat, coors.lng);
        return `el clima en  ${direccion} es de ${temperatura}`;

    } catch (e) {
        return `no se pudo determinar el clima en ${direccion}`;
    }
}




getInfo(argv.direccion)
    .then(mensaje => console.log(mensaje))
    .catch(e => console.log(e));