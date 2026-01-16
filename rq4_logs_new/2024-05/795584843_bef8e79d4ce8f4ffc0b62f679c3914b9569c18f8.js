const { exec } = require('child_process');
//const fs = require('fs');

// Función para generar la clave privada y el CSR
function generarClavePrivadaYCSR() {
    // Paso 1: Generar la clave privada
    exec('openssl genrsa -out pkAfip.key 2048', (error1, stdout1, stderr1) => {
        if (error1) {
            console.error(`Error al generar la clave privada: ${error1.message}`);
            return;
        }
        if (stderr1) {
            console.error(`Error: ${stderr1}`);
            return;
        }
        console.log(`Clave privada generada: ${stdout1} OK`);

        // Paso 2: Generar el CSR (Certificate Signing Request)
        // Ejecutar el comando
        const comando = 'openssl req -new -key pkAfip.key -subj "/C=AR/O=EmpresaPrueba/CN=TestSystem/serialNumber=CUIT 20435359974" -out MiPedidoCSR.pem'
        exec(comando, (error, stdout, stderr) => {
            if (error) {
                console.error(`Error al ejecutar el comando: ${error.message}`);
                reject(error);
                return;
            }
            console.log(`Archivo MiPedidoCSR.pem generado con éxito: ${stdout} Ok`);
        });
    });
}

generarClavePrivadaYCSR();