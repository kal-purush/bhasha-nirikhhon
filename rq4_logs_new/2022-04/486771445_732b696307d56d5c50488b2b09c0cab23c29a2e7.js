const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    
    event.dni = event.params.path.dni;
    
    let response = {}

    /*Reglas de Validacion*/
    if (isEmpty(event.dni)) {
        response.code = 'ERR-PERSON-DNI-01';
        response.message = 'No se puede realizar la consulta, porque el dni es obligatorio';
        return response;
    }
    else if (isNaN(event.dni)) {
        response.code = 'ERR-PERSON-DNI-02';
        response.message = 'No se puede realizar la consulta, porque el dni debe ser un numero';
        return response;
    }



    //Construccion de parametros de la transaccion
    const params = {
        TableName: 'personas',
        Key: {
            dni: event.dni
        }
    }


    //Ejecucion de la Transaccion
    try {
        response.code = 'OK-TRX';
        response.data = await docClient.get(params).promise()
        if (isEmpty(response.data) || isEmpty(response.data.Item)) {
            response.message = 'Consulta realizada correctamente; pero se ha encontrado la persona con el Dni: ' +event.dni; 
            response.data = null
        }else{
            response.message = 'Consulta realizada correctamente';    
        }
      
    }
    catch (err) {
        response.code = 'ERR-TRX';
        response.message = 'Transaccion con errores';
        response.error = err;
    }

    return response;
};

/*Funcion para validar si un atributo es vacio, nulo o undefined*/
function isEmpty(value) {
    return value == null || typeof value == 'string' && (!value.trim() || typeof value == 'undefined');
}