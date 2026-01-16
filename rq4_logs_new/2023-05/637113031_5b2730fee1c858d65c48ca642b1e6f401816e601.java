package es.base.cliente.controller;

import es.base.cliente.gen.type.ClienteType;
import es.base.cliente.gen.type.TipoDocumentoEnum;
import es.base.cliente.gen.type.TipoPersonaEnum;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@QuarkusTest
public class ClienteControllerTest {

    @Test
    public void testCrearClienteType() {
        // Preparación de datos
        ClienteType clienteType = new ClienteType();

        clienteType.setNumeroCliente(1234);
        clienteType.setTipoPersona(TipoPersonaEnum._1_JUR_DICA);
        clienteType.setTipoDocumento(TipoDocumentoEnum.CC_CEDULA_DE_CIUDADAN_A);
        clienteType.setNumeroDocumento(1234567890);
        clienteType.setFechaExpedicionDocumento(new Date());
        clienteType.setPrimerNombre("John");
        clienteType.setNumeroCelular(1234567890);
        clienteType.setSegundoNombre("David");
        clienteType.setPrimerApellido("Doe");
        clienteType.setSegundoApellido("Smith");
        clienteType.setCorreoElectronico("johndoe@example.com");

        // Ejecución de la solicitud REST
        given()
                .contentType(ContentType.JSON)
                .body(clienteType)
                .when()
                .post("/v1/es/cliente")
                .then()
                .statusCode(200)
                .body("$.size()", is(1),
                        "[0].numeroCliente", equalTo(1234),
                        "[0].tipoPersona", equalTo(null),
                        "[0].tipoDocumento", equalTo(null),
                        "[0].numeroDocumento", equalTo(1234567890),
                        "[0].primerNombre", equalTo("John"),
                        "[0].segundoNombre", equalTo("David"),
                        "[0].primerApellido", equalTo("Doe"),
                        "[0].segundoApellido", equalTo("Smith"),
                        "[0].correoElectronico", equalTo("johndoe@example.com"),
                        "[0].numeroCelular", equalTo(1234567890));
    }
//cuidado una vez se agreguen los enum en la entity se cambian el valor del null por un valor valido!
    @Test
    public void testObtenerCliente() {
        // Ejecución de la solicitud REST
        given()
                .when()
                .get("/v1/es/cliente")
                .then()
                .statusCode(200);
    }
}