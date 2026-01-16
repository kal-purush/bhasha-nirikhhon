package com.washi.subnpayservice;

import com.washi.subnpayservice.entity.Subscription;
import com.washi.subnpayservice.model.User;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.reactive.function.client.WebClient;

@SpringBootTest
public class SuscribirmeAWashi {
    @Autowired
    private WebClient.Builder webClientBuilder;

    Subscription subscription = new Subscription();
    User user;

    // Scenario : El washer desea visualizar sus planes de suscripción de la aplicación
    @Test
    void visualizarSuscripcion() {
        // Given : Desea ver suscripciones
        givenDeseaVisualizarSuscripciones();

        // When : Escoge su lista de suscripciones
        whenEscogeSuListaDeSuscripciones();

        // Then : Visualiza correctamente
        thenVisualizaCorrectamente();
    }

    private void givenDeseaVisualizarSuscripciones() {
        user = User.builder().name("Felipe").build();
        System.out.println(user);
    }

    private void whenEscogeSuListaDeSuscripciones() {
    }

    private void thenVisualizaCorrectamente() {
        System.out.println(subscription);
    }

    //Scenario: El washer desea ingresar una nuevo plan de suscripción a Washi
    @Test
    void ingresarNuevaSuscripcion() {

        //Given: El washer desea ingresar suscripción
        givenDeseaIngresarSuscripcion();

        //When:  Llena la nueva información
        whenLlenaLaNuevaInformacion();

        //Then:  Se suscribe correctamente
        thenSeSuscribeCorrectamente();
    }

    private void givenDeseaIngresarSuscripcion() {
        user = User.builder().name("Felipe").build();
        System.out.println(user);
    }

    private void whenLlenaLaNuevaInformacion() {
    }

    private void thenSeSuscribeCorrectamente() {
        System.out.println(subscription);
    }

    //Scenario: El washer desea actualizar uno de sus planes de suscripciones
    @Test
    void actualizarSuscripcion() {
        //Given: El washer desea actualizar suscripción
        givenDeseaActualizarSuscripcion();

        //When:  Actualiza la información
        whenActualiaSuscripcion();

        //Then: Se actualiza su suscripción
        thenSeActualizaSuSuscripcion();
    }

    private void givenDeseaActualizarSuscripcion() {
        user = User.builder().name("Felipe").build();
        System.out.println(user);
    }

    private void whenActualiaSuscripcion() {
    }

    private void thenSeActualizaSuSuscripcion() {
        System.out.println(subscription);
    }

    //Scenario: El washer desea eliminar uno de sus planes de suscripción
    @Test
    void eliminarSuscripcion() {
        //Given: El washer desea eliminar suscripción
        givenDeseaEliminarSuscripcion();

        //When:  Selecciona plan a eliminar
        whenSeleccionaPlanAEliminar();

        //Then: Se elimina correctamente
        thenSeEliminaCorrectamente();
    }

    private void givenDeseaEliminarSuscripcion() {
        user = User.builder().name("Felipe").build();
        System.out.println(user);
    }

    private void whenSeleccionaPlanAEliminar() {
    }

    private void thenSeEliminaCorrectamente() {
        System.out.println(subscription);
    }
}