package uo.sdi.tests;

import org.junit.*;

import static net.sourceforge.jwebunit.junit.JWebUnit.*;

public class Sesion4Tests {

    @Before
    public void prepare() {
        setBaseUrl("http://localhost:8280/sesion4.MVCCasero");
    }

    @Test
    public void testListarViajes() {
        beginAt("/");  // Navegar a la URL
        assertLinkPresent("listarViajes");  // Comprobar que existe el hipervínculo
        clickLink("listarViajes"); // Seguir el hipervínculo

        assertTitleEquals("ShareMyTrip - Listado de viajes");  // Comprobar título de la página

        // La base de datos contiene 2 viajes tal y como se entrega
        assertElementPresent("item_0"); // Comprobar elemento presente en la página
        assertElementPresent("item_1"); // Comprobar elemento presente en la página
    }

    @Test
    public void testIniciarSesionConExito() {
    	// Rellenando el formulario HTML
        beginAt("/");  // Navegar a la URL
        setTextField("nombreUsuario", "user1"); // Rellenar primer campo de formulario
        submit(); // Enviar formulario
        assertTitleEquals("ShareMyTrip - Página principal del usuario");  // Comprobar título de la página
        assertTextInElement("login", "user1");  // Comprobar cierto elemento contiene cierto texto
        assertTextInElement("name", "Fernando");  // Comprobar cierto elemento contiene cierto texto
        assertTextPresent("Es Vd el usuario número:"); // Comprobar cierto texto está presente
    }

    @Test
    public void testIniciarSesionConExitoConQueryString() {
    	// Rellenando el formulario HTML
        beginAt("/validarse?nombreUsuario=user2");  // Navegar a la URL
        assertTitleEquals("ShareMyTrip - Página principal del usuario");  // Comprobar título de la página
        assertTextInElement("login", "user2");  // Comprobar cierto elemento contiene cierto texto
        assertTextInElement("name", "Luisa");  // Comprobar cierto elemento contiene cierto texto
        assertTextPresent("Es Vd el usuario número:"); // Comprobar cierto texto está presente
    }
    
    @Test
    public void testIniciarSesionSinExito() {
    	// Rellenando el formulario HTML
        beginAt("/");  // Navegar a la URL
        setTextField("nombreUsuario", "yoNoExisto"); // Rellenar primer campo de formulario
        submit(); // Enviar formulario
        assertTitleEquals("ShareMyTrip - Inicie sesión");  // Comprobar título de la página
    }

}