import au.com.dius.pact.consumer.Pact;
import au.com.dius.pact.consumer.PactProviderRule;
import au.com.dius.pact.consumer.PactVerification;
import au.com.dius.pact.consumer.dsl.DslPart;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.model.PactFragment;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;

import org.junit.Rule;
import org.junit.Test;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

import static junit.framework.TestCase.assertEquals;

public class UsuarioTests {
    @Rule
    public PactProviderRule rule = new PactProviderRule("usuario", "localhost", 8080, this);

    @Pact(provider = "usuario", consumer = "imposto")
    public PactFragment pegaUsuario(PactDslWithProvider builder) throws Exception {
        DslPart usuario = new PactDslJsonBody().uuid("id").stringType("nome").stringType("salario");

        PactFragment fragment = builder.given("existe usuários cadastrados no serviço usuario")
                .uponReceiving("quando eu busco por um usuário")
                .path("/usuario/93e29f1d-3b21-4d00-8f87-a66a5df30786")
                .method("GET")
                .willRespondWith()
                .body(usuario)
                .toFragment();

        return fragment;
    }

    @Test
    @PactVerification("usuario")
    public void devePegarUmUsuario() throws Exception {
        URI uri = UriComponentsBuilder.fromUriString("http://localhost:8080/")
                                .path("usuario/93e29f1d-3b21-4d00-8f87-a66a5df30786")
                                .build().toUri();
                Response response = RestAssured.get(uri);
                assertEquals(200, response.getStatusCode());
    }
}