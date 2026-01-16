package imposto.api.Client;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.UUID;

import static java.lang.String.valueOf;

@Component
public class UsuarioClient {
    private static final String URL = "http://localhost:8081/usuario/" ;
    private RestTemplate restTemplate;

    public UsuarioClient() {
        restTemplate = new RestTemplate();
    }

    public Usuario pegaUsuarioPorId(UUID id) {
        return restTemplate.getForEntity(URL + valueOf(id), Usuario.class).getBody();
    }
}