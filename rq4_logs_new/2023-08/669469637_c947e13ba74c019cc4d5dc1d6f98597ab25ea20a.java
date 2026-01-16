import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.HttpClientErrorException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.maks.carrental.CarRentalApplication;
import pl.maks.carrental.controller.productDTO.ClientDTO;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Testcontainers
@SpringBootTest(classes = CarRentalApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ClientLifecycleIT {

    @LocalServerPort
    private Integer port;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:12");

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getPassword);
        registry.add("spring.datasource.password", postgres::getUsername);
        registry.add("spring.datasource.driver-class-name", postgres::getDriverClassName);
    }

    @Test
    @DisplayName("Check if the clients were created when the program was started")
    void checkClients() {
        TestRestTemplate restTemplate = new TestRestTemplate();
        ResponseEntity<ClientDTO[]> forEntity = restTemplate.getForEntity("http://localhost:" + port + "/clients", ClientDTO[].class);
        ClientDTO[] body = forEntity.getBody();

        assertThat(body).isNotEmpty();
    }
    @Test
    void verifyClientLifecycle() {
        // given
        TestRestTemplate restTemplate = new TestRestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ClientDTO someClient = someClient();
        ClientDTO updateClient = updateClient();

        HttpEntity<ClientDTO> request = new HttpEntity<>(someClient, headers);
        HttpEntity<ClientDTO> requestUpdate = new HttpEntity<>(updateClient, headers);

        // client creation
        ResponseEntity<Integer> createResponse = restTemplate.postForEntity("http://localhost:" + port + "/clients", request, Integer.class);
        Integer createdClientId = createResponse.getBody();

        // when
        ClientDTO actualClient = restTemplate.getForObject("http://localhost:" + port + "/clients/" + createdClientId, ClientDTO.class);

        // update client
        updateClient.setId(createdClientId);
        restTemplate.exchange("http://localhost:" + port + "/clients/" + createdClientId, HttpMethod.PUT, requestUpdate, ClientDTO.class);

        // delete client
        restTemplate.delete("http://localhost:" + port + "/clients/" + createdClientId);

        // then
        HttpClientErrorException.NotFound actualException = assertThrows(HttpClientErrorException.NotFound.class,
                () -> restTemplate.getForObject("http://localhost:" + port + "/clients/" + createdClientId, ClientDTO.class));

        assertThat(actualClient).isNotNull().isEqualTo(someClient);
        assertThat(actualException.getMessage()).isEqualTo("404 Not Found");
    }

    private ClientDTO someClient() {
        ClientDTO client = new ClientDTO();
        client.setFirstName("ClientFirstName");
        client.setLastName("ClientLastName");
        client.setDocumentNumber("rzc89456789");
        client.setAccidents(0);
        return client;
    }

    private ClientDTO updateClient() {
        ClientDTO client = new ClientDTO();
        client.setFirstName("UpdateFirstName");
        client.setLastName("ClientLAstName");
        client.setDocumentNumber("zkq87898789");
        client.setAccidents(1);
        return client;
    }
}