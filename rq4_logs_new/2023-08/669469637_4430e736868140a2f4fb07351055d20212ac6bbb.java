package pl.maks.carrental;

import org.apache.commons.codec.binary.Base64;
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
import pl.maks.carrental.controller.productDTO.CarDTO;

import java.nio.charset.Charset;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@SpringBootTest(classes = CarRentalApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CarLifecycleIT {

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
    void verifyCarLifecycle() {
        // given
        TestRestTemplate restTemplate = new TestRestTemplate();
        CarDTO anotherCar = anotherCar();
        CarDTO updateCar = updateCar();
        String updatedCarBrand = updateCar.getBrand();
        String updatedCarModel = updateCar.getModel();
        BigDecimal updatedCarPricePerDay = updateCar.getPricePerDay();

        // prepare request
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<CarDTO> request = new HttpEntity<>(anotherCar, headers);
        HttpEntity<CarDTO> requestUpdate = new HttpEntity<>(updateCar, headers);

        // security
        String auth = "admin" + ":" + "adminPassword";
        byte[] encodedAuth = Base64.encodeBase64(
                auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        headers.add("Authorization", authHeader);

        // car creation
        ResponseEntity<Integer> createResponse = restTemplate.postForEntity("http://localhost:" + port + "/cars", request, Integer.class);
        Integer createdCarId = createResponse.getBody();

        // when
        ResponseEntity<CarDTO> actualCarForEntity = restTemplate.exchange("http://localhost:" + port + "/cars/" + createdCarId, HttpMethod.GET, request, CarDTO.class);

        // update car
        updateCar.setId(createdCarId);
        ResponseEntity<CarDTO> updatedCar = restTemplate.exchange("http://localhost:" + port + "/cars/" + createdCarId, HttpMethod.PUT, requestUpdate, CarDTO.class);
        CarDTO updatedCarBody = updatedCar.getBody();

        // delete car
        restTemplate.exchange("http://localhost:" + port + "/cars/" + createdCarId, HttpMethod.DELETE, request, CarDTO.class);

        // then
        HttpClientErrorException.NotFound actualException = assertThrows(HttpClientErrorException.NotFound.class,
                () -> restTemplate.exchange("http://localhost:" + port + "/cars/" + createdCarId, HttpMethod.GET, request, CarDTO.class));

        String expectedMessage = String.format("404 : \"Car not found: %d\"", createdCarId);

        //create car then
        CarDTO actualCar = actualCarForEntity.getBody();
        assertThat(actualCar).isNotNull();
        assertThat(actualCar.getBrand()).isEqualTo(anotherCar.getBrand());
        assertThat(actualCar.getModel()).isEqualTo(anotherCar.getModel());
        assertThat(actualCar.getPricePerDay()).isEqualTo(anotherCar.getPricePerDay());

        //update car then
        assert updatedCarBody != null;
        assertThat(updatedCarBody.getBrand()).isEqualTo(updatedCarBrand);
        assertThat(updatedCarBody.getModel()).isEqualTo(updatedCarModel);
        assertThat(updatedCarBody.getPricePerDay()).isEqualTo(updatedCarPricePerDay);

        //delete car then
        assertThat(actualException.getMessage()).isEqualTo(expectedMessage);
    }

    private CarDTO anotherCar() {
        CarDTO car = new CarDTO();
        car.setBrand("Toyota");
        car.setModel("Camry");
        car.setCarClass("Business");
        car.setFuel("Gasoline");
        car.setPricePerDay(BigDecimal.valueOf(150.0));
        return car;
    }

    private CarDTO updateCar() {
        CarDTO car = new CarDTO();
        car.setBrand("Lexus");
        car.setModel("RX 450h");
        car.setCarClass("Premium ");
        car.setFuel("Hybrid");
        car.setPricePerDay(BigDecimal.valueOf(160.0));
        return car;
    }
}
