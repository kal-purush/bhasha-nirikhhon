package ru.hogwarts.school.controller;

import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import ru.hogwarts.school.model.Faculty;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.PUT;

@Transactional
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FacultyControllerTestRestTemplateTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    private String baseUrl;
    private Faculty testFaculty;

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port + "/faculty";
        testFaculty = new Faculty(null, "Gryffindor", "Red");
    }

    @Test
    void createFaculty_shouldReturnCreatedFaculty() {
        ResponseEntity<Faculty> response = restTemplate.postForEntity(
                baseUrl, testFaculty, Faculty.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .isNotNull()
                .extracting(Faculty::getName)
                .isEqualTo("Gryffindor");
    }

    @Test
    void getFacultyById_shouldReturnFacultyWhenExists() {
        Faculty created = createTestFaculty();

        ResponseEntity<Faculty> response = restTemplate.getForEntity(
                baseUrl + "/{id}", Faculty.class, created.getId());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .isNotNull()
                .extracting(Faculty::getId)
                .isEqualTo(created.getId());
    }

    @Test
    void getFacultyById_shouldReturnNotFoundWhenNotExists() {
        ResponseEntity<Faculty> response = restTemplate.getForEntity(
                baseUrl + "/{id}", Faculty.class, 999L);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void updateFaculty_shouldReturnUpdatedFaculty() {
        Faculty created = createTestFaculty();
        created.setName("Updated Name");

        ResponseEntity<Faculty> response = restTemplate.exchange(
                baseUrl,
                PUT,
                new HttpEntity<>(created),
                Faculty.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .isNotNull()
                .extracting(Faculty::getName)
                .isEqualTo("Updated Name");
    }

    @Test
    void deleteFaculty_shouldReturnOk() {
        Faculty created = createTestFaculty();

        restTemplate.delete(baseUrl + "/{id}", created.getId());

        ResponseEntity<Faculty> response = restTemplate.getForEntity(
                baseUrl + "/{id}", Faculty.class, created.getId());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void getFacultyByColorTest() {
        String color = "orange";
        String response = restTemplate.getForObject(
                baseUrl + "/by-color?color={color}", String.class, color);

        assertThat(response).isNotNull();
    }

    @Test
    void searchFaculties_shouldReturnMatchingFaculties() {
        createTestFaculty(); // Создаём "Gryffindor"

        // Ищем по полному имени (как работает текущий репозиторий)
        ResponseEntity<List<Faculty>> response = restTemplate.exchange(
                baseUrl + "/search?searchTerm=Gryffindor", // Полное совпадение
                GET,
                null,
                new ParameterizedTypeReference<>() {
                });

        assertThat(response.getBody()).isNotEmpty();
    }

    @Test
    void getFacultyStudents_shouldReturnStudentsList() {
        Faculty created = createTestFaculty();

        ResponseEntity<List<?>> response = restTemplate.exchange(
                baseUrl + "/{id}/students",
                GET,
                null,
                new ParameterizedTypeReference<>() {
                },
                created.getId());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
    }

    private Faculty createTestFaculty() {
        return restTemplate.postForObject(baseUrl, testFaculty, Faculty.class);
    }
}