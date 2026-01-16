package org.matheuscordeiro.controller;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import org.junit.jupiter.api.Test;
import org.matheuscordeiro.entity.Company;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;


@QuarkusTest
class CompanyControllerTest {
    @Test
    void shouldSaveCompany() throws Exception {
        final var company = new Company();
        company.setName("Test Company");
        company.setAddress("Street ABC");
        company.setPhone("222333555");
        company.setRegistry("222588888");
        RestAssured
                .given()
                .contentType("application/json")
                .body(company)
                .when()
                .post("/company")
                .then()
                .statusCode(200)
                .body("name", equalTo("Test Company"))
                .body("address", equalTo("Street ABC"))
                .body("phone", equalTo("222333555"))
                .body("registry", equalTo("222588888"));

    }

    @Test
    void shouldFindAllCompanies() throws Exception {
        RestAssured
                .when()
                .get("/company")
                .then()
                .statusCode(200)
                .body("size()", greaterThanOrEqualTo(0));
    }
}