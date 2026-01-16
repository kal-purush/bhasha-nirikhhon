package com.bharat.freelance;

import io.restassured.RestAssured;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class AddressApiTest {

    @Test
    public void testGetAllAddresses() {
        RestAssured.baseURI = "http://localhost:9092/api/addresses/v1/get";

        given()
                .when().get()
                .then()
                .statusCode(200)
                .body("size()", greaterThan(0));
    }

    @Test
    public void testAddAddresses() {
        RestAssured.baseURI = "http://localhost:9092/api/addresses/v1/add";

        String newAddressJson = """
        {
            "firstLine": "123 Test Street",
            "secondLine": "Apt 4B",
            "thirdLine": "Test City",
            "fourthLine": "Test State",
            "fifthLine": "Test Country",
            "postCode": "54321",
            "dateMovedIn": "2023-05-01",
            "preferNotToSay": false
        }
        """;

        given()
                .contentType("application/json")
                .body(newAddressJson)
                .when()
                .post()
                .then()
                .statusCode(201);
    }

    @Test
    public void testUpdateAddresses() {
        RestAssured.baseURI = "http://localhost:9092/api/addresses/v1/update/5";

        String newAddressJson = """
        {
            "firstLine": "123 Test Street",
            "secondLine": "Apt 4B",
            "thirdLine": "Test City",
            "fourthLine": "Test State",
            "fifthLine": "Test Country",
            "postCode": "54321",
            "dateMovedIn": "2023-10-01",
            "preferNotToSay": true
        }
        """;

        given()
                .contentType("application/json")
                .body(newAddressJson)
                .when()
                .put()
                .then()
                .statusCode(200)
                .body("dateMovedIn",equalTo("2023-10-01"))
                .body("preferNotToSay",equalTo(true))
                .body("fifthLine",equalTo("Test Country"));
    }

    @Test
    public void testDeleteAddresses() {
        RestAssured.baseURI = "http://localhost:9092/api/addresses/v1/delete/9";

        given()
                .when().delete()
                .then()
                .statusCode(200);
    }
}