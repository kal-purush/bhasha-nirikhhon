import io.restassured.RestAssured;
import io.restassured.authentication.PreemptiveBasicAuthScheme;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.*;

public class CrudCountryTest {
    @BeforeAll
    public static void setUpAuth() {
        PreemptiveBasicAuthScheme authScheme = new PreemptiveBasicAuthScheme();
        authScheme.setUserName("admin");
        authScheme.setPassword("admin");
        RestAssured.authentication = authScheme;
    }

    @BeforeAll
    public static void setUpErrorAuth() {
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Test
    public void getWithoutParams() {
        when().get("/api/countries")
                .then()
                .statusCode(200)
                .body("size()", notNullValue(),
                        "[0].locations", nullValue(),
                        "[0].id", notNullValue());
    }

    @Test
    public void getWithId() {
        when().get("/api/countries/1")
                .then()
                .statusCode(200)
                .body(
                        "id", is(1),
                        "countryName", is("vo"),
                        "locations", nullValue());
    }

    @Test
    public void postSuccessWithoutId(){
        given()
                .contentType("application/json")
                .body("{\n" +
                        "\"countryName\" : \"jo\"\n"  +
                        "}")
                .when().post("/api/countries")
                .then()
                .statusCode(201)
                .body(
                        "countryName", is("jo"),
                        "id", notNullValue());
    }
    @Test
    public void postErrorWithId(){
        given()
                .contentType("application/json")
                .body("{\n" +
                        "\"countryName\" : \"ho\",\n"  +
                        "\"id\" : \"1005\"\n" +
                        "}")
                .when().post("/api/countries")
                .then()
                .statusCode(400)
                .body(
                        "title", is("A new country cannot already have an ID"),
                        "status", is(400));
    }

    @Test
    public void patchSuccess(){
        given()
                .contentType("application/json")
                .body("{\n" +
                        "\"countryName\" : \"yo\",\n"  +
                        "\"id\" : \"5\"\n" +
                        "}")
                .when().patch("/api/countries/5")
                .then()
                .statusCode(200)
                .body(
                        "id", is(5),
                        "countryName", is("yo"),
                        "locations", nullValue()
                );
    }

    @Test
    public void patchError(){
        given()
                .contentType("application/json")
                .body("{\n" +
                        "\"countryName\" : \"ho\",\n"  +
                        "\"id\" : \"9\"\n" +
                        "}")
                .when().patch("/api/countries")
                .then()
                .statusCode(405)
                .body(
                        "title", is("Method Not Allowed"),
                        "detail", is("Request method 'PATCH' not supported")
                );
    }

    @Test
    public void putSuccess(){
        given()
                .contentType("application/json")
                .body("{\n" +
                        "\"countryName\" : \"do\",\n"  +
                        "\"id\" : \"10\"\n" +
                        "}")
                .when().put("/api/countries/10")
                .then()
                .statusCode(200)
                .body(
                        "id", is(10),
                        "countryName", is("do"),
                        "locations", nullValue()
                );
    }

}