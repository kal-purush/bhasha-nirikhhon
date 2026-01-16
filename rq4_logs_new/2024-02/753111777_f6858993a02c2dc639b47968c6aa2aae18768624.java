package starter.stepdef;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.restassured.module.jsv.JsonSchemaValidator;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import net.serenitybdd.rest.SerenityRest;
import net.thucydides.core.annotations.Steps;
import starter.koskita.KoskitaAPI;
import starter.koskita.KoskitaKosAPI;
import starter.koskita.KoskitaResponses;
import starter.utils.Constants;


import java.io.File;
import static org.hamcrest.Matchers.equalTo;



public class KosStepDef {


    @Steps
    KoskitaAPI koskitaAPI;
    @Steps
    KoskitaKosAPI koskitaKosAPI;

    @Given("Login users with valid {string}")
    public void loginUsersWithValid(String json) {
        File jsonLoginUser = new File(Constants.REQ_BODY + json);
        koskitaAPI.postLoginUser(jsonLoginUser);
    }


    @When("Send request login user")
    public void sendRequestLoginUser() {
        SerenityRest.when().post(KoskitaAPI.LOGIN_USER);

        // Mendapatkan respons dari permintaan login
        Response response = SerenityRest.lastResponse();

        // Menggunakan JsonPath untuk mengambil token dari respons JSON
        JsonPath jsonPath = response.jsonPath();
        String token = jsonPath.getString("data.token");

        // Menyimpan token menggunakan TokenManager
        Constants.setAuthToken(token);
        System.out.println("Token disimpan: " + Constants.getAuthToken());
    }


    @Then("Status code should be {int}")
    public void statusCodeShouldBe(int statusCode) {
        SerenityRest.then().statusCode(statusCode);
    }

        @And("Response body message was {string} and role was {string}")
    public void responseBodyMessageWas(String message, String role) {
        SerenityRest.and()
                .body(KoskitaResponses.MESSAGE, equalTo(message))
                .body(KoskitaResponses.ROLE, equalTo(role));
    }

    @And("Validate login user json schema {string}")
    public void validateLoginUserJsonSchema(String json) {
        File jsonSchema = new File(Constants.JSON_SCHEMA+json);
        SerenityRest.and()
                .assertThat()
                .body(JsonSchemaValidator.matchesJsonSchema(jsonSchema));
    }



// case 1

    @Given("Get kos with valid {string}")
    public void getKosWithValidKos_id(String kos_id) {
        koskitaKosAPI.setGetSingleKos(kos_id);

    }

    @When("Send request get kos")
    public void sendRequestGetKos() {
        SerenityRest.when().get(koskitaKosAPI.GET_SINGLE_KOS);
    }

    @And("Response body message was {string}")
    public void responseBodyMessageWas(String message) {
        SerenityRest.and()
                .body(KoskitaResponses.MESSAGE, equalTo(message));
    }

    @And("Validate get kos_id json schema {string}")
    public void validateGetKos_idJsonSchema(String json) {
        File jsonSchema = new File(Constants.JSON_SCHEMA+json);
        SerenityRest.and()
                .assertThat()
                .body(JsonSchemaValidator.matchesJsonSchema(jsonSchema));
    }

    @Given("Get my kos")
    public void getMyKos() {
        koskitaKosAPI.setGetMyKos();
    }

    @When("Send request get my kos")
    public void sendRequestGetMyKos() {
        String Tokek = "Bearer " + Constants.getAuthToken();
        // Mengirim permintaan dengan menggunakan token yang telah disimpan sebelumnya
        SerenityRest.given()
                .header("Authorization", Tokek)
                .get(koskitaKosAPI.GET_MY_KOS);

    }
}