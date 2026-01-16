package com.reservibe.bdd;

import com.reservibe.domain.input.restaurant.RestaurantInput;
import com.reservibe.helper.ReservationHelper;
import com.reservibe.helper.RestaurantHelper;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.restassured.response.Response;
import org.springframework.http.MediaType;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;

public class StepDefinitionReservation {

    private Response response;

    private RestaurantHelper helper = new RestaurantHelper();
    private ReservationHelper reservationHelper = new ReservationHelper();

    private RestaurantInput restaurantValid;

    private final String ENDPOINT_API_REGISTER = "http://localhost:8080/restaurant/register";
    private final String ENDPOINT_API_REGISTER_RESERVATION = "http://localhost:8080/reservation/create";
    private final String ENDPOINT_API_FIND_BYNAME = "/restaurant/search/name/{name}";



    @Given("I create a restaurant with {string} and {string} and {string} and {string} and {string}")
    public void iCreateARestaurantWithAndAndAndAnd(String name, String city, String cuisine, String status1, String status2) {
        var restaurantCreated = helper.createRestaurantInputWithStatus(name, city, cuisine, status1, status2);
        restaurantValid = restaurantCreated;
        response = given()
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(restaurantCreated)
                .when()
                .post(ENDPOINT_API_REGISTER);

        }

    @And("I try to reserve the FREE table")
    public void iTryToReserveTheFREETable() {
        var reservationCreated = reservationHelper.createRestaurantInput(UUID.fromString(response.getBody().jsonPath().getString("restaurant.tables[0].id")));
        response = given()
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(reservationCreated)
                .when()
                .post(ENDPOINT_API_REGISTER_RESERVATION);

    }

    @And("I look for the restaurant")
    public void iLookForTheRestaurant() {
        response = when().get(ENDPOINT_API_FIND_BYNAME, restaurantValid.name());

    }

    @Then("the return reservation must be {string}")
    public void theReturnReservationMustBe(String status) {
        response.then().statusCode(Integer.parseInt(status))
                .log().all();
        }
}