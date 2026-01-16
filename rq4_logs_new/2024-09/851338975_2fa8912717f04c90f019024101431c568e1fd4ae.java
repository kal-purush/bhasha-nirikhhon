package com.reservibe.bdd;

import com.reservibe.infra.model.restaurant.RestaurantModel;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.restassured.response.Response;

public class StepDefinitionRestaurant {

    private Response response;

    private RestaurantModel restaurantModel;

    private final String ENDPOINT_API = "http://localhost:8080/restaurant/search/name";

    @Given("that I access the reservibe restaurant api")
    public void that_i_access_the_reservibe_restaurant_api() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }
    @When("I put {string}, {string}, {string}, {string}, {string}, {string} and {string}")
    public void iPutAnd(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5, String arg6) {
    }

    @Then("the return must be {string} success")
    public void the_return_must_be_success(String string) {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }
    @Then("the restaurant must be presented")
    public void the_restaurant_must_be_presented() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("that there is a saved restaurant")
    public void that_there_is_a_saved_restaurant() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }
    @When("I look for a restaurant by {string}")
    public void i_look_for_a_restaurant_by(String string) {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("that there are some saved restaurants")
    public void that_there_are_some_saved_restaurants() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }
    @When("I look for all restaurant")
    public void i_look_for_all_restaurant() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }
    @Then("all restaurants must be presented")
    public void all_restaurants_must_be_presented() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("I update the restaurant")
    public void i_update_the_restaurant() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("I delete a restaurant")
    public void i_delete_a_restaurant() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    //without tables
    @When("I put {string}, {string}, {string}, {string}, {string}, {string}")
    public void iPut(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5) {
    }

    @Then("the return must be {string} unsuccess")
    public void theReturnMustBeUnsuccess(String arg0) {
    }
}