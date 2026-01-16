package bdd.step_definitions;

import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.restassured.response.Response;
import net.thucydides.core.annotations.Steps;
import utils.ConfigurationLoader;
import utils.Helper;
import utils.RequestMaker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static net.serenitybdd.rest.SerenityRest.then;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

public class UserSearchSteps {

    Integer userId;
    List<Integer> postIDs;
    String userName;
    List<String> emails = new ArrayList<>();
    public static String url;

    @Steps
    RequestMaker requestMaker;

    @Steps
    ConfigurationLoader configurationLoader;

    @Before
    public void setup()
    {
        configurationLoader.readProperties();
        url = configurationLoader.getPropertyValue("baseURL");
    }

    @Given("a user with username {string}")
    public void getUserId(String userName) {
        this.userName = userName;
        requestMaker.makeGetRequest(
                url + configurationLoader.getPropertyValue("users")
        );
        Response response = then().extract().response();
        this.userId = response.jsonPath().getInt("find {user -> user.username==\"" + userName + "\"}.id");
    }

    @When("posts are retrieved for the user")
    public void retrievePostsByUser() {
        requestMaker.makeGetRequest(
                url + configurationLoader.getPropertyValue("users"),
                Arrays.asList(new String[]{
                        "/",
                        userId.toString(),
                        configurationLoader.getPropertyValue("posts")
                })
        );
        Response response = then().extract().response();
        this.postIDs = response.jsonPath().getList("id");
    }

    @When("comments are retrieved for those posts")
    public void retrieveCommentsForPostsByUser() {
        for(Integer postID : postIDs) {
            requestMaker.makeGetRequest(
                    url + configurationLoader.getPropertyValue("posts"),
                    Arrays.asList(new String[]{
                            "/",
                            postID.toString(),
                            configurationLoader.getPropertyValue("comments")
                    })
            );
            Response response = then().extract().response();
            List<String> emailByPost = response.jsonPath().getList("email");
            emailByPost.stream().forEach(this.emails::add);
        }
    }

    @Then("a user with username {string} should be present")
    public void assertUserIsPresent(String userName) {
        Response response = then().extract().response();
        List<String> users = response.jsonPath().getList("username");
        assertThat(users.contains(userName), is(true));
    }

    @Then("a user with username {string} should not be present")
    public void assertUserIsNotPresent(String userName) {
        Response response = then().extract().response();
        List<String> users = response.jsonPath().getList("username");
        assertThat(users.contains(userName), is(false));
    }

    @Then("those comments must have emails in valid format")
    public void validateEmailFormatOnComments() {
        assertThat(this.emails.stream()
                .map(Helper::isValidEmail)
                .collect(Collectors.toList()), everyItem(is(true)));
    }
}