package starter.koskita;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import net.serenitybdd.rest.SerenityRest;
import net.thucydides.core.annotations.Step;
import starter.utils.Constants;

import java.io.File;

public class KoskitaBookingUserAPI {

    public static String POST_CREATE_BOOKING_USER = Constants.BASE_URL+"/booking";
    @Step("Post Create Booking User")
    public void postCreateBookingUser(File json){
//        SerenityRest.given()
//                .contentType(ContentType.JSON)
//                .body(json);
        String token = "Bearer " + Constants.getAuthToken();
        Response response = SerenityRest.given()
                .contentType(ContentType.JSON)
                .header("Authorization", token)
                .body(json)
                .post(POST_CREATE_BOOKING_USER);
    }
}