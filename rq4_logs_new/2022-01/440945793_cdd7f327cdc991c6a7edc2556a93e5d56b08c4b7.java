package api.adapters;

import com.google.gson.Gson;
import io.restassured.response.Response;
import utils.Property;

import java.io.IOException;

import static io.restassured.RestAssured.given;

public class BaseAdapter {

    public final String token = "";
    public final String contentType = "application/json";
    public final String BASE_URL = "https://api.qase.io/v1";
    public final String SUITE_URL = "/suite/%s";
    public final String SPECIFIC_TEST_SUITE = "/suite/%s/%d";
    public final String CASE_URL = "/case/%s";
    public final String SPECIFIC_TEST_CASE = "/case/%s/%d";

    Gson converter = new Gson();

    Property property;

    {
        try {
            property = new Property();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Response get(String url) {
        return
                given()
                        .header("Token", token)
                        .header("Content-Type", contentType)
                .when()
                        .get(BASE_URL + url)
                .then()
                        .log().all()
                        .extract().response();
    }

    public Response post(String url, String body) {
        return
                given()
                        .header("Token", token)
                        .header("Content-Type", contentType)
                        .body(body)
                .when()
                        .post(BASE_URL + url)
                .then()
                        .log().all()
                        .extract().response();
    }

}