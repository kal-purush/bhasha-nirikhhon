package org.tms.api.adapters;

import io.restassured.response.Response;

import static io.restassured.RestAssured.given;
import static org.tms.utils.constants.Headers.*;
import static org.tms.utils.constants.Urls.BASE_URL;

public class BaseAdapter {
    public static final String TOKEN_VALUE = "72b2653608fade7ce5de34b2db36aa9eafd86826f56f6517ffd90c084b383528";
    public Response get(String uri) {
        return
                given()
                        .log().all()
                        .header(TOKEN_NAME, TOKEN_VALUE)
                .when()
                        .get(BASE_URL + uri)
                .then()
                        .log().all()
                        .extract().response();
    }

    public Response post(String uri, String body) {
        return
                given()
                        .log().all()
                        .header(TOKEN_NAME, TOKEN_VALUE)
                        .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
                        .body(body)
                .when()
                        .post(BASE_URL + uri)
                .then()
                        .log().all()
                        .extract().response();
    }
}