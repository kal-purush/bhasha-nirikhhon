package adapters;

import lombok.extern.log4j.Log4j2;
import utils.PropertyReader;

import static io.restassured.RestAssured.given;

@Log4j2
public class BaseAdapter {
    final String BASE_URL_API = System.getenv().getOrDefault("TESTRAIL_URL_API", PropertyReader.getProperty("testrail.url.api"));
    final String TOKEN = System.getenv().getOrDefault("TESTRAIL_API_TOKEN", PropertyReader.getProperty("testrail.api.token"));

    public String post(String body, String uri) {
        given().
                header("Content-Type", "application/json").
                header("Authorization", TOKEN).
                body(body).
        when().
                post(BASE_URL_API + uri).
        then().
                log().all().
                statusCode(200)
                .extract().body().asString();
        return body;
    }

    public void deleete(String uri) {
        given().
                header("Content-Type", "application/json").
                header("Authorization", TOKEN).
        when().
                post(BASE_URL_API + uri).
        then().
                log().all().
                statusCode(200);
    }

    public String got(String uri) {
       String body =
       given().
                header("Content-Type", "application/json").
                header("Authorization", TOKEN).
       when().
                get(BASE_URL_API + uri).
       then().
                log().all().
                statusCode(200)
                .extract().body().asString();
       return body;
    }
}
