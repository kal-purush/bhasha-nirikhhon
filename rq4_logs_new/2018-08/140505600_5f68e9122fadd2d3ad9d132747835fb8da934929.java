import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.junit.Test;

import static io.restassured.RestAssured.given;



/**
 * Created by tiffanytillett on 7/31/18.
 */

public class BloggerTester {
    String client_id = "989638276769-p9vrt6a1vn9athl7vhcno54t5b2tm08o.apps.googleusercontent.com";
    String client_secret = "5N4H4OAViTpxWb-MT-MMqlbM";

    String baseURI = "https://www.googleapis.com/blogger/v3/blogs/";
    String apiKey = "AIzaSyAYHQw2hPRA5yAdz7A3I7qUo43qa-aZUCM";
    String blogID = "54850391780151973";


    //GET https://www.googleapis.com/blogger/v3/blogs/54850391780151973?key=AIzaSyAYHQw2hPRA5yAdz7A3I7qUo43qa-aZUCM

    @Test
    public void getBlog()
    {
        RestAssured.baseURI = baseURI + blogID;

        Response response =
                given()
                .param("key", apiKey)
                .when()
                .get()
                .then()
                .assertThat()
                .statusCode(200)
                .extract().response();
        System.out.println(response.asString());
    }
}