import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.http.Cookie;
import io.restassured.parsing.Parser;
import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static java.lang.String.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;


class ShoppingCart {

    private static Cookie cookie;

    @BeforeAll
    public static void setUp() {
        cookie = doReturnCookie("localhost", "/shoppingcart");
    }

    @Test
    public void checkDoubleAmount() {

        doPostRequest("http://localhost:8090/shoppingcart/addcart",
                "1 imported pills at 10.09");
        Response response = doGetRequest("http://localhost:8090/shoppingcart/report");

        List<Map<String, String>> lineItems = response.jsonPath().getList("lineItems");
        Object var = lineItems.get(0).get("amount");
        if (var instanceof java.lang.Float) {
            String amountS = valueOf(var);
            assertEquals("10.09", amountS);
        } else { assert false; }
    }

    public static Cookie doReturnCookie(String host, String path) {
        String sessionID = given().when().get(String.format("http://%s:%s%s", host, "8090", path)).andReturn().sessionId();
        return new Cookie.Builder("JSESSIONID", sessionID).
                setSecured(false).
                setDomain(host).
                setHttpOnly(false).
                setPath(path).build();
    }

    public static Response doGetRequest(String endpoint) {
        RestAssured.defaultParser = Parser.JSON;
        return
                given().headers("Content-Type", ContentType.JSON, "Accept", ContentType.JSON).
                        when().cookie(cookie).get(endpoint).
                        then().contentType(ContentType.JSON).extract().response();
    }

    public static void doPostRequest(String endpoint, String payload) {
        RestAssured.defaultParser = Parser.JSON;
        given().headers("Content-Type", ContentType.JSON, "Accept", ContentType.TEXT).
                when().cookie(cookie).when().body(payload).post(endpoint);
    }

}