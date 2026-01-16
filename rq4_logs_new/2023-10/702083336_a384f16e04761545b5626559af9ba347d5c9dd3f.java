package team01.base_url;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import static team01.utilities.Authentication.generateToken;

public class Medunna {
    public static RequestSpecification spec;
    public static void setup(String username, String password, Boolean rememberMe) {
        spec = new RequestSpecBuilder()
                .setBaseUri("https://medunna.com/")
                .addHeader("Authorization", "Bearer " + generateToken(password,username, rememberMe))
                .setContentType(ContentType.JSON)
                .build();
    }

}