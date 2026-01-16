package arcadia.utils;

import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import java.util.List;
import java.util.Map;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.restassured.RestAssured;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import org.openqa.selenium.WebDriver;

public class RestAssuredUtility {

    private static Response response;
    public String getResponse(String component,WebDriver driver)
    {
        String BASE_URL=ConfigLoader.getInstance().getBaseUrl() + System.getProperty("testInstance")+"/index.lp";
    //    String BASE_URL="https://qa.cadonix.online/mercury1_21/index.lp";
        String url="?app=componentsv2&database=quickstart&form="+component+"&ajax=true&hidden=true&command=ajaxgrid&company=CADONIX&order=asc&offset=0&limit=0";
        RestAssured.baseURI = BASE_URL;
        RequestSpecification request = RestAssured.given();
        String cookie=getCookie(driver);
        request.header("cookie", "ArchonixAuth="+cookie+"");
        response = request.get(url);
        String jsonString = response.asString();
        return jsonString;
    }

    public String getCookie(WebDriver driver)
    {
        String token=driver.manage().getCookieNamed("ArchonixAuth").getValue();
        return token;
    }
}