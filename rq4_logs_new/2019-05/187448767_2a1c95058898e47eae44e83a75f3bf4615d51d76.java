package commonUtils;

import io.restassured.RestAssured;
import org.testng.annotations.BeforeSuite;

public class Base {

    ReadProperties readProperties = new ReadProperties();

    @BeforeSuite(alwaysRun = true)
    public void setBaseUrl(){
        RestAssured.baseURI = readProperties.getPropertyValue("endpoint");
    }

}