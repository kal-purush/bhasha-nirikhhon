package testBase;

import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.testng.annotations.BeforeClass;

public class TestBase {

    public static RequestSpecification httprequest;
    public static Response response;
    public String empID="51838";

    public static Logger logger;

    @BeforeClass
    public static void setup(){
        logger=Logger.getLogger("EmployeeRestAPI");
        PropertyConfigurator.configure("log4j.properties");
        logger.setLevel(Level.DEBUG);
    }

}