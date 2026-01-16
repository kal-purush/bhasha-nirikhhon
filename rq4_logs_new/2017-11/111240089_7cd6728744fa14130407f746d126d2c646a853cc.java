import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TestInitialAPI {

    @Test
    public void makeSureThatGoogleIsUp() {
        given().when().get("http://www.google.com").then().statusCode(200);
    }

    @Test
    public void test_NumberOfCircuitsFor2017Season_ShouldBe20() {

        given().
                when().
                get("http://ergast.com/api/f1/2017/circuits.json").
                then().
                assertThat().
                body("MRData.CircuitTable.Circuits.circuitId",hasSize(20));
    }

    @Test
    public void test_ResponseHeaderData_ShouldBeCorrect() {
        given().
                when().
                get("http://ergast.com/api/f1/2017/circuits.json").
                then().
                assertThat().
                statusCode(200).
                and().
                contentType(ContentType.JSON).
                and().
                header("Content-Length",equalTo("4551"));

    }
       // @BeforeClass
        public static void setup() {
            String port = System.getProperty("server.port");
            if (port == null) {
                RestAssured.port = 8080;
            }
            else{
                RestAssured.port = Integer.valueOf(port);
            }


            String basePath = System.getProperty("server.base");
            if(basePath==null){
                basePath = "/rest-garage-sample/";
            }
            RestAssured.basePath = basePath;
            String baseHost = System.getProperty("server.host");
            if(baseHost==null){
                baseHost = "http://localhost";
            }
            RestAssured.baseURI = baseHost;

        }


}