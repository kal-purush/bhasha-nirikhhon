package liveproject;

import au.com.dius.pact.consumer.dsl.DslPart;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslJsonRootValue;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.given;

@ExtendWith(PactConsumerTestExt.class)
public class ConsumerTest {
    //headers
    Map<String,String> headers = new HashMap<>();
    //Resource path
    String resourcePath="/api/users";

    //create the contract
    @Pact(consumer="UserConsumer",provider="UserProvider")
    public RequestResponsePact consumerTest(PactDslWithProvider builder){
    headers.put("Content-Type","application/json");

    //create the request and response body, here req resp are same so 1 variable
        DslPart  requestResponseBody = new PactDslJsonBody()
                .numberType("id",144)
                .stringType("firstName","Sheetal")
                .stringType("lastName","Karande")
                .stringType("email","abc@gmail.com");

    //create the contract
        return builder.given("A request to create a user")
                .uponReceiving("A request to create a user")
                .method("POST")
                .path(resourcePath)
                .headers(headers)
                .body(requestResponseBody)      //request body
                .willRespondWith()
                .status(201)
                .body(requestResponseBody)      //response body
                /*
                .uponReceiving("A request to get a user")
                .method("GET")
                .path(resourcePath)
                .headers(headers)
                .body(requestResponseBody)
                .willRespondWith()
                .status(201)
                .body(requestResponseBody)
                */
                .toPact();

    }
    @Test
    @PactTestFor(providerName="UserProvider", port="8282")
    public void consumerTest(){
        //set the base URI
        String baseURI="http://localhost:8282"+resourcePath;
        Map<String,Object> reqBody= new HashMap<>();
        reqBody.put("id",144);
        reqBody.put("firstName","Sheetal");
        reqBody.put("lastName","Karande");
        reqBody.put("email","abc@gmail.com");

    //Generate response and assert
                given().headers(headers).body(reqBody).log().all()
                .when().post(baseURI)
                .then().statusCode(201).log().all();


    }


}