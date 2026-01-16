package stepdefinitions;

import org.testng.Assert;

import com.apiautomation.model.ResponseItem;
import com.apiautomation.model.addObjectResponse;
import com.apiautomation.model.getListOfAllObjectResponse;
import com.apiautomation.model.getListSingleObjectResponse;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.restassured.RestAssured;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import resources.DataRequest;

import java.util.List;

public class StepDefinition_Implementation {

        ResponseItem responseItem;
        String json;
        Response response;
        getListOfAllObjectResponse listOfAllObjectResponse;
        addObjectResponse objectResponse;
        getListSingleObjectResponse listSingleObjectResponse;

        @Given("A list of item are available")
        public void getListOfAllObjects() {
                // String json = "[\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"1\",\r\n" + //
                // " \"name\": \"Google Pixel 6 Pro\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"color\": \"Cloudy White\",\r\n" + //
                // " \"capacity\": \"128 GB\"\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"2\",\r\n" + //
                // " \"name\": \"Apple iPhone 12 Mini, 256GB, Blue\",\r\n" + //
                // " \"data\": null\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"3\",\r\n" + //
                // " \"name\": \"Apple iPhone 12 Pro Max\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"color\": \"Cloudy White\",\r\n" + //
                // " \"capacity GB\": 512\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"4\",\r\n" + //
                // " \"name\": \"Apple iPhone 11, 64GB\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"price\": 389.99,\r\n" + //
                // " \"color\": \"Purple\"\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"5\",\r\n" + //
                // " \"name\": \"Samsung Galaxy Z Fold2\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"price\": 689.99,\r\n" + //
                // " \"color\": \"Brown\"\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"6\",\r\n" + //
                // " \"name\": \"Apple AirPods\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"generation\": \"3rd\",\r\n" + //
                // " \"price\": 120\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"7\",\r\n" + //
                // " \"name\": \"Apple MacBook Pro 16\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"year\": 2019,\r\n" + //
                // " \"price\": 1849.99,\r\n" + //
                // " \"CPU model\": \"Intel Core i9\",\r\n" + //
                // " \"Hard disk size\": \"1 TB\"\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"8\",\r\n" + //
                // " \"name\": \"Apple Watch Series 8\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"Strap Colour\": \"Elderberry\",\r\n" + //
                // " \"Case Size\": \"41mm\"\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"9\",\r\n" + //
                // " \"name\": \"Beats Studio3 Wireless\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"Color\": \"Red\",\r\n" + //
                // " \"Description\": \"High-performance wireless noise cancelling
                // headphones\"\r\n"
                // + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"10\",\r\n" + //
                // " \"name\": \"Apple iPad Mini 5th Gen\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"Capacity\": \"64 GB\",\r\n" + //
                // " \"Screen size\": 7.9\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"11\",\r\n" + //
                // " \"name\": \"Apple iPad Mini 5th Gen\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"Capacity\": \"254 GB\",\r\n" + //
                // " \"Screen size\": 7.9\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"12\",\r\n" + //
                // " \"name\": \"Apple iPad Air\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"Generation\": \"4th\",\r\n" + //
                // " \"Price\": \"419.99\",\r\n" + //
                // " \"Capacity\": \"64 GB\"\r\n" + //
                // " }\r\n" + //
                // " },\r\n" + //
                // " {\r\n" + //
                // " \"id\": \"13\",\r\n" + //
                // " \"name\": \"Apple iPad Air\",\r\n" + //
                // " \"data\": {\r\n" + //
                // " \"Generation\": \"4th\",\r\n" + //
                // " \"Price\": \"519.99\",\r\n" + //
                // " \"Capacity\": \"256 GB\"\r\n" + //
                // " }\r\n" + //
                // " }\r\n" + //
                // "]";

                RestAssured.baseURI = "https://api.restful-api.dev";
                RequestSpecification requestSpecification = RestAssured.given();
                Response response = requestSpecification.log().all().get("objects");
                System.out.println("getListOfAllObjects" + response.asPrettyString());

                JsonPath jsonPath = response.jsonPath();
                List<getListOfAllObjectResponse> responseObjects = jsonPath.getList("",
                                getListOfAllObjectResponse.class);

                // Validate the first object
                listOfAllObjectResponse = responseObjects.get(0);
                Assert.assertEquals(listOfAllObjectResponse.id, "1");
                Assert.assertEquals(listOfAllObjectResponse.name, "Google Pixel 6 Pro");
                Assert.assertEquals(listOfAllObjectResponse.dataItem.color, "Cloudy White");
                Assert.assertEquals(listOfAllObjectResponse.dataItem.capacity, "128 GB");

                // Validate the second object with 'data' being null
                listOfAllObjectResponse = responseObjects.get(1);
                Assert.assertEquals(listOfAllObjectResponse.id, "2");
                Assert.assertEquals(listOfAllObjectResponse.name, "Apple iPhone 12 Mini, 256GB, Blue");
                Assert.assertNull(listOfAllObjectResponse.dataItem, "null");
        }

        /* untuk non-outline */
        // @When("I add item to list")
        // public void createObject() {
        // // String json = "{\n" + //
        // // " \"name\": \"Apple MacBook Pro 16\",\n" + //
        // // " \"data\": {\n" + //
        // // " \"year\": 2019,\n" + //
        // // " \"price\": 20000,\n" + //
        // // " \"CPU model\": \"Intel Core i9\",\n" + //
        // // " \"Hard disk size\": \"1 TB\"\n" + //
        // // " }\n" + //
        // // "}";

        /* untuk scenario outline */
        @When("I add item to list {string}")
        public void createObject(String payload) {
                DataRequest dataRequest = new DataRequest();

                String json = dataRequest.addItemCollection().get(payload);
                RestAssured.baseURI = "https://api.restful-api.dev";
                RequestSpecification requestSpecification = RestAssured
                                .given();

                Response response = requestSpecification
                                .log()
                                .all()
                                .pathParam("path", "objects")
                                .body(json)
                                .contentType("application/json")
                                .when()
                                .post("{path}");
                System.out.println("Add Object" + response.asPrettyString());

                JsonPath jsonPath = response.jsonPath();
                objectResponse = jsonPath.getObject("", addObjectResponse.class);

                Assert.assertEquals(objectResponse.name, "Apple MacBook Pro 16");
                Assert.assertNotNull(objectResponse.createdAt);
                Assert.assertNotNull(objectResponse.id);
                Assert.assertEquals(objectResponse.dataItem.year, 2019);
                Assert.assertEquals(objectResponse.dataItem.price, 20000);
                Assert.assertEquals(objectResponse.dataItem.cpuModel, "Intel Core i9");
                Assert.assertEquals(objectResponse.dataItem.hardDiskSize, "1 TB");
        }

        @Then("The item is available")
        public void getListSingleObject() {
                String json = "{\r\n" + //
                                "    \"id\": \"7\",\r\n" + //
                                "    \"name\": \"Apple MacBook Pro 16\",\r\n" + //
                                "    \"data\": {\r\n" + //
                                "        \"year\": 2019,\r\n" + //
                                "        \"price\": 1849.99,\r\n" + //
                                "        \"CPU model\": \"Intel Core i9\",\r\n" + //
                                "        \"Hard disk size\": \"1 TB\"\r\n" + //
                                "    }\r\n" + //
                                "}";

                RestAssured.baseURI = "https://api.restful-api.dev";
                RequestSpecification requestSpecification = RestAssured.given();
                Response response = requestSpecification.log().all()
                                .pathParam("path", "objects")
                                .pathParam("idObject", 7)
                                .when()
                                .get("{path}/{idObject}");
                System.out.println("get Single Object" + response.asPrettyString());

                JsonPath jsonPath = response.jsonPath();
                listSingleObjectResponse = jsonPath.getObject("", getListSingleObjectResponse.class);

                Assert.assertEquals(listSingleObjectResponse.id, "7");
                Assert.assertEquals(listSingleObjectResponse.name, "Apple MacBook Pro 16");
                Assert.assertEquals(listSingleObjectResponse.dataItem.year, 2019);
                Assert.assertEquals(listSingleObjectResponse.dataItem.price, 1849.99);
                Assert.assertEquals(listSingleObjectResponse.dataItem.cpuModel, "Intel Core i9");
                Assert.assertEquals(listSingleObjectResponse.dataItem.hardDiskSize, "1 TB");
        }
}