package scenario;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.apiautomation.model.ResponseItem;

import io.restassured.RestAssured;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import static io.restassured.RestAssured.given;

public class RestE2ETest_exmpl {
        ResponseItem responseItem;

        /*
         * Scenario e2e test
         * 1. Hit add products (verify response)
         * 2. Hit get products (verify response)
         * 3. Hit update product (verify response)
         */

        @Test
        public void scenarioE2ETest() {
                String json = "{\n" + //
                                "  \"id\": 1,\n" + //
                                "  \"title\": \"Le Minerale\",\n" + //
                                "  \"description\": \"Minuman segar dan menyehatkan.\",\n"
                                + //
                                "  \"category\": \"food\",\n" + //
                                "  \"price\": 10000,\n" + //
                                "  \"discountPercentage\": 5,\n" + //
                                "  \"rating\": 5,\n" + //
                                "  \"stock\": 15,\n" + //
                                "  \"tags\": [\n" + //
                                "    \"beauty\",\n" + //
                                "    \"mascara\"\n" + //
                                "  ],\n" + //
                                "  \"dimensions\": {\n" + //
                                "    \"width\": 23.17,\n" + //
                                "    \"height\": 14.43,\n" + //
                                "    \"depth\": 28.01\n" + //
                                "  }\n" + //
                                "}";

                // Add Product
                RestAssured.baseURI = "https://dummyjson.com";

                Response response = given()
                                .log()
                                .all()
                                .pathParam("path", "products")
                                .pathParam("method", "add")
                                .body(json)
                                .contentType("application/json")
                                .when()
                                .post("{path}/{method}");

                System.out.println("add product" + response.asPrettyString());

                JsonPath addJsonPath = response.jsonPath();
                responseItem = addJsonPath.getObject("", ResponseItem.class);
                Assert.assertEquals(response.statusCode(), 201);
                Assert.assertEquals(responseItem.title, "Le Minerale");
                Assert.assertEquals(responseItem.price, 10000);
                Assert.assertEquals(responseItem.discountPercentage, 5);
                Assert.assertEquals(responseItem.stock, 15);
                Assert.assertEquals(responseItem.category, "food");

                String idObject = responseItem.id;

                // Get Product

                Response response2 = given()
                                .pathParam("path", "products")
                                .pathParam("idProduct", idObject)
                                .log()
                                .all()
                                .when()
                                .get("{path}/{idProduct}");

                System.out.println("response2" + response2.asPrettyString());
                // Validation POJO

                // Update Product
                RestAssured.baseURI = "https://dummyjson.com";

                Response response3 = given()
                                .log()
                                .all()
                                .pathParam("path", "products")
                                .pathParam("idProduct", idObject)
                                .body(json)
                                .contentType("application/json")
                                .when()
                                .put("{path}/{idProduct}");

                System.out.println("update product" + response.asPrettyString());
                // Validation POJO
        }
}