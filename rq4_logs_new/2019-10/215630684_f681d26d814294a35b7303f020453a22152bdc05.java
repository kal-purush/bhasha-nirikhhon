package net.jamie.client;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import net.jamie.datamodels.MapResponse;
import net.jamie.datamodels.PriceConversionResponse;

import java.util.HashMap;
import java.util.Map;

import static net.serenitybdd.rest.SerenityRest.given;

public class CryptoClient {

    //TODO: Ideally i'd like to put common methods+config in a base client + interface. Have different client impl for each service. E.g
    // one for tools, one for cryptocurrency

    private static String BASE_URL="https://pro-api.coinmarketcap.com/v1";
    Map<String,Object> headerMap = new HashMap<String,Object>();


    public CryptoClient(){
        headerMap.put("X-CMC_PRO_API_KEY", "9321844f-1b6d-4ff8-9ed3-8948029b3616");
    }

    private RequestSpecification given(){
        return RestAssured.given()
                .baseUri(BASE_URL)
                .headers(headerMap)
                .contentType(ContentType.JSON);
    }

    //TODO: These methods can be overloaded to supply different query+path params
    public MapResponse getMap(String crypto) {
       Response result = given()
            .basePath("/cryptocurrency/map")
            .queryParam("symbol",crypto)
            .get();
        result.then().assertThat().statusCode(200);
        return result.as(MapResponse.class);
    }

    public PriceConversionResponse getPrice(int id, String convert){
        Response result = given()
                .basePath("/tools/price-conversion")
                .queryParam("id", id)
                .queryParam("convert", convert)
                .queryParam("amount",10)
                .get();
        result.then().assertThat().statusCode(200);
        return result.as(PriceConversionResponse.class);
    }

}