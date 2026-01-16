package Tests;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Card_Details_Service {

    @Test
    public void positiveTest() throws UnirestException {
        HttpResponse<JsonNode> jsonResponse = Unirest.get("http://www.mocky.io/v2/5babae1d3100004d006544ba")
                .header("accept", "application/json")
                .asJson();
        assertNotNull(jsonResponse.getBody());
        assertEquals(200, jsonResponse.getStatus());
    }

    @Test
    public void negativeTest() throws UnirestException {
        HttpResponse<JsonNode> jsonResponse = Unirest.get("http://www.mocky.io/v2/5babae7531000037006544bd")
                .header("accept", "application/json")
                .asJson();
        assertNotNull(jsonResponse.getBody());
        assertEquals(404, jsonResponse.getStatus());
    }
}