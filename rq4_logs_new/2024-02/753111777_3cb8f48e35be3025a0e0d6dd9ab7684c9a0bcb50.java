package starter.koskita;


import io.restassured.http.ContentType;
import io.restassured.response.Response;
import net.serenitybdd.rest.SerenityRest;
import net.thucydides.core.annotations.Step;
import starter.utils.Constants;

import java.io.File;

public class KosKitaRatingAPI {

    public static String RATING_KOS = Constants.BASE_URL+"/kos/{kos_id}/rating";

    @Step("rating kos")
    public Response postRating(String kos_id, File json) {
        String token = "Bearer " + Constants.getAuthToken();
        return SerenityRest.given()
                .pathParam("kos_id", kos_id)
                .contentType(ContentType.JSON)
                .header("Authorization", token)
                .body(json)
                .post(RATING_KOS); // Ganti dengan endpoint yang sesuai
    }
}