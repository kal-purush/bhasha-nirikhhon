package api;

import com.sun.net.httpserver.Request;
import entity.CommonIngredient;
import entity.Ingredient;
import okhttp3.Request;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.sun.management.HotSpotDiagnosticMXBean.ThreadDumpFormat.JSON;

public class initiate {
    private static final String APP_ID = "YOUR_APP_ID";
    private static final String APP_KEY = "YOUR_APP_KEY";
    private static final String URL = "https://api.edamam.com/api/recipes/v2?type=public&app_id=<" + APP_ID + ">" +
            "&app_key=<" + APP_KEY + ">&q=chicken&diet=balanced&health=alcohol-free&health=dairy-free&cuisineType=" +
            "American&cuisineType=Chinese&cuisineType=Indian&mealType=Lunch&mealType=Dinner&calories=100-1000&time=0-100";
    private List<CommonIngredient> ingredients = new ArrayList<>();
    // 用于搜索的ingredients

    public static void main(String[] args) {
        Request request = new Request.Builder()
                .url(URL)
                .method("GET", null)
                .addHeader("Content-Type", "application/json")
                .build();
        //sending request through api using okhttp3 package

        try (Response response = client.newCall(request).execute()) {
            return response.body().string();
        } catch (Exception e) {
             e.getMessage();
        }
    }
}