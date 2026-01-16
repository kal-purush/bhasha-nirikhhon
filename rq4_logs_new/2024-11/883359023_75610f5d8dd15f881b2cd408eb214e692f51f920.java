package api;

import entity.Recipe;
import okhttp3.Request;

import java.util.List;

public class recipe_search_api {


    private static final String API_URL = "";
    private static final String APP_ID = "bb181cd2";
    private static final String APP_KEY = "bf09ce656684790d61f32c328f1e720f";

    List<Recipe> searchRecipe(){
        String requestUrl = "https://api.edamam.com/api/recipes/v2?type=public&app_id=<APP_ID>&app_key=<APP_KEY>&q=chicken&diet=balanced&health=alcohol-free&health=dairy-free&cuisineType=American&cuisineType=Chinese&cuisineType=Indian&mealType=Lunch&mealType=Dinner&calories=100-1000&time=0-100"
        Request request = new Request.Builder() .url(requestUrl.toString()) .method("GET", null) .addHeader("Content-Type", "application/json") .build();
        request.
    }
}