package api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.CommonIngredient;
import entity.Ingredient;
import okhttp3.*;

import java.util.ArrayList;
import java.util.List;

public class API_request {
    private static final String APP_ID = "&app_id=6b5710fe";
    private static final String APP_KEY = "&app_key=8d04006d4c25a92a53ba4bc90e36bc03";
    private String QUERY;
    private String HEALTH;
    private String CUISINE;
    private String MEALTYPE;
    private String URL;

    public API_request(List<Ingredient> ingredients, String health, String cuisine, String mealType) {
        if (ingredients.isEmpty()){
            this.QUERY = "";
        }
        else{
            this.QUERY = QUERY_Generator(ingredients);
        }

        if (health.isEmpty()){
            this.HEALTH = "";
        }
        else{
            this.HEALTH = "&health=" + health;
        }

        if (CUISINE_Checker(cuisine)){
            this.CUISINE = "&cuisine=" + cuisine;
        }
        else{
            this.CUISINE = "";
        }

        if (mealType.isEmpty()){
            this.MEALTYPE = "";
        }
        else{
            this.MEALTYPE = "&mealType=" + mealType;
        }
        this.URL = "https://api.edamam.com/api/recipes/v2?type=public" + APP_ID + APP_KEY +
                QUERY+"&diet=balanced"+ HEALTH + CUISINE+ MEALTYPE;

    }

    public List<List<String>> Searching_Recipe(){
        return Recipe_info_analyzer(Requesting());
    }
    //这个return出来的每个list中第一个项是菜名

    private String Requesting(){
        Request request = new Request.Builder()
                .url(URL)
                .method("GET", null)
                .addHeader("Edamam-Account-User", "MaxwellsCat")
                .build();
        //sending request through api using okhttp3 package
        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            return response.body().string();
        } catch (Exception e) {
             return e.getMessage();
        }
    }

    private List<List<String>> Recipe_info_analyzer(String intake){
        List<List<String>> result = new ArrayList<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(intake);
            JsonNode hits = rootNode.get("hits");
            if (hits != null && hits.isArray()) {
                for (JsonNode hit : hits) {
                    JsonNode recipe = hit.get("recipe");
                    String recipeName = recipe.get("label").asText();
                    List<String> temp = new ArrayList<>();
                    temp.add(recipeName);
                    JsonNode ingredients = recipe.get("ingredientLines");
                    for (JsonNode ingredient : ingredients) {
                        temp.add(ingredient.asText());
                    }
                    result.add(temp);
                }
            }
            else{
                return null;
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String QUERY_Generator(List<Ingredient> ingredients) {
        StringBuilder result = new StringBuilder();
        for (Ingredient ingredient : ingredients) {
            result.append("&q=").append(ingredient.getName());
        }
        return result.toString();
    }

    private boolean CUISINE_Checker(String cuisine){
        List<String> list = List.of("American", "Asian", "British", "Caribbean", "Central Europe", "Chinese",
                "Eastern Europe", "French", "Indian", "Italian", "Japanese", "Kosher", "Mediterranean", "Mexican",
                "Middle Eastern", "Nordic", "South American", "South East Asian");
        return !(!list.contains(cuisine)|cuisine.isEmpty());
    }
}