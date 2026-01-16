package data_access;

import entity.Recipe;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import use_case.recipe_search.RecipeSearchDataAccessInterface;

import java.io.IOException;
import java.util.*;

public class RecipeDataAccessObject implements RecipeSearchDataAccessInterface {

    private static final String API_URL = "https://api.edamam.com/api/recipes/v2";
    private static final String APP_ID = "bb181cd2";
    private static final String APP_KEY = "bf09ce656684790d61f32c328f1e720f";

    private final Runnable customTask;

    // Constructor accepting a Runnable
    public RecipeDataAccessObject(Runnable customTask) {
        this.customTask = customTask;
    }

    // Default constructor for compatibility
    public RecipeDataAccessObject() {
        this.customTask = null;
    }

    @Override
    public List<Recipe> searchRecipe(String q, String cal_min, String cal_max, String carb_min, String carb_max, String protein_min, String protein_max, String fat_min, String fat_max) {
        final OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        String newq = "";
        if (!"".equals(q)) {
            newq = "&q=" + q;
        }
        String calories1 = "";
        if (!"".equals(cal_min)) {
            if (!"".equals(cal_max)) {
                calories1 = "&calories=" + cal_min + "-" + cal_max;
            }
        }
        if (!"".equals(cal_min)) {
            if ("".equals(cal_max)) {
                calories1 = "&calories=" + cal_min + "%2B";
            }
        }
        if ("".equals(cal_min)) {
            if (!"".equals(cal_max)) {
                calories1 = "&calories=" + cal_max;
            }
        }

        String carbs = "";
        if (!"".equals(carb_min)) {
            if (!"".equals(carb_max)) {
                carbs = "&nutrients%5BCHOCDF%5D=" + carb_min + "-" + carb_max;
            }
        }
        if (!"".equals(carb_min)) {
            if ("".equals(carb_max)) {
                carbs = "&nutrients%5BCHOCDF%5D=" + carb_min + "%2B";
            }
        }
        if ("".equals(carb_min)) {
            if (!"".equals(carb_max)) {
                carbs = "&nutrients%5BCHOCDF%5D=" + carb_max;
            }
        }

        String protein = "";
        if (!"".equals(protein_min)) {
            if (!"".equals(protein_max)) {
                protein = "&nutrients%5BPROCNT%5D=" + protein_min + "-" + protein_max;
            }
        }
        if (!"".equals(protein_min)) {
            if ("".equals(protein_max)) {
                protein = "&nutrients%5BPROCNT%5D=" + protein_min + "%2B";
            }
        }
        if ("".equals(protein_min)) {
            if (!"".equals(protein_max)) {
                protein = "&nutrients%5BPROCNT%5D=" + protein_max;
            }
        }

        String fat = "";
        if (!"".equals(fat_min)) {
            if (!"".equals(fat_max)) {
                fat = "&nutrients%5BPROCNT%5D=" + fat_min + "-" + fat_max;
            }
        }
        if (!"".equals(fat_min)) {
            if ("".equals(fat_max)) {
                fat = "&nutrients%5BPROCNT%5D=" + fat_min + "%2B";
            }
        }
        if ("".equals(fat_min)) {
            if (!"".equals(fat_max)) {
                fat = "&nutrients%5BPROCNT%5D=" + fat_max;
            }
        }

        final Request request = new Request.Builder()
                .url(String.format("%s?type=public%s&app_id=%s&app_key=%s%s%s%s%s", API_URL, newq, APP_ID, APP_KEY, calories1, carbs, fat, protein))
                .build();

        try {
            Response response = client.newCall(request).execute();

            // Check response
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code: " + response);
            }

            String responseBody = response.body().string();
            JSONObject root = new JSONObject(responseBody);

            JSONArray hits = root.getJSONArray("hits");
            List<Recipe> recipes = new ArrayList<>();

            for (int i = 0; i < hits.length(); i++) {
                JSONObject recipeObj = hits.getJSONObject(i).getJSONObject("recipe");

                // Extract recipe details
                String name = recipeObj.getString("label");
                int servings = recipeObj.optInt("yield", 1);
                int calories = (int) Math.round(recipeObj.getDouble("calories") / servings);
                String url = recipeObj.getString("url");
                String image = recipeObj.getJSONObject("images").getJSONObject("THUMBNAIL").getString("url");

                // Extract nutrients
                Map<String, Integer> nutrients = new HashMap<>();
                JSONObject totalNutrients = recipeObj.getJSONObject("totalNutrients");
                for (String key : totalNutrients.keySet()) {
                    JSONObject nutrient = totalNutrients.getJSONObject(key);
                    nutrients.put(nutrient.getString("label"), (int) nutrient.getDouble("quantity"));
                }

                // Extract tags
                Set<String> tags = new HashSet<>();
                JSONArray healthLabels = recipeObj.getJSONArray("healthLabels");
                for (int j = 0; j < healthLabels.length(); j++) {
                    tags.add(healthLabels.getString(j));
                }

                // Build the Recipe object
                Recipe recipe = Recipe.builder()
                        .name(name)
                        .servings(servings)
                        .calories(calories)
                        .nutrients(nutrients)
                        .tags(tags)
                        .url(url)
                        .image(image)
                        .build();

                recipes.add(recipe);
            }

            return recipes;
        }
        catch (IOException | JSONException event) {
            throw new RuntimeException(event);
        }
    }
}