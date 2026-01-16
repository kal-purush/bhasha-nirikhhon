package com.example.a327finalprojectcalorietracker;

import androidx.appcompat.app.AppCompatActivity;


import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.SearchView;

import com.example.a327finalprojectcalorietracker.ui.ApiResponse;
import com.example.a327finalprojectcalorietracker.ui.labelNutrients;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Query;


public class Add_Meal extends AppCompatActivity {

    //Make Retrofit interface to be able to make API calls
    public interface FoodSearchService {
        @GET("v1/foods/search")
        Call<ApiResponse> searchFoodByName(@Query("query") String query, @Query("api_key") String apiKey);

        @GET("v1/food/{fdcId}")
        Call<labelNutrients> getLabelNutrients(@Path("fdcId") long fdcId, @Query("api_key") String apiKey);
    }


    //Set up the retrofit
    Retrofit retrofit = new Retrofit.Builder()
            .baseUrl("https://api.nal.usda.gov/fdc/") // Replace with the actual base URL
            .addConverterFactory(GsonConverterFactory.create())
            .build();

    FoodSearchService foodSearchService = retrofit.create(FoodSearchService.class);


    SearchView searchView = findViewById(R.id.searchView);

    // Set a listener for the SearchView
  searchView.setOnQueryTextListener(new SearchView.OnQueryTextListener() {
        @Override
        public boolean onQueryTextSubmit(String query) {
            // Called when the user submits the query (e.g., presses Enter)
            // Perform food search using the user's query
            performFoodSearch(query);
            return true;
        }

        @Override
        public boolean onQueryTextChange(String newText) {
            // Called when the query text changes (e.g., user types)
            // You may want to implement auto-suggestions or instant search here
            return true;
        }
    });

// ...

    private void performFoodSearch(String query) {
        // Use the user's query in the Retrofit call
        Call<ApiResponse> searchCall = foodSearchService.searchFoodByName(query, "your_api_key");

        searchCall.enqueue(new Callback<ApiResponse>() {
            @Override
            public void onResponse(Call<ApiResponse> call, Response<ApiResponse> response) {
                if (response.isSuccessful()) {
                    // Handle the API response for food search
                    ApiResponse apiResponse = response.body();
                    // ... handle the response
                } else {
                    // Handle the API response error for food search
                }
            }

            @Override
            public void onFailure(Call<ApiResponse> call, Throwable t) {
                // Handle the failure to make the food search call
            }
        });
    }




    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_meal);

    }

    public void goToMainMenu(View view) {
        Intent intent = new Intent(this, MainActivity.class);
        startActivity(intent);
    }
};