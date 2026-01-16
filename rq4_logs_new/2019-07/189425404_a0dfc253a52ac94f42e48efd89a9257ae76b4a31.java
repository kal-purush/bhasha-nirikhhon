package com.squadro.tandir;

import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.graphics.Color;
import android.graphics.Typeface;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.text.InputType;
import android.util.TypedValue;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;

public class SearchActivity extends AppCompatActivity {

    private String tag=null;

    private RestAPI rest;
    private RetrofitCreate rc;
    private Retrofit retrofit;

    protected static int recipeNumber = -1;

    private Recipe[] recipes;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_search);

        final EditText editTextSearch = (EditText) findViewById(R.id.editText_Search);
        editTextSearch.setHint("Enter Tag or Recipe name");

        Button searchButton = (Button)findViewById(R.id.search_searchButton);

        searchButton.setOnClickListener(
                new View.OnClickListener()
                {
                    public void onClick(View view)
                    {
                        tag = (editTextSearch.getText().toString());
                        search();
                        //finish();

                    }
                });



    }

  //  private void search(String tag){
  //      getRecipeIds();
  //  }


    private void search(){

        rc = new RetrofitCreate();
        retrofit = rc.createRetrofit();

        rest = retrofit.create(RestAPI.class);

        JsonObject obj = new JsonObject();

        obj.addProperty("tag",tag);

        Call<JsonObject> call = rest.search(obj);

        call.enqueue(new Callback<JsonObject>() {
            @Override
            public void onResponse(Call<JsonObject> call, Response<JsonObject> response) {

                Toast.makeText(getBaseContext(),"hll agaaa",Toast.LENGTH_LONG).show();

                Gson gson = new Gson();

                JsonObject resultList = response.body();
                JsonElement element = resultList.get("recipes");
                JsonArray array = element.getAsJsonArray();

                recipes = new Recipe[array.size()];

                for (int i = 0; i < array.size(); i++) {
                    Recipe obj = gson.fromJson(array.get(i).toString(), Recipe.class);
                    recipes[i] = obj;
                }
                makeList();
            }
            public void onFailure(Call<JsonObject> call, Throwable t) {

                Toast.makeText(getBaseContext(), t.getMessage(),Toast.LENGTH_LONG).show();
            }
        });
        }

    public void makeList(){

        final String names[] = new String[recipes.length];
        for(int i=0;i<recipes.length;i++){
            names[i]=recipes[i].getRecipe_name();
        }

        ListView listView = (ListView)findViewById(R.id.search_listview);
        ArrayAdapter<String> dataAdapter=new ArrayAdapter<String>
                (this, android.R.layout.simple_list_item_1, android.R.id.text1, names){

            /////////check this out

            @Override
            public View getView(int position, View convertView, ViewGroup parent){
                // Cast the list view each item as text view



                TextView item = (TextView) super.getView(position,convertView,parent);

                // Set the typeface/font for the current item


                // Set the list view item's text color
                item.setTextColor(Color.parseColor("#000000"));

                // Set the item text style to bold
                item.setTypeface(item.getTypeface(), Typeface.BOLD);


                // Change the item text size
                item.setTextSize(TypedValue.COMPLEX_UNIT_DIP,22);

                ViewGroup.LayoutParams layoutparams = item.getLayoutParams();
                layoutparams.height = 170;

                item.setLayoutParams(layoutparams);


                // return the view
                return item;
            }
        };
        listView.setAdapter(dataAdapter);


        listView.setOnItemClickListener(new AdapterView.OnItemClickListener(){
            @Override
            public void onItemClick(AdapterView<?> parent, View view, final int position,
                                    long id) {


                AlertDialog.Builder desc =
                        new AlertDialog.Builder(SearchActivity.this);

                desc.setPositiveButton("Show Username", new DialogInterface.OnClickListener() {

                            @Override
                            public void onClick(DialogInterface dialog, int which) {
                                recipeNumber = position;
                                Toast.makeText(getBaseContext(),"Username = " + recipes[position].getUser_name(),Toast.LENGTH_LONG).show();
                            }
                        }
                );
                desc.create().show();
            }
        });
    }




}