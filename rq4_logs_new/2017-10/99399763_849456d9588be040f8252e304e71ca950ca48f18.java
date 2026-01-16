package Controllers;

import android.app.Activity;
import android.support.v7.app.AlertDialog;
import android.util.Log;
import android.widget.ArrayAdapter;
import android.widget.Toast;

import com.android.volley.AuthFailureError;
import com.android.volley.Request;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.VolleyLog;
import com.android.volley.toolbox.JsonArrayRequest;
import com.android.volley.toolbox.JsonObjectRequest;
import com.smartapp.web.smart.MainActivity;
import com.smartapp.web.smart.categoryList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import Model.Category;
import  conexion.AppController;
import utils.Const;

/**
 * Created by kanat on 16/9/2017.
 * CategoryView's controller
 */

public class CategoryController {
    /**
     * Attributes**/
    private ArrayList<Category> categories;
    private String TAG = "CategoriesController";
    private String tag_json_arry = "jarray_req";
    public static Category currentCategory;

    private categoryList categoryActivity;


    //private ArrayList<CategoryView> categories;

     /**
      * **/

    public CategoryController(categoryList pCategoryActivity) throws UnknownHostException, ExecutionException, InterruptedException {
        this.categories  = new ArrayList<Category>();
        this.categoryActivity = pCategoryActivity;
        getAllCategories();
    }


    private void getAllCategories() {
        this.categoryActivity.showProgressBar();
        JsonArrayRequest req = new JsonArrayRequest(Const.URL_CATEGORIES,
                new Response.Listener<JSONArray>() {
                    @Override
                    public void onResponse(JSONArray response) {
                        Log.d(TAG, response.toString());
                        try {
                            getArrayCategories(response.toString());
                            createCategoryListView();
                            setListViewActivity();
                        } catch (JSONException e) {
                            showAlertMessage("Error al convertir string en JSON");
                        }
                        hideProgressDialog();
                    }
                }, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError error) {
                VolleyLog.d(TAG, "Error: " + error.getMessage());
                Toast.makeText(MainActivity.activity,"Error: " + error.toString(),Toast.LENGTH_LONG).show();
                //hideProgressDialog();
            }
        });

        // Adding request to request queue
        if (req == null){
            Toast.makeText(MainActivity.activity,req.toString(),Toast.LENGTH_LONG).show();
        }
        AppController.getInstance().addToRequestQueue(req,
                tag_json_arry);

        // Cancelling request
        // ApplicationController.getInstance().getRequestQueue().cancelAll(tag_json_arry);
    }

    private void hideProgressDialog() {
        this.categoryActivity.hiddenProgressBar();
    }

    private void getArrayCategories(String pString) throws JSONException {
        JSONArray arrayJson = new JSONArray(pString);
        arrayJson = arrayJson.getJSONArray(0);
        //Declare temporal variables
        int idCategory = 0;
        String nameCategory = "";
        Category tempCategory;

        for (int i = 0; i < arrayJson.length(); i++) {
            JSONObject jsonObj = arrayJson.getJSONObject(i);
            idCategory = jsonObj.getInt("idcategory");
            nameCategory = jsonObj.getString("name");
            tempCategory = new Category(idCategory, nameCategory);
            this.categories.add(tempCategory);
        }
    }
    public  void showAlertMessage(String pMessage){
        AlertDialog alertDialog = new AlertDialog.Builder(this.categoryActivity).create();
        alertDialog.setTitle("Alert");
        alertDialog.setMessage(pMessage);
        alertDialog.show();
    }

    private void createCategoryListView() {
        for ( Category category : this.categories) {
            this.categoryActivity.addCategoryViewData(category.getNameCategory());
        }
    }
    private void setListViewActivity(){
        this.categoryActivity.setListAdapter();
        this.categoryActivity.setListClickListener();
    }


    public void setCurrentCategory(String category) {
        for (Category cat: categories) {
            if (cat.getNameCategory() == category){
                this.currentCategory = cat;
                break;
            }
        }
    }
}