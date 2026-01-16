package edu.auburn.rfid.rfind;

import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.JsonObjectRequest;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Created by Kyle on 4/3/2015.
 */
public class QueryManager {

    private ArrayList<RfidItem> featuredItems = new ArrayList<RfidItem>();

    public QueryManager()
    {

    }

    public ArrayList<RfidItem> getFeaturedItems()
    {
        featuredItems.clear();
        JsonObjectRequest request = new JsonObjectRequest("http://aurfid.herokuapp.com/upc_descriptions.json", null,
                new Response.Listener<JSONObject>() {

                    @Override
                    public void onResponse(JSONObject response) {
                        try
                        {
                            JSONArray upcDescriptions = response.getJSONArray("upc_descriptions");
                            for(int i = 0; i < upcDescriptions.length(); i++)
                            {
                                RfidItem rfidItem = new RfidItem(upcDescriptions.getJSONObject(i));
                                featuredItems.add(rfidItem);
                            }

                        }
                        catch (JSONException e)
                        {
                            //do something graceful
                        }

                    }
                },

                new Response.ErrorListener() {

                    @Override
                    public void onErrorResponse(VolleyError error) {

                    }
                }
        );
        VolleyApplication.getInstance().getRequestQueue().add(request);
        return featuredItems;
    }
}