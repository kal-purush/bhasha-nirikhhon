package nyc.c4q.androidtest_unit4final;

import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by amirahoxendine on 1/10/18.
 */
// Just messing around with reorganizing how the data is retrieved so everything is not in the main activity.

public class Rainbow {
    private List<String> rainbow;
    private HashMap<String, String> doubleRainbow;

    public Rainbow(String string) {
        parseJson(string);
    }

    public List<String> getRainbow() {
        return rainbow;
    }

    public HashMap<String, String> getDoubleRainbow() {
        return doubleRainbow;
    }

    private JSONObject setJSon(String jSonString) {
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(jSonString);
        } catch (JSONException e) {

        }
        return jsonObject;
    }

    private JSONArray getJsonArray(JSONObject jsonObject) {
        JSONArray jsonArray = jsonObject.names();
        return jsonArray;
    }

    public void parseJson(String json) {
        //get json object
        JSONObject jsonObject = setJSon(json);
        //get keys from jsonobject, add to arraylist.
        JSONArray names = getJsonArray(jsonObject);
        rainbow = new ArrayList<>();
        doubleRainbow = new HashMap<>();
        for (int i = 0; i < names.length(); i++) {
            try {
                rainbow.add(names.getString(i));
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < rainbow.size(); i++) {
            String colorKey = rainbow.get(i);
            String hexValue;
            try {
                hexValue = jsonObject.getString(colorKey);
            } catch (JSONException e) {
                hexValue = "n/a";
                e.printStackTrace();
            }
            doubleRainbow.put(colorKey, hexValue);
            //test parsing.
            Log.d("hex values", hexValue);
        }
    }

}