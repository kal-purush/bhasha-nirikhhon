package am.wedo.sunshine;

import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * class FetchWeatherTask for fetching data from webServer
 */
public class FetchWeatherTask extends AsyncTask<String, Void, Void> {

    private Context mContext;

    public FetchWeatherTask( Context context ){
        mContext = context;
    }

    @Override
    protected Void doInBackground(String... params) {

        if(params.length == 0){
            return null;
        }

        String locationQuery = params[0];

        // These two need to be declared outside the try/catch
        // so that they can be closed in the finally block.
        HttpURLConnection urlConnection = null;
        BufferedReader reader = null;

        // Will contain the raw JSON response as a string.
        String forecastJsonStr = null;
        String format = "json";
        String units = "metric";
        String[] results = null;
        int numDays = 14;

        try {
            // Construct the URL for the OpenWeatherMap query
            // Possible parameters are available at OWM's forecast API page, at
            // http://openweathermap.org/API#forecast

//            final String QUERY_PARAM = "id";
            final String QUERY_PARAM = "q";

            Uri.Builder builder = new Uri.Builder();
            builder.scheme("http")
                    .authority("api.openweathermap.org")
                    .appendPath("data")
                    .appendPath("2.5")
                    .appendPath("forecast")
                    .appendPath("daily")
                    .appendQueryParameter(QUERY_PARAM, params[0])
                    .appendQueryParameter("APPID", "15166a61ffa7409e7b2b6786bd575ba0")
                    .appendQueryParameter("units", units)
                    .appendQueryParameter("cnt", String.valueOf(numDays));
            String myUrl = builder.build().toString();

//                URL url = new URL("http://api.openweathermap.org/data/2.5/forecast/daily?id=616051&APPID=15166a61ffa7409e7b2b6786bd575ba0&units=metric&cnt=7");
            URL url = new URL(myUrl);

            // Create the request to OpenWeatherMap, and open the connection
            urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("GET");
            urlConnection.connect();

            // Read the input stream into a String
            InputStream inputStream = urlConnection.getInputStream();
            StringBuffer buffer = new StringBuffer();
            if (inputStream == null) {
                // Nothing to do.
                forecastJsonStr = null;
            }
            reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                // Since it's JSON, adding a newline isn't necessary (it won't affect parsing)
                // But it does make debugging a *lot* easier if you print out the completed
                // buffer for debugging.
                buffer.append(line + "\n");
            }

            if (buffer.length() == 0) {
                // Stream was empty.  No point in parsing.
                forecastJsonStr = null;
            }
            forecastJsonStr = buffer.toString();

//            if(forecastJsonStr != null){
//                WeatherDataParser weatherDataParser = new WeatherDataParser();
//                try {
////                    results = weatherDataParser.getWeatherDataFromJson(forecastJsonStr, numDays, mContext);
//                } catch (JSONException e) {
//                    e.printStackTrace();
//                }
//
//            }


        } catch (IOException e) {
            Log.d("test", "Error ", e);
            // If the code didn't successfully get the weather data, there's no point in attemping
            // to parse it.
            forecastJsonStr = null;
        } finally {
            if (urlConnection != null) {
                urlConnection.disconnect();
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (final IOException e) {
                    Log.e("test", "Error closing stream", e);
                }
            }
        }

        // These are the names of the JSON objects that need to be executed.

        // location information
        final String OWM_CITY = "city";
        final String OWM_CITY_NAME = "name";
        final String OWM_COORD = "coord";

        // location cordinate
        final String OWM_LATITUDE = "lat";
        final String OWM_LONGITUDE = "lng";

        // Weather information. Each day's forecast info is an element of the "list" array.
        final String OWM_LIST = "list";

        final String OWM_DATETIME = "dt";
        final String OWM_PRESSURE = "pressure";
        final String OWM_HUMIDITY = "humidity";
        final String OWM_WINDSPEED = "speed";
        final String OWM_WIND_DIRECTION = "deg";

        // all temperatures are children of the "temp" object
        final String OWM_TEMPERATURE = "temp";
        final String OWM_MAX = "max";
        final String OWM_MIN = "min";

        final String OWM_WEATHER = "weather";
        final String OWM_DESCRIPTION = "main";
        final String OWM_ID = "id";

        try {
            JSONObject forecastJson = new JSONObject( forecastJsonStr );
            JSONArray weatherArray = forecastJson.getJSONArray(OWM_LIST);

            JSONObject cityJson = forecastJson.getJSONObject(OWM_CITY);
            String cityName = cityJson.getString(OWM_CITY_NAME);

            JSONObject cityCoord = forecastJson.getJSONObject(OWM_COORD);
            double cityLatitude = cityJson.getDouble(OWM_LATITUDE);
            double cityLongitude = cityJson.getDouble(OWM_LONGITUDE);
        } catch (JSONException e) {
            e.printStackTrace();
        }


        // this will only happen if there was an error getting or parsing the forecast
        return null;
    }
}