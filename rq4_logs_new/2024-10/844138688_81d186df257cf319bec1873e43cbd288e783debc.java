package aston.MyAPI;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.http.util.EntityUtils;
import org.hibernate.query.sqm.produce.function.StandardFunctionReturnTypeResolvers;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Connector {
    private static final int CONNECTION_TIMEOUT = 1;


    public static void main(String[] args) throws IOException, InterruptedException, JSONException {

        System.out.println("Погода в городе. Введите город: ");
        Scanner scanner = new Scanner(System.in);
        String city = scanner.nextLine();

        final CloseableHttpClient httpclient = HttpClients.createDefault();

        final HttpUriRequest httpGet = new HttpGet("https://api.openweathermap.org/data/2.5/weather?q="+city+"&appid=" +
                "b037c4b33d8edafab1536844f9d3bf0c");
        //ObjectMapper mapper = new ObjectMapper();
        CloseableHttpResponse response1 = httpclient.execute(httpGet);
        final HttpEntity entity1 = response1.getEntity();
        //System.out.println(EntityUtils.toString(entity1));
        String weatherResult = ParseResult(EntityUtils.toString(entity1));
        System.out.println(weatherResult);
        //WeatherAnswer answer = mapper.readValue(response1.getEntity().getContent(), WeatherAnswer.class);
        //System.out.println(answer);


        httpclient.close();


    }
    static private String ParseResult(String json) throws JSONException{

        String parsedResult = "";

        JSONObject jsonObject = new JSONObject(json);

        parsedResult += "Number of object = " + jsonObject.length() + "\n\n";

        //"coord"
        JSONObject JSONObject_coord = jsonObject.getJSONObject("coord");
        Double result_lon = JSONObject_coord.getDouble("lon");
        Double result_lat = JSONObject_coord.getDouble("lat");

        //"sys"
        JSONObject JSONObject_sys = jsonObject.getJSONObject("sys");
        String result_country = JSONObject_sys.getString("country");
        int result_sunrise = JSONObject_sys.getInt("sunrise");
        int result_sunset = JSONObject_sys.getInt("sunset");

        //"weather"
        String result_weather;
        JSONArray JSONArray_weather = jsonObject.getJSONArray("weather");
        if(JSONArray_weather.length() > 0){
            JSONObject JSONObject_weather = JSONArray_weather.getJSONObject(0);
            int result_id = JSONObject_weather.getInt("id");
            String result_main = JSONObject_weather.getString("main");
            String result_description = JSONObject_weather.getString("description");
            String result_icon = JSONObject_weather.getString("icon");

            result_weather = "weather\tid: " + result_id +"\tmain: " + result_main + "\tdescription: " + result_description + "\ticon: " + result_icon;
        }else{
            result_weather = "weather empty!";
        }

        //"base"
        String result_base = jsonObject.getString("base");

        //"main"
        JSONObject JSONObject_main = jsonObject.getJSONObject("main");
        Double result_temp = JSONObject_main.getDouble("temp");
        Double result_pressure = JSONObject_main.getDouble("pressure");
        Double result_humidity = JSONObject_main.getDouble("humidity");
        Double result_temp_min = JSONObject_main.getDouble("temp_min");
        Double result_temp_max = JSONObject_main.getDouble("temp_max");

        //"wind"
        JSONObject JSONObject_wind = jsonObject.getJSONObject("wind");
        Double result_speed = JSONObject_wind.getDouble("speed");
        //Double result_gust = JSONObject_wind.getDouble("gust");
        Double result_deg = JSONObject_wind.getDouble("deg");
        String result_wind = "wind\tspeed: " + result_speed + "\tdeg: " + result_deg;

        //"clouds"
        JSONObject JSONObject_clouds = jsonObject.getJSONObject("clouds");
        int result_all = JSONObject_clouds.getInt("all");

        //"dt"
        int result_dt = jsonObject.getInt("dt");

        //"id"
        int result_id = jsonObject.getInt("id");

        //"name"
        String result_name = jsonObject.getString("name");

        //"cod"
        int result_cod = jsonObject.getInt("cod");

        return
                "Город: " + result_name + "\n" +
                        "Координаты:\tlon: " + result_lon + "\tlat: " + result_lat + "\n" +
                        "sys:\tстрана: " + result_country + "\tвосход: " + result_sunrise + "\tзакат: " + result_sunset + "\n" +
                        result_weather + "\n"+
                        "base: " + result_base + "\n" +
                        "Основное:\tтемпература: " + result_temp + "\tвлажность: " + result_humidity + "\tдавление: " + result_pressure + "\ttemp_min: " + result_temp_min + "\ttemp_max: " + result_temp_min + "\n" +
                        result_wind + "\n" +
                        "clouds\tall: " + result_all + "\n" +
                        "dt: " + result_dt + "\n" +
                        "id: " + result_id + "\n" +
                        "cod: " + result_cod + "\n" +
                        "\n";
        /*return
                "coord\tlon: " + result_lon + "\tlat: " + result_lat + "\n" +
                        "sys\tcountry: " + result_country + "\tsunrise: " + result_sunrise + "\tsunset: " + result_sunset + "\n" +
                        result_weather + "\n"+
                        "base: " + result_base + "\n" +
                        "main\ttemp: " + result_temp + "\thumidity: " + result_humidity + "\tpressure: " + result_pressure + "\ttemp_min: " + result_temp_min + "\ttemp_max: " + result_temp_min + "\n" +
                        result_wind + "\n" +
                        "clouds\tall: " + result_all + "\n" +
                        "dt: " + result_dt + "\n" +
                        "id: " + result_id + "\n" +
                        "name: " + result_name + "\n" +
                        "cod: " + result_cod + "\n" +
                        "\n";*/
    }
}