/*
Rick Wallace
HttpDownloader
Desc: Pull states, stations and data from the NOAA server.
 */
package weatherapp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONObject;

public class HttpDownloader {

    //STATES
    public static WeatherHashTable downloadStates() throws IOException {
        URL url = new URL("https://www.ncdc.noaa.gov/cdo-web/api/v2/locations?locationcategoryid=ST&includemetadata=false&limit=52");
        HttpURLConnection uc = (HttpURLConnection) url.openConnection();
        uc.setRequestProperty("token", "YudttbHulnyYtDGZnXIdwuboPyxWieGn");
        uc.connect();

        JSONObject obj;
        String line, str = "";

        WeatherHashTable hm = new WeatherHashTable();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(uc.getInputStream()))) {
            //get JSON from web
            while ((line = reader.readLine()) != null) {
                str += line;
            }

            obj = new JSONObject(str);

            //create array of states
            JSONArray jArray = obj.getJSONArray("results");

            //fill HashMap with ids and state names
            for (int j = 0; j < (jArray.length()); j++) {
                JSONObject objTemp = jArray.getJSONObject(j);
                State s = new State(objTemp.getString("id"), objTemp.getString("name"));
                hm.put(objTemp.getString("name"), s);
            }

        } catch (Exception e) {
            System.err.println("Error receiving States " + e);
        }

        uc.disconnect();

        return hm;
    }//end downloadstates

    //STATES
    public static WeatherHashTable downloadStations(String id) throws IOException {
        LocalDate enddate = LocalDate.now();
        LocalDate startdate = enddate.minusMonths(1);

        URL url = new URL("https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?datasetid=GHCND&datatypeid=TOBS&startdate=2016-12-01&includemetadata=false&limit=15&locationid=" + id);
        HttpURLConnection uc1 = (HttpURLConnection) url.openConnection();
        uc1.setRequestProperty("token", "YudttbHulnyYtDGZnXIdwuboPyxWieGn");
        uc1.connect();
        System.out.println("URL: " + url);
        JSONObject obj;
        String line, str = "";
        //temp
        int aveTemp;

        WeatherHashTable hm = new WeatherHashTable();

        try (BufferedReader reader1 = new BufferedReader(
                new InputStreamReader(uc1.getInputStream()))) {
            //get JSON from web
            while ((line = reader1.readLine()) != null) {
                str += line;
            }

            obj = new JSONObject(str);

            //create array of states
            JSONArray jArray = obj.getJSONArray("results");

            //fill HashMap with ids and state names and ave temp
            for (int j = 0; j < (jArray.length()); j++) {
                JSONObject objTemp = jArray.getJSONObject(j);
                Station stn = new Station(objTemp.getString("id"), objTemp.getString("name"));
                hm.put(stn.getName(), stn);
                //get temp
                aveTemp = downloadInfo(stn.getID(), startdate, enddate);
                //set avetemp
                stn.setAveTemp(aveTemp);

            }

        } catch (Exception e) {
            System.err.println("No stations available with selected criteria - \n" + e);
        }

        uc1.disconnect();

        return hm;
    }

    //STATES
    public static int downloadInfo(String sid, LocalDate startD, LocalDate endD) throws IOException {
        URL url = new URL("https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=TOBS&units=metric&includemetadata=false&startdate=" + startD + "&enddate=" + endD + "&stationid=" + sid);
        HttpURLConnection uc2 = (HttpURLConnection) url.openConnection();
        uc2.setRequestProperty("token", "YudttbHulnyYtDGZnXIdwuboPyxWieGn");
        uc2.connect();

        System.out.println(url);
        int[] newTemps = {};
        int aveTemp;
        JSONObject obj;
        String line, str = "";

        try (BufferedReader reader2 = new BufferedReader(
                new InputStreamReader(uc2.getInputStream()))) {
            //get JSON from web
            while ((line = reader2.readLine()) != null) {
                str += line;
            }

            obj = new JSONObject(str);

            //create array of temp data
            JSONArray jArray = obj.getJSONArray("results");
            newTemps = new int[jArray.length()];
            for (int j = 0; j < (jArray.length()); j++) {
                JSONObject objTemp = jArray.getJSONObject(j);
                newTemps[j] = objTemp.getInt("value");
                //System.out.println("Temp: " + newData[j]);
            }

        } catch (Exception e) {
            System.err.print("specified data is not available for this Station.");
        }
        uc2.disconnect();

        aveTemp = calcAveTemp(newTemps);

        return aveTemp;
    }//end downloadInfo

    //get the average temp for the last month
    public static int calcAveTemp(int[] s) { //param = array of temp data from the last month
        int used = 0, temp = 0;

        try {

            for (int i = 0; i < s.length; i++) {
                //check for valid data
                if (s[i] > -170 && s[i] < 170) {
                    temp += s[i];
                    used++;
                }
            }
            temp = temp / used;

        } catch (Exception e) {
            System.err.println();
        }

        return temp;
    }

}