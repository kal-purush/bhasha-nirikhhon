package com.odeproject;
import org.json.JSONObject;

import java.io.*;
import java.net.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.FileWriter;

public class Server {
    static Socket client;
    public static void main(String[] args) throws IOException {


        // *****************************    Try to connect with Server      *******************************

        try {

            ServerSocket server = new ServerSocket(4711);
            System.out.println("Server: waiting for connection on Port: "+ server.getLocalPort());
            client = server.accept();
        } catch (IOException e) {
            System.out.println("There is an error in Server Connection");
            e.printStackTrace();
        }

        // *****************************    Try to connect with API      *******************************


        String apiKey = "8b06e9e136094be6acd171511232702";
        String location = "Vienna"; // replace with desired location

        URL url = new URL("https://api.weatherapi.com/v1/current.json?key=" + apiKey + "&q=" + location);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");

        int responseCode = con.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            String jsonResponse = response.toString();

            // save JSON response to file
            FileWriter fileWriter = new FileWriter("weather.json");
            fileWriter.write(jsonResponse);
            fileWriter.close();

            // parse JSON response to extract location and current weather
            JSONObject jsonObj = new JSONObject(jsonResponse);
            String locationName = jsonObj.getJSONObject("location").getString("name");
            String weatherCondition = jsonObj.getJSONObject("current").getJSONObject("condition").getString("text");
            double temperature = jsonObj.getJSONObject("current").getDouble("temp_c");
            String locationCountry = jsonObj.getJSONObject("location").getString("country");
            String localTime = jsonObj.getJSONObject("location").getString("localtime");
            double feelsLikeC = jsonObj.getJSONObject("current").getDouble("feelslike_c");
            double windKph = jsonObj.getJSONObject("current").getDouble("wind_kph");


            // print location and current weather
            System.out.println("Location:\t\t " + locationName);
            System.out.println("Local Time:\t\t " + localTime);
            System.out.println("Current Weather: " + weatherCondition);
            System.out.println("Temprature:\t\t " + temperature + "°");
            System.out.println("Feels like:\t\t " + feelsLikeC+ "°");
            System.out.println("Wind:\t\t\t "+ windKph+ " kph");
            System.out.println("Country:\t\t " + locationCountry);

            // *******************************    Create a File      ***********************************

            try {
                File myFile = new File("myWeatherData.txt");
                if (myFile.createNewFile()){
                    System.out.println("File created: " + myFile.getName());
                } else {
                    System.out.println("File already exists.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }

            // *****************************    Write date into a File      *******************************

            try {
                FileWriter myWriter = new FileWriter("myWeatherData.txt");
                myWriter.write("Location: " + locationName + "\n");
                myWriter.write("Local Time: " + localTime + "\n");
                //myWriter.write("Current Weather: " + weatherCondition + "\n");
                //myWriter.write("Temprature:\t\t " + temperature + "°\n");
                //myWriter.write("Feels like:\t\t " + feelsLikeC+ "°\n");
                //myWriter.write("Wind:\t\t\t "+ windKph+ " kph\n");
                //myWriter.write("Country:\t\t " + locationCountry + "\n");
                myWriter.close();
                System.out.println("Successfully wrote to the file.");
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }


        } else {
            System.out.println("Error getting weather data");
        }


        // *****************************       Reading the Weather Data      *******************************

        String data= "";

        try {

            File file= new File("C:\\AODE\\Wetter-App\\myWeatherData.txt");
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String text;

            while ((text= bufferedReader.readLine()) != null){
                data = bufferedReader.readLine();
            }
        } catch (IOException e1) {
            System.out.println("There is an error in Reading of Weather Data");
            e1.printStackTrace();
        }


        // *****************************    Sending the Weather Data to Client   *******************************

        try{
            OutputStream stream = client.getOutputStream();
            OutputStream out = client.getOutputStream();
            stream.write(data.getBytes());
        } catch (IOException e2) {
            System.out.println("There is an error in Sending the Weather Data to Client!");
            e2.printStackTrace();
        }


        /*
        String data= "";
        
        try {               // Try to connect with Server

            ServerSocket server = new ServerSocket(4711);
            System.out.println("Server: waiting for connection on Port: "+ server.getLocalPort());
            client = server.accept();
        } catch (IOException e) {
            System.out.println("There is an error in Server Connection");
            e.printStackTrace();
        }

        try {               //Reading the Weather Data

            File file= new File("C:\\Users\\cnryl\\Fh_technikum\\ODE\\Wetter-App\\src\\main\\java\\weather.txt");
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String text;

            while ((text= bufferedReader.readLine()) != null){
                data = bufferedReader.readLine();
            }
        } catch (IOException e1) {
            System.out.println("There is an error in Reading of Weather Data");
            e1.printStackTrace();
        }

        try{                //Sending the Weather Data to Client
            OutputStream stream = client.getOutputStream();
            OutputStream out = client.getOutputStream();
            stream.write(data.getBytes());
        } catch (IOException e2) {
            System.out.println("There is an error in Sending the Weather Data to Client!");
            e2.printStackTrace();
        }

         */
    }
}