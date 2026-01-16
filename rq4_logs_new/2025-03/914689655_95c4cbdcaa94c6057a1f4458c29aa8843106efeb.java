package ui;

import com.google.gson.Gson;
import model.AuthData;
import model.GameData;
import model.JoinRequest;
import model.UserData;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.UUID;


public class ServerFacade {
    private final String serverURl;
    private UUID authorization;

    public ServerFacade(String url){
        serverURl = url;
    }

    public AuthData registerUser(UserData registerData) {
        var path = "/user";
        AuthData response = makeRequest("POST", path, registerData, AuthData.class);
        authorization = response.authToken();
        return response;

    }
    public AuthData loginUser(UserData loginData){
        var path = "/session";
        AuthData response = makeRequest("POST", path, loginData, AuthData.class);
        authorization = response.authToken();
        return response;
    }

    public void logoutUser(){
        var path = "/session";
        makeRequest("DELETE", path, null, null);
        authorization = null;
    }

    public GameData createGame(GameData gameData){
        var path = "/game";
        GameData response = makeRequest("POST", path, gameData, GameData.class);
        return response;
    }

    public void joinGame(JoinRequest request){
        var path = "/game";
        makeRequest("PUT", path, request, null);
    }
    public GameData listGames(){
        var path = "/game";
        GameData response = makeRequest("GET", path, null, GameData.class);
        return response;

    }
    public void clearAll(){
        var path = "/db";
        makeRequest("DELETE", path, null, null);
    }

    private <T> T makeRequest(String method, String path, Object request, Class<T> responseClass){
        try{
            URL url = (new URI(serverURl + path)).toURL();
            HttpURLConnection http = (HttpURLConnection) url.openConnection(); //Sets up an empty request
            http.setRequestMethod(method); //sets request method
            if(authorization != null){
                //set authorization header to UUID
                http.addRequestProperty("authorization", String.valueOf(authorization));
            }
            http.getDoOutput(); // lets us know to expect things in return
            writeBody(request, http); //sets request body
            http.connect(); //actually connects with this now not empty request



            throwIfNotSuccessful(http);
            return readBody(responseClass, http);
        }
        catch(Exception e){
            System.out.println(e);
        }
        return null;
    }

    private void writeBody(Object request, HttpURLConnection http){
        String body = new Gson().toJson(request);
        http.addRequestProperty("Content-Type", "application/json");
        try(OutputStream reqBody = http.getOutputStream()){
            reqBody.write(body.getBytes()); //connection wants a stream for the body, so we give it a stream
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private void throwIfNotSuccessful(HttpURLConnection http) throws Exception {
        var status = http.getResponseCode();
        if (status / 100 != 2) {
            try (InputStream respErr = http.getErrorStream()) {
                if (respErr != null) {
                    throw new Exception();
                }
            }

            throw new Exception();
        }
    }

    private <T> T readBody(Class<T> responseClass, HttpURLConnection http) throws IOException{
        T response = null;
        if (http.getContentLength() < 0) { // if the content length header is an unknown number or too big
            try (InputStream respBody = http.getInputStream()) {
                InputStreamReader reader = new InputStreamReader(respBody);
                if (responseClass != null) {
                    response = new Gson().fromJson(reader, responseClass);
                }
            }
        }
        else{
            System.out.println(http.getContentLength() + " <- content length header field number");
        }
        return response;

    }
}